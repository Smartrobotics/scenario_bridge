# scenario_bridge

SitePorter のサーバー(FastAPI、ホスト側 Docker)と、コンテナ内の
`scenario_control_json` をつなぐ HTTP ブリッジ。rospy + Flask、Python 2.7。

仕様: `Taisei_takuhai_system/docs/siteporter-bridge-api.ja.md`（§3 をそのまま実装）。

## 口

| メソッド | パス | ROS 側 |
|---|---|---|
| `POST` | `/scenario` `{"name": "...", "run_id": N}` | `/scenario_name` (std_msgs/String) に publish |
| `GET` | `/state` | `/scenario_state` (scenario_control/ScenarioState, latched) の最新値 + `ros_ok` + `uptime_sec` |
| `POST` | `/cancel` | `/scenario_cancel` (std_msgs/Bool true) に publish |

## 待受アドレス

既定は **172.17.0.1:8080**（docker0）。

バックエンドは `--network host` ではなく compose のネットワークで動いており、
`host.docker.internal` = 172.17.0.1 でホストへ来る。127.0.0.1 では届かない。
0.0.0.0 にするとコンテナが `--network host` なので LAN に開いてしまう。
docker0 は同じホストのコンテナからしか見えない。

## ros_ok

`GET /state` の `ros_ok` は **マスターに繋がっていて、かつエンジンが `/scenario_name` を
購読している**ときだけ true。エンジンが居ないのに受理すると名前は誰にも届かず、
サーバーの再送と `duplicate` が噛み合って永久に止まるため。`ros_ok:false` のときは
`POST` は 503 を返し、サーバーはロボットを error 扱いにして依頼を保持する。

## duplicate

同じ名前を二重に publish しない。エンジンはキューを持つので、二重に届くと
SUCCESS の後にもう一度走ってしまう。「まだ終わっていない」の判定はエンジンが
その名前で終了状態を出したかどうか。エンジンが IDLE（起動直後）のときは投入済みの
名前を忘れているので、再送を通す（仕様 §4.4 E）。

## 起動

```bash
roslaunch scenario_bridge scenario_bridge.launch                  # 172.17.0.1:8080
roslaunch scenario_bridge scenario_bridge.launch host:=127.0.0.1  # コンテナ内から curl で試すとき
```

`run_takuhai.launch` からは `use_bridge` 引数で入る（既定 true）。

## 手で試す

```bash
curl -s http://172.17.0.1:8080/state
curl -s -X POST http://172.17.0.1:8080/scenario -H 'Content-Type: application/json' \
     -d '{"name":"get_state","run_id":0}'
curl -s -X POST http://172.17.0.1:8080/cancel
```

## パラメータ

| 名前 | 既定 | 説明 |
|---|---|---|
| `~host` | `172.17.0.1` | 待受アドレス |
| `~port` | `8080` | 待受ポート |
| `~scenario_dir` | 空 | 空なら `/scenario_control_json/scenario_dir`、それも無ければ `scenario_control/scenarios` |
