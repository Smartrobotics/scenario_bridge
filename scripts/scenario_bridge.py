#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scenario_bridge — SitePorter サーバー(FastAPI) と scenario_control_json の橋渡し。

サーバーは ROS を持たない(ホストは ROS2、コンテナは ROS1 Melodic / Python 2.7)。
そこでこのノードが HTTP を受け、ROS トピックに流す。仕様は
docs/siteporter-bridge-api.ja.md。ここでは仕様の §3 をそのまま実装する。

    POST /scenario {"name": "run_123_04_elv", "run_id": 123}
         → /scenario_name (std_msgs/String) に publish
    GET  /state
         → /scenario_state (scenario_control/ScenarioState, latched) の最新値 + 死活情報
    POST /cancel
         → /scenario_cancel (std_msgs/Bool true) に publish

判断はサーバー側(engine.py)が行う。ここは「投げる・返す」に徹するが、
サーバーの再送ロジックが成り立つための3点だけはこちらで守る:

  1. duplicate — 同じ名前を二重に publish しない。
     エンジンはキューを持つので、二重に届くと SUCCESS の後にもう一度走る。
  2. ros_ok — マスターに繋がり、かつエンジンが /scenario_name を購読しているときだけ true。
     エンジンが居ないのに 202 を返すと、名前は誰にも届かず、サーバーは IDLE を見て
     再送し、こちらは duplicate と答え、永久に止まる。
  3. /scenario_name は latch しない。
     latch すると、エンジンが再起動したときに最後の名前を受け取ってもう一度走る。

Python 2.7 / rospy / Flask 1.1 で動く。
"""

from __future__ import print_function

import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta

import rospy
import rosgraph
import rospkg
from flask import Flask, jsonify, request
from std_msgs.msg import Bool, String

from scenario_control.msg import ScenarioState

# ---------------------------------------------------------------- 定数

STATUS_NAME = {
    ScenarioState.IDLE: 'IDLE',
    ScenarioState.RUNNING: 'RUNNING',
    ScenarioState.SUCCESS: 'SUCCESS',
    ScenarioState.FAILURE: 'FAILURE',
    ScenarioState.CANCELED: 'CANCELED',
}
TERMINAL = (ScenarioState.SUCCESS, ScenarioState.FAILURE, ScenarioState.CANCELED)

# 仕様 §3.1: 拡張子なしのファイル名。`/` と `..` は不可
NAME_RE = re.compile(r'^[A-Za-z0-9_.\-]+$')

# ros_ok の確認周期。リクエストのたびにマスターへ聞きに行かない
HEALTH_PERIOD_SEC = 1.0

# stamp を ISO8601 にするときのタイムゾーン(コンテナは UTC で動いている)
TZ_OFFSET_HOURS = 9


def iso_stamp(stamp):
    """rospy.Time → '2026-09-08T10:21:04.310+09:00'"""
    if stamp is None or stamp.to_sec() == 0.0:
        stamp = rospy.Time.now()
    dt = datetime.utcfromtimestamp(stamp.to_sec()) + timedelta(hours=TZ_OFFSET_HOURS)
    sign = '+' if TZ_OFFSET_HOURS >= 0 else '-'
    return '%s.%03d%s%02d:00' % (dt.strftime('%Y-%m-%dT%H:%M:%S'),
                                 dt.microsecond // 1000, sign, abs(TZ_OFFSET_HOURS))


# ---------------------------------------------------------------- ROS 側

class Bridge(object):
    """ROS との接点。latched な /scenario_state の最新値を持ち、publish を引き受ける"""

    def __init__(self, scenario_dir):
        self.scenario_dir = scenario_dir
        self.started = time.time()
        self.lock = threading.Lock()

        self.state = None          # 最後に受けた ScenarioState。まだ無ければ None
        self.last_accepted = None  # 直近に受理して publish した名前
        self.last_accepted_at = 0.0

        self.master_ok = False
        self.engine_ok = False

        # latch=False。理由は冒頭の 3.
        self.pub_name = rospy.Publisher('/scenario_name', String, queue_size=10, latch=False)
        self.pub_cancel = rospy.Publisher('/scenario_cancel', Bool, queue_size=10, latch=False)
        self.sub_state = rospy.Subscriber('/scenario_state', ScenarioState,
                                          self._on_state, queue_size=1)

        self._health_thread = threading.Thread(target=self._health_loop)
        self._health_thread.daemon = True
        self._health_thread.start()

    # ------------------------------------------------------------ 状態
    def _on_state(self, msg):
        with self.lock:
            self.state = msg
        if msg.status in TERMINAL or msg.status == ScenarioState.IDLE:
            rospy.loginfo('/scenario_state %s %s step %d/%d %s',
                          STATUS_NAME.get(msg.status, msg.status), msg.scenario_name,
                          msg.step_index, msg.step_total, msg.reason)

    def _health_loop(self):
        master = rosgraph.Master('/scenario_bridge')
        while not rospy.is_shutdown():
            try:
                master.getPid()
                ok = True
            except Exception:
                ok = False
            # エンジンが /scenario_name を購読していなければ、投げても届かない
            engine = self.pub_name.get_num_connections() > 0
            if ok != self.master_ok or engine != self.engine_ok:
                rospy.logwarn('ros_ok: master=%s engine=%s', ok, engine)
            self.master_ok, self.engine_ok = ok, engine
            time.sleep(HEALTH_PERIOD_SEC)

    @property
    def ros_ok(self):
        return self.master_ok and self.engine_ok and not rospy.is_shutdown()

    def uptime_sec(self):
        return int(time.time() - self.started)

    def snapshot(self):
        """GET /state の本文"""
        with self.lock:
            msg = self.state
        if msg is None:
            # まだ何も届いていない = エンジンがまだ居ない(latched なので居れば即届く)
            body = {
                'status': 'IDLE', 'scenario_name': '', 'step_index': 0, 'step_total': 0,
                'action': '', 'reason': '', 'queue_size': 0, 'stamp': iso_stamp(None),
            }
        else:
            body = {
                'status': STATUS_NAME.get(msg.status, str(msg.status)),
                'scenario_name': msg.scenario_name,
                'step_index': int(msg.step_index),
                'step_total': int(msg.step_total),
                'action': msg.action,
                'reason': msg.reason,
                'queue_size': int(msg.queue_size),
                'stamp': iso_stamp(msg.header.stamp),
            }
        body['ros_ok'] = self.ros_ok
        body['uptime_sec'] = self.uptime_sec()
        return body

    # ------------------------------------------------------------ 投入
    def scenario_exists(self, name):
        return os.path.isfile(os.path.join(self.scenario_dir, name + '.json'))

    def is_duplicate(self, name):
        """
        同名がすでに投入済みで、まだ終わっていないか(仕様 §3.1 / §4.2)。

        「終わった」はエンジンがその名前で終了状態を出したこと。
        エンジンが IDLE を出しているときは再起動直後で、投入済みの名前は
        忘れられている(§4.4 E)。その再送は duplicate ではない。
        """
        if name != self.last_accepted:
            return False
        with self.lock:
            msg = self.state
        if msg is None or msg.status == ScenarioState.IDLE:
            return False
        if msg.scenario_name == name and msg.status in TERMINAL:
            return False
        return True

    def publish_scenario(self, name):
        self.pub_name.publish(String(data=name))
        with self.lock:
            self.last_accepted = name
            self.last_accepted_at = time.time()

    def publish_cancel(self):
        self.pub_cancel.publish(Bool(data=True))


# ---------------------------------------------------------------- HTTP 側

def make_app(bridge):
    app = Flask('scenario_bridge')

    def error(code, name):
        return jsonify({'error': name}), code

    @app.errorhandler(404)
    def _not_found(_e):
        return error(404, 'not_found')

    @app.errorhandler(405)
    def _bad_method(_e):
        return error(405, 'method_not_allowed')

    @app.route('/state', methods=['GET'])
    def state():
        return jsonify(bridge.snapshot()), 200

    @app.route('/scenario', methods=['POST'])
    def scenario():
        payload = request.get_json(force=True, silent=True) or {}
        name = payload.get('name')
        run_id = payload.get('run_id')
        if not isinstance(name, (str, type(u''))):
            return error(400, 'invalid_name')
        name = name.strip()
        if not name or not NAME_RE.match(name) or '..' in name:
            rospy.logwarn('POST /scenario invalid name %r', name)
            return error(400, 'invalid_name')
        if not bridge.scenario_exists(name):
            rospy.logwarn('POST /scenario not found %s (dir=%s)', name, bridge.scenario_dir)
            return error(404, 'scenario_not_found')
        if not bridge.ros_ok:
            rospy.logwarn('POST /scenario %s refused: ros unavailable (master=%s engine=%s)',
                          name, bridge.master_ok, bridge.engine_ok)
            return error(503, 'ros_unavailable')

        if bridge.is_duplicate(name):
            rospy.logwarn('POST /scenario %s run_id=%s duplicate, not published', name, run_id)
            return jsonify({'accepted': True, 'name': name, 'duplicate': True}), 202

        bridge.publish_scenario(name)
        rospy.loginfo('POST /scenario %s run_id=%s -> /scenario_name', name, run_id)
        return jsonify({'accepted': True, 'name': name, 'duplicate': False}), 202

    @app.route('/cancel', methods=['POST'])
    def cancel():
        if not bridge.ros_ok:
            rospy.logwarn('POST /cancel refused: ros unavailable')
            return error(503, 'ros_unavailable')
        bridge.publish_cancel()
        rospy.logwarn('POST /cancel -> /scenario_cancel true')
        return jsonify({'accepted': True}), 202

    return app


# ---------------------------------------------------------------- main

def default_scenario_dir():
    """エンジンと同じディレクトリ。launch の ~scenario_dir が無ければ package の scenarios/"""
    d = rospy.get_param('/scenario_control_json/scenario_dir', '')
    if d:
        return d
    return os.path.join(rospkg.RosPack().get_path('scenario_control'), 'scenarios')


def main():
    rospy.init_node('scenario_bridge')

    host = rospy.get_param('~host', '172.17.0.1')
    port = int(rospy.get_param('~port', 8080))
    scenario_dir = rospy.get_param('~scenario_dir', '') or default_scenario_dir()

    if not os.path.isdir(scenario_dir):
        rospy.logerr('scenario_dir is not a directory: %s', scenario_dir)

    bridge = Bridge(scenario_dir)
    app = make_app(bridge)

    # werkzeug のアクセスログは 1 秒ごとの GET /state で埋まるので黙らせる
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    def serve():
        try:
            app.run(host=host, port=port, threaded=True, debug=False, use_reloader=False)
        except Exception as e:
            rospy.logfatal('HTTP server stopped: %s', e)
            rospy.signal_shutdown('http server failed')

    t = threading.Thread(target=serve)
    t.daemon = True   # roslaunch が止めたらプロセスごと終わる
    t.start()

    rospy.loginfo('scenario_bridge listening on http://%s:%d scenario_dir=%s',
                  host, port, scenario_dir)
    rospy.spin()


if __name__ == '__main__':
    main()
