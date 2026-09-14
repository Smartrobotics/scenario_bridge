cmake_minimum_required(VERSION 3.0.2)
project(scenario_bridge)

# scenario_control は find_package しない。
# scenario_control の catkin_package() は LIBRARIES scenario_control を宣言しているが
# そのライブラリは作られておらず、find_package すると configure が失敗する。
# 必要なのは実行時の Python モジュール(scenario_control/msg)だけなので、
# package.xml の exec_depend で足りる。
find_package(catkin REQUIRED COMPONENTS
  rospy
  std_msgs
)

catkin_package(
  CATKIN_DEPENDS rospy std_msgs
)

catkin_install_python(PROGRAMS
  scripts/scenario_bridge.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch/
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch
)
