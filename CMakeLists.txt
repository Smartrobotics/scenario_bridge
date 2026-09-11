cmake_minimum_required(VERSION 3.0.2)
project(scenario_bridge)

find_package(catkin REQUIRED COMPONENTS
  rospy
  std_msgs
  scenario_control
)

catkin_package(
  CATKIN_DEPENDS rospy std_msgs scenario_control
)

catkin_install_python(PROGRAMS
  scripts/scenario_bridge.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY launch/
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch
)
