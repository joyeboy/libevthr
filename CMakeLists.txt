cmake_minimum_required(VERSION 2.8)
project(libevthr)

FIND_PATH(LIBEVENT_INCLUDE_DIR NAMES event.h)
FIND_LIBRARY(LIBEVENT_LIBRARY NAMES event)

add_library(libevthr STATIC evthr.c)
set_target_properties(libevthr PROPERTIES OUTPUT_NAME "evthr")

install (TARGETS libevthr DESTINATION lib)
install (FILES evthr.h DESTINATION include)
