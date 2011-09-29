cmake_minimum_required(VERSION 2.8)
project(libevthr)

FIND_PATH(LIBEVENT_INCLUDE_DIR NAMES event.h)
FIND_LIBRARY(LIBEVENT_LIBRARY NAMES event)
MARK_AS_ADVANCED(LIBEVENT_INCLUDE_DIR LIBEVENT_LIBRARY)

include_directories(${LIBEVENT_INCLUDE_DIR})

add_library(libevthr STATIC evthr.c)
set_target_properties(libevthr PROPERTIES OUTPUT_NAME "evthr")

install (TARGETS libevthr DESTINATION lib)
install (FILES evthr.h DESTINATION include)
