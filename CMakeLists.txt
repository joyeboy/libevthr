cmake_minimum_required(VERSION 2.8)
project(libevthr)

add_library(libevthr STATIC evthr.c)
set_target_properties(libevthr PROPERTIES OUTPUT_NAME "evthr")

install (TARGETS libevthr DESTINATION lib)
install (FILES evthr.h DESTINATION include)
