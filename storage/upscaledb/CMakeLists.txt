#
# Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
# All Rights Reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# See the file COPYING for License information.
#

include_directories("/home/ruppc/prj/upscaledb/include")
link_directories("/home/ruppc/prj/upscaledb/src/.libs")
#find_library(upscaledb UPSCALEDB_LIBRARY)


SET(UPSCALEDB_PLUGIN_DYNAMIC "ha_upscaledb")
SET(UPSCALEDB_SOURCES ha_upscaledb.cc)
IF(WITH_UPSCALEDB_STORAGE_ENGINE AND NOT WITHOUT_UPSCALEDB_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(upscaledb ${UPSCALEDB_SOURCES} STORAGE_ENGINE DEFAULT)
ELSEIF(NOT WITHOUT_UPSCALEDB_STORAGE_ENGINE)
  MYSQL_ADD_PLUGIN(upscaledb ${UPSCALEDB_SOURCES} STORAGE_ENGINE MODULE_ONLY)
ENDIF()

target_link_libraries(upscaledb -lupscaledb -lboost_filesystem)
