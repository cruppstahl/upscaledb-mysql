/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#ifndef CONFIGURATION_H__
#define CONFIGURATION_H__

#include <map>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>

#include <ups/upscaledb_int.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#define DEFAULT_CACHE_SIZE   (128 * 1024 * 1024)
#define DEFAULT_SERVER_PORT  54123

struct Configuration {
  Configuration(bool is_open_ = false)
    : is_open(is_open_), is_server_enabled(false),
      server_port(DEFAULT_SERVER_PORT),
      flags(UPS_ENABLE_TRANSACTIONS), record_compression(0) {
  }

  bool add(std::string &key, std::string &value) {
    if (key == "enable_crc32") {
      if (value == "true") {
        flags |= UPS_ENABLE_CRC32;
        return true;
      }
      if (value == "false")
        return true;
      return false;
    }

    if (key == "enable_server") {
      if (value == "true") {
        is_server_enabled = true;
        return true;
      }
      if (value == "false") {
        is_server_enabled = false;
        return true;
      }
      return false;
    }

    if (key == "disable_recovery") {
      if (value == "true") {
        flags |= UPS_DISABLE_RECOVERY;
        return true;
      }
      if (value == "false")
        return true;
      return false;
    }

    /*
    if (key == "in_memory") {
      if (value == "true") {
        flags |= UPS_IN_MEMORY;
        return true;
      }
      if (value == "false")
        return true;
      return false;
    }
    */

    if (key == "enable_compression") {
      if (is_open) // ignore when opening
        return true;
      if (value == "zlib") {
        record_compression = UPS_COMPRESSOR_ZLIB;
        return true;
      }
      if (value == "snappy") {
        record_compression = UPS_COMPRESSOR_SNAPPY;
        return true;
      }
      if (value == "lzf") {
        record_compression = UPS_COMPRESSOR_SNAPPY;
        return true;
      }
      if (value == "none")
        return true;
      return false;
    }

    if (key == "cache_size") {
      if (value == "unlimited") {
        flags |= UPS_CACHE_UNLIMITED;
        return true;
      }
      try {
        ups_parameter_t p = {UPS_PARAM_CACHE_SIZE,
                boost::lexical_cast<uint32_t>(value)};
        params.push_back(p);
        return true;
      }
      catch (boost::bad_lexical_cast &) {
        return false;
      }
    }

    if (key == "page_size") {
      if (is_open) // ignore when opening
        return true;
      try {
        ups_parameter_t p = {UPS_PARAM_PAGE_SIZE,
                boost::lexical_cast<uint32_t>(value)};
        params.push_back(p);
        return true;
      }
      catch (boost::bad_lexical_cast &) {
        return false;
      }
    }

    if (key == "file_size_limit") {
      try {
        ups_parameter_t p = {UPS_PARAM_FILE_SIZE_LIMIT,
                boost::lexical_cast<uint32_t>(value)};
        params.push_back(p);
        return true;
      }
      catch (boost::bad_lexical_cast &) {
        return false;
      }
    }

    if (key == "server_port") {
      try {
        server_port = boost::lexical_cast<uint16_t>(value);
        return true;
      }
      catch (boost::bad_lexical_cast &) {
        return false;
      }
    }

    return false;
  }

  void finalize() {
    // no cache size specified? then set the default (128 mb)
    bool found = false;
    for (std::vector<ups_parameter_t>::iterator it = params.begin();
                    it != params.end(); ++it) {
      if (it->name == UPS_PARAM_CACHE_SIZE) {
        found = true;
        break;
      }
    }
    if (!found) {
      ups_parameter_t p = {UPS_PARAM_CACHE_SIZE, DEFAULT_CACHE_SIZE};
      params.push_back(p);
    }

    // add the "terminating" element
    ups_parameter_t p = {0, 0};
    params.push_back(p);
  }

  bool is_open;
  bool is_server_enabled;
  uint16_t server_port;
  uint32_t flags;
  uint32_t record_compression;
  std::vector<ups_parameter_t> params;
};

extern bool
parse_comment_list(const char *comment, Configuration &sink);

extern bool
parse_file(const std::string &filename, Configuration &sink);

extern void
write_configuration_settings(std::string env_name, const char *comment,
                Configuration &list);

#endif // CONFIGURATION_H__
