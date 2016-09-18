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

static bool
parse_single_comment(std::string &token, std::string &key, std::string &value)
{
  size_t pos;
  if ((pos = token.find("=")) == std::string::npos) {
    sql_print_error("Failed to parse '%s', '=' is missing", token.c_str());
    return false;
  }
  key = token.substr(0, pos);
  value = token.substr(pos + 1);
  boost::trim(key);
  boost::trim(value);
  boost::to_lower(key);
  boost::to_lower(value);
  return true;
}

template<typename T>
bool
parse_comment_list(const char *comment, T &sink)
{
  std::string s = comment;
  std::string key;
  std::string value;

  size_t last = 0;
  size_t next = 0;
  while ((next = s.find(";", last)) != std::string::npos) {
    std::string token = s.substr(last, next - last);
    boost::trim(token);
    if (!parse_single_comment(token, key, value))
      return false;
    if (!sink.add(key, value)) {
      sql_print_error("Invalid option '%s' => '%s'", key.c_str(),
                      value.c_str());
      return false;
    }
    last = next + 1;
  }
  std::string token = s.substr(last);
  boost::trim(token);
  if (!token.empty()) {
    if (!parse_single_comment(token, key, value))
      return false;
    if (!sink.add(key, value)) {
      sql_print_error("Invalid option '%s' => '%s'", key.c_str(),
                      value.c_str());
      return false;
    }
  }

  sink.finalize();

  return true;
}

template<typename T>
bool
parse_file(const std::string &filename, T &sink)
{
  std::ifstream in(filename.c_str(), std::ios::in | std::ios::binary);
  if (!in) // file does not exist - not an error!
    return true;

  std::string line, key, value;
  while (std::getline(in, line)) {
    size_t comment = line.find("#");
    if (comment != std::string::npos)
      line.resize(comment);

    boost::trim(line);

    if (line.size() == 0)
      continue;

    if (!parse_single_comment(line, key, value))
      return false;
    if (!sink.add(key, value)) {
      sql_print_error("Invalid option '%s' => '%s'", key.c_str(),
                      value.c_str());
      return false;
    }
  }
  sink.finalize();

  return true;
}

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

static void
write_configuration_settings(std::string env_name, const char *comment,
                Configuration &list)
{
  std::ofstream f;
  std::string fname = env_name + ".cnf";
  f.open(fname.c_str());

  f << "# Configuration settings for " << env_name << std::endl
    << "# original table COMMENT:" << std::endl
    << "# " << comment << std::endl
    << std::endl;
  if (list.flags & UPS_ENABLE_CRC32)
    f << "enable_crc32 = true" << std::endl;
  else
    f << "enable_crc32 = false" << std::endl;

  if (list.flags & UPS_DISABLE_RECOVERY)
    f << "disable_recovery = true" << std::endl;
  else
    f << "disable_recovery = false" << std::endl;
  if (list.flags & UPS_CACHE_UNLIMITED)
    f << "cache_size = unlimited" << std::endl;

  for (size_t i = 0; i < list.params.size(); i++) {
    switch (list.params[i].name) {
      case UPS_PARAM_RECORD_COMPRESSION:
        f << "enable_compression = ";
        if (list.params[i].value == UPS_COMPRESSOR_ZLIB)
          f << "zlib";
        else if (list.params[i].value == UPS_COMPRESSOR_SNAPPY)
          f << "snappy";
        else if (list.params[i].value == UPS_COMPRESSOR_LZF)
          f << "lzf";
        else
          f << "none";
        f << std::endl;
        break;

      case UPS_PARAM_CACHE_SIZE:
        f << "cache_size = " << list.params[i].value << std::endl;
        break;

      case UPS_PARAM_PAGE_SIZE:
        f << "page_size = " << list.params[i].value << std::endl;
        break;

      case UPS_PARAM_FILE_SIZE_LIMIT:
        f << "file_size_limit = " << list.params[i].value << std::endl;
        break;
    }
  }

  if (list.is_server_enabled)
    f << "enable_server = true" << std::endl;
  else
    f << "enable_server = false" << std::endl;

  f << "server_port = " << list.server_port << std::endl;
}

