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
#include <fstream>

#include "configuration.h"

static bool
parse_single_comment(std::string &token, std::string &key, std::string &value)
{
  size_t pos;
  if ((pos = token.find("=")) == std::string::npos) {
    ::fprintf(stderr, "Failed to parse '%s', '=' is missing\n", token.c_str());
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

bool
parse_comment_list(const char *comment, Configuration &sink)
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
      ::fprintf(stderr, "Invalid option '%s' => '%s'\n", key.c_str(),
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
      ::fprintf(stderr, "Invalid option '%s' => '%s'\n", key.c_str(),
                      value.c_str());
      return false;
    }
  }

  sink.finalize();

  return true;
}

bool
parse_file(const std::string &filename, Configuration &sink)
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
      ::fprintf(stderr, "Invalid option '%s' => '%s'\n", key.c_str(),
                      value.c_str());
      return false;
    }
  }
  sink.finalize();

  return true;
}

void
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

