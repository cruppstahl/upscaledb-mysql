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
#include <stdio.h>
#include <stdarg.h>

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "configuration.h"
#include "catalogue.h"

static bool
parse_single_comment(std::string &token, std::string &key, std::string &value)
{
  size_t pos;
  if ((pos = token.find("=")) == std::string::npos)
    return false;
  key = token.substr(0, pos);
  value = token.substr(pos + 1);
  boost::trim(key);
  boost::trim(value);
  boost::to_lower(key);
  boost::to_lower(value);
  return true;
}

static ParserStatus
make_parser_status(bool result, const char *format, ...)
{
  va_list ap;
  char buffer[1024];

  va_start(ap, format);
  ::vsnprintf(buffer, sizeof(buffer), format, ap);
  va_end(ap);

  return std::make_pair(result, format);
}

ParserStatus
parse_comment_list(const char *comment, Catalogue::Table *cattbl)
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
      return make_parser_status(false,
                      "Failed to parse '%s', '=' is missing", token.c_str());
    if (!cattbl->add_config_value(key, value, false))
      return make_parser_status(false,
                      "Invalid option '%s' => '%s'", key.c_str(),
                      value.c_str());
    last = next + 1;
  }
  std::string token = s.substr(last);
  boost::trim(token);
  if (!token.empty()) {
    if (!parse_single_comment(token, key, value))
      return make_parser_status(false,
                      "Failed to parse '%s', '=' is missing", token.c_str());
    if (!cattbl->add_config_value(key, value, false))
      return make_parser_status(false,
                      "Invalid option '%s' => '%s'", key.c_str(),
                      value.c_str());
  }

  return make_parser_status(true, "");
}

ParserStatus
parse_config_file(const std::string &filename, Catalogue::Database *catdb,
                bool is_open)
{
  std::ifstream in(filename.c_str(), std::ios::in | std::ios::binary);
  if (!in) // file does not exist - not an error!
    return make_parser_status(true, "");

  std::string line, key, value;
  while (std::getline(in, line)) {
    size_t comment = line.find("#");
    if (comment != std::string::npos)
      line.resize(comment);

    boost::trim(line);

    if (line.size() == 0)
      continue;

    if (!parse_single_comment(line, key, value))
      return make_parser_status(false,
                      "Failed to parse '%s', '=' is missing", line.c_str());
    if (!catdb->add_config_value(key, value, is_open))
      return make_parser_status(false,
                      "Invalid option '%s' => '%s'", key.c_str(),
                      value.c_str());
  }

  catdb->finalize_config();

  return make_parser_status(true, "");
}

static ups_parameter_t *
get_parameter(std::vector<ups_parameter_t> params, uint32_t name)
{
  for (size_t i = 0; i < params.size(); i++) {
    if (params[i].name == name)
      return &params[i];
  }
  return 0;
}

void
write_configuration_settings(std::string env_name, const char *comment,
                Catalogue::Database *catdb)
{
  ups_parameter_t *p = 0;
  std::ofstream f;
  std::string fname = env_name + ".cnf";
  f.open(fname.c_str());

  f << "# Configuration settings for " << env_name << std::endl
    << "# original table COMMENT:" << std::endl
    << "# " << std::string(comment ? comment : "") << std::endl
    << "# If you change these settings then also restart the MySQL server!"
        << std::endl
    << std::endl
    << std::endl;

  f << "# Enables CRC32. Slightly decreases performance, but protects against "
          << std::endl
    << "# corrupt files and broken disks. Default: disabled. " << std::endl
    << "#" << std::endl;
  if (catdb->flags & UPS_ENABLE_CRC32)
    f << "enable_crc32 = true" << std::endl;
  else
    f << "#   enable_crc32 = false" << std::endl;
  f << std::endl
    << std::endl;

  f << "# Disables journalling and recovery. Will improve performance, but "
         << std::endl
    << "# also cause data loss if the server is not shut down gracefully. "
         << std::endl
    << "# Not recommended, unless you know what you are doing. Default: disabled." 
         << std::endl
    << "#" << std::endl;
  if (catdb->flags & UPS_DISABLE_RECOVERY)
    f << "disable_recovery = true" << std::endl;
  else
    f << "#   disable_recovery = false" << std::endl;
  f << std::endl
    << std::endl;

  f << "# Overwrites the cache size that is reserved for this database. Only "
          << std::endl
    << "# limits the heap memory, not the memory mapped memory that is used "
          << std::endl
    << "# when an existing file is opened. Value is specified in bytes, or "
          << std::endl
    << "# 'unlimited' for unlimited memory. Default: 128mb"
          << std::endl
    << "#" << std::endl;
  if (catdb->flags & UPS_CACHE_UNLIMITED)
    f << "cache_size = unlimited" << std::endl;
  else {
    p = get_parameter(catdb->params, UPS_PARAM_CACHE_SIZE);
    if (p && p->value != Catalogue::Database::kDefaultCacheSize)
      f << "cache_size = " << p->value << std::endl;
    else
      f << "#   cache_size = " << Catalogue::Database::kDefaultCacheSize
              << std::endl;
  }
  f << std::endl
    << std::endl;

  f << "# Sets a hard limit for the size of the MySQL database. Specified in "
          << std::endl
    << "# bytes. Default: disabled."
          << std::endl
    << "#" << std::endl;
  p = get_parameter(catdb->params, UPS_PARAM_FILE_SIZE_LIMIT);
  if (p)
    f << "file_size_limit = " << p->value << std::endl;
  else
    f << "#   file_size_limit = " << (10 * 1024 * 1024) << std::endl;
  f << std::endl
    << std::endl;

  f << "# Sets the Btree page size of the upscaledb environment. This setting "
          << std::endl
    << "# MUST be specified directly after creating the MySQL database, but "
          << std::endl
    << "# before creating the first table! Default: 16kb."
          << std::endl
    << "#" << std::endl;
  p = get_parameter(catdb->params, UPS_PARAM_PAGE_SIZE);
  if (p)
    f << "page_size = " << p->value << std::endl;
  else
    f << "#   page_size = " << (16 * 1024) << std::endl;
  f << std::endl
    << std::endl;

  f << "# Enables remote access via the upscaledb server API. Opens a backdoor "
          << std::endl
    << "# to your data, bypassing all security measures (user grants) in MySQL. "
          << std::endl
    << "# Default: disabled" << std::endl
    << "#" << std::endl;
  if (catdb->is_server_enabled)
    f << "enable_server = true" << std::endl;
  else
    f << "#   enable_server = false" << std::endl;
  f << std::endl
    << std::endl;

  f << "# Specifies the server port for the upscaledb server API. Default: 54123."
          << std::endl
    << "#" << std::endl;
  f << "#   server_port = " << catdb->server_port << std::endl;

  f << std::endl
    << std::endl;

  f << "# Table-specific configuration settings are prefixed with the table's "
        << std::endl
    << "# name, followed by a dot ('.') and the parameter name."
        << std::endl
    << "# Example: users.enable_compression = zlib"
        << std::endl
    << "#" << std::endl
    << "# Enables compression for the row data. The following codecs can be selected: "
        << std::endl
    << "# 'zlib', 'snappy', 'lzf' (zlib and snappy's availability depend on "
        << std::endl
    << "# upscaledb's compile-time configuration). Default: disabled."
        << std::endl
    << "# " << std::endl;
  Catalogue::Table *cattbl = catdb->tables.begin()->second;
  switch (cattbl->record_compression) {
    case UPS_COMPRESSOR_ZLIB:
      f << "    " << cattbl->name << ".enable_compression = zlib" << std::endl;
      break;
    case UPS_COMPRESSOR_SNAPPY:
      f << "    " << cattbl->name << ".enable_compression = snappy" << std::endl;
      break;
    case UPS_COMPRESSOR_LZF:
      f << "    " << cattbl->name << ".enable_compression = lzf" << std::endl;
      break;
    default:
      f << "#   " << cattbl->name << ".enable_compression = none" << std::endl;
      break;
  }
}

