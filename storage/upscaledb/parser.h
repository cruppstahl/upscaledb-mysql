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
#include <iostream>

#include <ups/upscaledb_int.h>

#include <boost/algorithm/string.hpp>

static bool
parse_single_comment(std::string &token, std::string &key, std::string &value)
{
  size_t pos;
  if ((pos = token.find("=")) == std::string::npos)
    return false;
  key = token.substr(0, pos - 1);
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
    if (!parse_single_comment(token, key, value))
      return false;
    if (!sink.add(key, value))
      return false;
    last = next + 1;
  }
  std::string token = s.substr(last);
  if (!parse_single_comment(token, key, value))
    return false;
  if (!sink.add(key, value))
    return false;

  return true;
}
