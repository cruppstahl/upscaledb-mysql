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

#include "catalogue.h"

typedef std::pair<bool, std::string> ParserStatus;

extern ParserStatus
parse_comment_list(const char *comment, Catalogue::Table *cattbl);

extern ParserStatus
parse_config_file(const std::string &filename, Catalogue::Database *catdb,
                bool is_open);

extern void
write_configuration_settings(std::string env_name, const char *comment,
                Catalogue::Database *catdb);

#endif // CONFIGURATION_H__
