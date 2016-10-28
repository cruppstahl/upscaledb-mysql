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

#ifndef CATALOGUE_H__
#define CATALOGUE_H__

#include <map>
#include <vector>
#include <string>
#include <stdio.h>

#include <boost/thread/mutex.hpp>

#include <ups/upscaledb_int.h>
#include <ups/upscaledb_srv.h>

class Field;

namespace Catalogue {

//
// This struct stores information about a database index
//
struct Index {
  Index(ups_db_t *db_ = 0, Field *field_ = 0, bool enable_duplicates_ = false,
                  bool is_primary_index_ = false, uint32_t key_type_ = 0)
    : db(db_), field(field_), enable_duplicates(enable_duplicates_),
      is_primary_index(is_primary_index_), key_type(key_type_) {
  }

  // The upscaledb database structure
  ups_db_t *db;

  // The associated MySQL Field structure
  Field *field;

  // |true| if duplicates are enabled (for a non-unique index)
  bool enable_duplicates; 

  // |true| if this is the primary index
  bool is_primary_index; 

  // the upscaledb key type (UPS_TYPE_UINT32 etc)
  uint32_t key_type;
};


//
// This struct stores information about a MySQL table, its indices and the
// associated upscaledb databases.
//
struct Table {
  Table(std::string name_)
    : name(name_), ref_length(0), initial_autoinc_value(0), autoinc_value(0),
      record_compression(0) {
  }

  // Adds a configuration value that was read from the configuration file
  bool add_config_value(std::string &key, std::string &value, bool is_open);

  // the table name
  std::string name;

  // ref_length value of this table
  uint32_t ref_length;

  // initial AUTO_INCREMENT value
  uint64_t initial_autoinc_value;

  // current AUTO_INCREMENT value
  uint64_t autoinc_value;

  // The table's indices
  std::vector<Index> indices;

  // If there's no index then an auto-incremented primary key is created
  Index autoidx;

  // Configuration settings: record compression
  int record_compression;
};


//
// This struct stores information about a MySQL database and the associated
// upscaledb environment.
//
struct Database {
  enum {
    kDefaultServerPort = 54123,
    kDefaultCacheSize = 128 * 1024 * 1024
  };

  Database(std::string name_)
    : name(name_), env(0), srv(0), is_server_enabled(false),
      server_port(kDefaultServerPort), flags(UPS_ENABLE_TRANSACTIONS) {
  }

  // Adds a configuration value that was read from the configuration file
  bool add_config_value(std::string &key, std::string &value, bool is_open);

  // Adds the terminating element and sets the default cache size
  void finalize_config();

  // the database name
  std::string name;

  // The upscaledb environment with all tables of this database
  ups_env_t *env;

  // The upscaledb server
  ups_srv_t *srv;

  // The tables, indexed by their name
  typedef std::map<std::string, Table *> TableMap;
  TableMap tables;

  // Configuration setting: true if the server is enabled
  bool is_server_enabled;

  // Configuration setting: the server's port
  uint16_t server_port;

  // Configuration settings: flags for opening/creating the Environment
  uint32_t flags;

  // Configuration settings: parameters for opening/creating the Environment
  std::vector<ups_parameter_t> params;
};


// A mutex for accessing |databases|. Used to prevent race conditions in
// UpscaledbHandler::create and UpscaledbHandler::open, when multiple handler
// instances try to create a Catalogue::Database object for the same
// MySQL database
extern boost::mutex databases_mutex;

// Global map of catalogues> databases
typedef std::map<std::string, Database *> DatabaseMap;
extern DatabaseMap databases;

} // namespace Catalogue

#endif // CATALOGUE_H__
