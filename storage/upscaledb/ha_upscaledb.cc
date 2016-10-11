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

#include <fstream>

#define MYSQL_SERVER
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION

#include "log.h"
#include "ha_upscaledb.h"
#include "probes_mysql.h"
#include "sql_plugin.h"
#include "sql_executor.h"

#include <ups/upscaledb_uqi.h>
#include <ups/upscaledb_int.h>
#include <ups/upscaledb_srv.h>

#include <boost/thread/mutex.hpp>
#include <boost/filesystem.hpp>

// helper macros to improve CPU branch prediction
#if defined __GNUC__
#   define likely(x) __builtin_expect ((x), 1)
#   define unlikely(x) __builtin_expect ((x), 0)
#else
#   define likely(x) (x)
#   define unlikely(x) (x)
#endif

#define CUSTOM_COMPARE_NAME "varlencmp"

struct CreateInfo {
  CreateInfo(HA_CREATE_INFO *ci)
    : autoinc_value(ci->auto_increment_value) {
  }

  uint64_t autoinc_value;
};

struct KeyPart {
  KeyPart(uint32_t type_ = 0, uint32_t length_ = 0)
    : type(type_), length(length_) {
  }

  uint32_t type;
  uint32_t length;
};

struct CustomCompareState {
  CustomCompareState(KEY *key) {
    if (key) {
      for (uint32_t i = 0; i < key->user_defined_key_parts; i++) {
        KEY_PART_INFO *key_part = &key->key_part[i];
        parts.push_back(KeyPart(key_part->type, key_part->length));
      }
    }
  }

  std::vector<KeyPart> parts;
};

static inline int
encoded_length_bytes(uint32_t type)
{
  if (type == HA_KEYTYPE_VARTEXT1
          || type == HA_KEYTYPE_VARBINARY1)
    return 1;
  if (type == HA_KEYTYPE_VARTEXT2
          || type == HA_KEYTYPE_VARBINARY2)
    return 2;
  return 0;
}

static TABLE *recovery_table;

static int
custom_compare_func(ups_db_t *db, const uint8_t *lhs, uint32_t lhs_length,
                const uint8_t *rhs, uint32_t rhs_length)
{
  CustomCompareState *ccs = (CustomCompareState *)ups_get_context_data(db, 1);
  CustomCompareState tmp_ccs(ccs != 0
                        ? 0
                        : &recovery_table->key_info[ups_db_get_name(db) - 2]);
  // during recovery, the context pointer was not yet installed. in this case
  // we use the temporary one.
  if (ccs == 0)
    ccs = &tmp_ccs;

  for (size_t i = 0; i < ccs->parts.size(); i++) {
    KeyPart *key_part = &ccs->parts[i];

    int cmp;

    // TODO use *real* comparison function for other columns like uint32,
    // float, ...?
    switch (encoded_length_bytes(key_part->type)) {
      case 0: // fixed length columns
        cmp = ::memcmp(lhs, rhs, key_part->length);
        lhs_length = key_part->length;
        rhs_length = key_part->length;
        break;
      case 1:
        lhs_length = *lhs;
        lhs += 1;
        rhs_length = *rhs;
        rhs += 1;
        cmp = ::memcmp(lhs, rhs, std::min(lhs_length, rhs_length));
        break;
      case 2:
        lhs_length = *(uint16_t *)lhs;
        lhs += 2;
        rhs_length = *(uint16_t *)rhs;
        rhs += 2;
        cmp = ::memcmp(lhs, rhs, std::min(lhs_length, rhs_length));
        break;
      default:
        assert(!"shouldn't be here");
    }

    if (cmp != 0)
      return cmp;
    // cmp == 0
    if (lhs_length < rhs_length)
      return -1;
    if (lhs_length > rhs_length)
      return +1;

    // both keys are equal - continue with the next one
    lhs += lhs_length;
    rhs += rhs_length;
  }

  // still here? then both keys must be equal
  return 0;
}

static std::map<uint16_t, ups_srv_t *> servers;
static boost::mutex servers_mutex;

static std::map<std::string, UpscaledbShare *> shares;
static boost::mutex shares_mutex;

static std::map<std::string, CreateInfo *> create_infos;
static boost::mutex create_infos_mutex;

static void
log_error_impl(const char *file, int line, const char *function,
                ups_status_t st);

#define log_error(f, s) log_error_impl(__FILE__, __LINE__, f, s)

// A class which aborts a transaction when it's destructed
struct TxnProxy {
  TxnProxy(ups_env_t *env) {
    ups_status_t st = ups_txn_begin(&txn, env, 0, 0, 0);
    if (unlikely(st != 0)) {
      log_error("ups_txn_begin", st);
      txn = 0;
    }
  }

  ~TxnProxy() {
    if (txn != 0) {
      ups_status_t st = ups_txn_abort(txn, 0);
      if (unlikely(st != 0))
        log_error("ups_txn_abort", st);
    }
  }

  ups_status_t commit() {
    ups_status_t st = ups_txn_commit(txn, 0);
    if (likely(st == 0))
      txn = 0;
    return st;
  }

  ups_txn_t *txn;
};

// A class which closes a cursor when going out of scope
struct CursorProxy {
  CursorProxy(ups_cursor_t *c = 0)
    : cursor(c) {
  }

  CursorProxy(ups_db_t *db, ups_txn_t *txn = 0) {
    ups_status_t st = ups_cursor_create(&cursor, db, txn, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_create", st);
      cursor = 0;
    }
  }

  ~CursorProxy() {
    if (cursor != 0) {
      ups_status_t st = ups_cursor_close(cursor);
      if (unlikely(st != 0))
        log_error("ups_cursor_close", st);
    }
  }

  ups_cursor_t *detach() {
    ups_cursor_t *c = cursor;
    cursor = 0;
    return c;
  }

  ups_cursor_t *cursor;
};

static inline void
store_create_info(const char *table_name, CreateInfo *create_info)
{
  boost::mutex::scoped_lock lock(create_infos_mutex);
  create_infos[table_name] = create_info;
}

static inline CreateInfo *
fetch_create_info(const char *table_name)
{
  boost::mutex::scoped_lock lock(create_infos_mutex);
  return create_infos[table_name];
}

static inline void
rename_create_info(const char *old_name, const char *new_name)
{
  boost::mutex::scoped_lock lock(create_infos_mutex);
  CreateInfo *ci = create_infos[old_name];
  create_infos[new_name] = ci;
  create_infos.erase(create_infos.find(old_name));
}

static inline void
store_share_locked(const char *table_name, UpscaledbShare *share)
{
  shares[table_name] = share;
}

static inline void
close_and_remove_share(const char *table_name, UpscaledbShare *share)
{
  boost::mutex::scoped_lock lock1(shares_mutex);
  boost::mutex::scoped_lock lock2(servers_mutex);
  ups_env_t *env = shares[table_name] ? shares[table_name]->env : 0;
  ups_srv_t *srv = share ? servers[share->config.server_port] : 0;

  // also remove the environment from a server
  if (env && srv) {
    (void)ups_srv_remove_env(srv, env);
  }

  // close the environment
  if (env) {
    ups_status_t st = ups_env_close(env, UPS_AUTO_CLEANUP);
    if (unlikely(st != 0))
      log_error("ups_env_close", st);
  }

  shares.erase(shares.find(table_name));
}

static inline void
log_error_impl(const char *file, int line, const char *function,
                ups_status_t st)
{
  sql_print_error("%s[%d] %s: failed with error %d (%s)", file, line,
                  function, st, ups_strerror(st));
}

static handler *
upscaledb_create_handler(handlerton *hton, TABLE_SHARE *table,
                MEM_ROOT *mem_root)
{
  return new (mem_root) UpscaledbHandler(hton, table);
}

static int
upscaledb_init_func(void *p)
{
  DBUG_ENTER("upscaledb_init_func");

  handlerton *hton = (handlerton *)p;
  hton->state = SHOW_OPTION_YES;
  hton->create = upscaledb_create_handler;
  hton->flags = 0;
  hton->system_database = 0;
  hton->is_supported_system_table = 0;

  DBUG_RETURN(0);
}

static inline std::pair<uint32_t, uint32_t> // <key, size>
table_field_info(KEY_PART_INFO *key_part)
{
  Field *field = key_part->field;
  Field_temporal *f;

  // if key can be null: use a variable-length key
  if (key_part->null_bit)
    return std::make_pair((uint32_t)UPS_TYPE_BINARY, UPS_KEY_SIZE_UNLIMITED);

  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_YEAR:
      return std::make_pair((uint32_t)UPS_TYPE_UINT8, 1u);
    case MYSQL_TYPE_SHORT:
      return std::make_pair((uint32_t)UPS_TYPE_UINT16, 2u);
    case MYSQL_TYPE_LONG:
      return std::make_pair((uint32_t)UPS_TYPE_UINT32, 4u);
    case MYSQL_TYPE_LONGLONG:
      return std::make_pair((uint32_t)UPS_TYPE_UINT64, 8u);
    case MYSQL_TYPE_FLOAT:
      return std::make_pair((uint32_t)UPS_TYPE_REAL32, 4u);
    case MYSQL_TYPE_DOUBLE:
      return std::make_pair((uint32_t)UPS_TYPE_REAL64, 8u);
    case MYSQL_TYPE_DATE:
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 3u);
    case MYSQL_TYPE_TIME:
      f = (Field_temporal *)field;
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 3u + f->decimals());
    case MYSQL_TYPE_DATETIME:
      f = (Field_temporal *)field;
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 5u + f->decimals());
    case MYSQL_TYPE_TIMESTAMP:
      f = (Field_temporal *)field;
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 4u + f->decimals());
    case MYSQL_TYPE_SET:
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, field->pack_length());
    default:
      break;
  }

  // includes ENUM, CHAR, BINARY...
  // For multi-part keys we return the length that is required to store the
  // full column. For single-part keys we only store the actually used
  // length.
  switch (key_part->type) {
    case HA_KEYTYPE_BINARY:
    case HA_KEYTYPE_TEXT:
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, key_part->length);
    case HA_KEYTYPE_VARTEXT1:
    case HA_KEYTYPE_VARTEXT2:
    case HA_KEYTYPE_VARBINARY1:
    case HA_KEYTYPE_VARBINARY2:
      return std::make_pair((uint32_t)UPS_TYPE_BINARY, 0);
  }

  return std::make_pair((uint32_t)UPS_TYPE_BINARY, 0u);
}

static inline std::pair<uint32_t, uint32_t> // <key, size>
table_key_info(KEY *key_info)
{
  // if this index is made from a single column: return type/size of this column
  if (key_info->user_defined_key_parts == 1)
    return table_field_info(key_info->key_part);

  // Otherwise accumulate the total size of all columns which form this
  // key.
  // If any key has variable length then we need to use our custom compare
  // function.
  uint32_t total_size = 0;
  bool need_custom_compare = false;
  for (uint32_t i = 0; i < key_info->user_defined_key_parts; i++) {
    std::pair<uint32_t, uint32_t> p = table_field_info(&key_info->key_part[i]);
    if (total_size == UPS_KEY_SIZE_UNLIMITED
            || p.second == 0
            || p.second == UPS_KEY_SIZE_UNLIMITED)
      total_size = UPS_KEY_SIZE_UNLIMITED;
    else
      total_size += p.second;

    switch (key_info->key_part[i].type) {
      case HA_KEYTYPE_VARTEXT1:
      case HA_KEYTYPE_VARTEXT2:
      case HA_KEYTYPE_VARBINARY1:
      case HA_KEYTYPE_VARBINARY2:
        need_custom_compare = true;
    }
  }

  return std::make_pair(need_custom_compare
                            ? UPS_TYPE_CUSTOM
                            : UPS_TYPE_BINARY, total_size);
}

static inline std::string
format_environment_name(const char *name)
{
  std::string n(name);
  return n + ".ups";
}

static inline ups_key_t
key_from_single_row(TABLE *table, const uchar *buf, KEY_PART_INFO *key_part)
{
  uint16_t key_size = (uint16_t)key_part->length;
  uint32_t offset = key_part->offset;

  switch (encoded_length_bytes(key_part->type)) {
    case 1:
      key_size = buf[offset];
      offset++;
      key_size = std::min(key_size, key_part->length);
      break;
    case 2:
      key_size = *(uint16_t *)&buf[offset];
      offset += 2;
      key_size = std::min(key_size, key_part->length);
      break;
  }

  ups_key_t key = ups_make_key((uchar *)buf + offset, key_size);
  return key;
}

static inline ups_key_t
key_from_row(TABLE *table, const uchar *buf, int index, ByteVector &arena)
{
  arena.clear();

  if (likely(table->key_info[index].user_defined_key_parts == 1))
    return key_from_single_row(table, buf, table->key_info[index].key_part);

  arena.clear();
  for (uint32_t i = 0; i < table->key_info[index].user_defined_key_parts; i++) {
    KEY_PART_INFO *key_part = &table->key_info[index].key_part[i];
    uint16_t key_size = (uint16_t)key_part->length;
    uint8_t *p = (uint8_t *)buf + key_part->offset;

    switch (encoded_length_bytes(key_part->type)) {
      case 1:
        key_size = *p;
        arena.insert(arena.end(), p, p + 1);
        p += 1;
        break;
      case 2:
        key_size = *(uint16_t *)p;
        arena.insert(arena.end(), p, p + 2);
        p += 2;
        break;
      case 0: // fixed length columns
        arena.insert(arena.end(), p, p + key_size);
        continue;
      default:
        assert(!"shouldn't be here");
    }

    // append the data to our memory buffer
    // BLOB/TEXT data is encoded as a "pointer-to-pointer" in the stream
    if ((key_part->key_part_flag & HA_VAR_LENGTH_PART) == 0)
      p = *(uint8_t **)p;
    arena.insert(arena.end(), p, p + key_size);
  }

  ups_key_t key = ups_make_key(arena.data(), (uint16_t)arena.size());
  return key;
}

static inline bool
row_is_fixed_length(TABLE *table)
{
  return table->s->blob_fields + table->s->varchar_fields == 0;
}

static inline void
extract_varchar_field_info(Field *field, uint32_t *len_bytes,
                uint32_t *field_size, uint8_t *src)
{
  // see Field_blob::Field_blob() (in field.h) - need 1-4 bytes to
  // store the real size
  if (likely(field->field_length <= 255)) {
    *field_size = *src;
    *len_bytes = 1;
    return;
  }
  if (likely(field->field_length <= 65535)) {
    *field_size = *(uint16_t *)src;
    *len_bytes = 2;
    return;
  }
  if (likely(field->field_length <= 16777215)) {
    *field_size = (src[2] << 16) | (src[1] << 8) | (src[0] << 0);
    *len_bytes = 3;
    return;
  }
  *field_size = *(uint32_t *)src;
  *len_bytes = 4;
}

// transforms record from the condensed upscaledb format to the MySQL row
// format
static inline ups_record_t
unpack_record(TABLE *table, ups_record_t *record, uint8_t *buf)
{
  assert(!row_is_fixed_length(table));

  uint8_t *src = (uint8_t *)record->data;
  uint8_t *dst = buf;

  // copy the "null bytes" descriptors
  for (uint32_t i = 0; i < table->s->null_bytes; i++) {
    *dst = *src;
    dst++;
    src++;
  }

  uint32_t i = 0;
  for (Field **field = table->field; *field != 0; field++, i++) {
    uint32_t size;
    uint32_t type = (*field)->type();

    if (type == MYSQL_TYPE_VARCHAR) {
      uint32_t len_bytes;
      extract_varchar_field_info(*field, &len_bytes, &size, src);
      ::memcpy(dst, src, size + len_bytes);
      if (likely((*field)->pack_length() > size + len_bytes))
        ::memset(dst + size + len_bytes, 0,
                        (*field)->pack_length() - (size + len_bytes));
      dst += (*field)->pack_length();
      src += size + len_bytes;
      continue;
    }

    if (type == MYSQL_TYPE_TINY_BLOB
            || type == MYSQL_TYPE_BLOB
            || type == MYSQL_TYPE_MEDIUM_BLOB
            || type == MYSQL_TYPE_LONG_BLOB) {
      size = *(uint32_t *)src;
      src += sizeof(uint32_t);
      Field_blob *blob = *(Field_blob **)field;
      switch (blob->pack_length() - 8) {
        case 1:
          *dst = (uint8_t)size;
          break;
        case 2:
          *(uint16_t *)dst = (uint16_t)size;
          break;
        case 3: // TODO not yet tested...
          dst[2] = (size & 0xff0000) >> 16;
          dst[1] = (size & 0xff00) >> 8;
          dst[0] = (size &  0xff);
          break;
        case 4:
          *(uint32_t *)dst = size;
          break;
        case 8:
          *(uint64_t *)dst = size;
          break;
        default:
          assert(!"not yet implemented");
          break;
      }

      dst += (*field)->pack_length() - 8;
      *(uint8_t **)dst = src;
      src += size;
      dst += sizeof(uint8_t *);
      continue;
    }

    size = (*field)->key_length();
    ::memcpy(dst, src, size);
    src += size;
    dst += size;
  }

  ups_record_t r = ups_make_record(buf, (uint32_t)(dst - buf));
  return r;
}

// transforms record from the MySQL row format to the condensed
// upscaledb format
static inline ups_record_t
pack_record(TABLE *table, uint8_t *buf, ByteVector &arena)
{
  assert(!row_is_fixed_length(table));

  uint8_t *src = buf;
  arena.clear();
  arena.reserve(1024);
  uint8_t *dst = arena.data();

  // copy the "null bytes" descriptors
  arena.resize(table->s->null_bytes);
  for (uint32_t i = 0; i < table->s->null_bytes; i++) {
    *dst = *src;
    dst++;
    src++;
  }

  for (Field **field = table->field; *field != 0; field++) {
    uint32_t size;
    uint32_t type = (*field)->type();

    if (type == MYSQL_TYPE_VARCHAR) {
      uint32_t len_bytes;
      extract_varchar_field_info(*field, &len_bytes, &size, src);

      // make sure we have sufficient space
      uint32_t pos = dst - arena.data();
      arena.resize(arena.size() + size + len_bytes);
      dst = &arena[pos];

      ::memcpy(dst, src, size + len_bytes);
      src += (*field)->pack_length();
      dst += size + len_bytes;
      continue;
    }

    if (type == MYSQL_TYPE_TINY_BLOB
            || type == MYSQL_TYPE_BLOB
            || type == MYSQL_TYPE_MEDIUM_BLOB
            || type == MYSQL_TYPE_LONG_BLOB) {
      uint32_t packlength;
      uint32_t length;
      extract_varchar_field_info(*field, &packlength, &length, src);

      // make sure we have sufficient space
      uint32_t pos = dst - arena.data();
      arena.resize(arena.size() + sizeof(uint32_t) + length);
      dst = &arena[pos];

      *(uint32_t *)dst = length;
      dst += sizeof(uint32_t);
      ::memcpy(dst, *(char **)(src + packlength), length);
      dst += length;
      src += packlength + sizeof(void *);
      continue;
    }

    size = (*field)->key_length();

    // make sure we have sufficient space
    uint32_t pos = dst - arena.data();
    arena.resize(arena.size() + size);
    dst = &arena[pos];

    ::memcpy(dst, src, size);
    src += size;
    dst += size;
  }

  ups_record_t r = ups_make_record(arena.data(), (uint32_t)(dst - &arena[0]));
  return r;
}

static inline ups_record_t
record_from_row(TABLE *table, uint8_t *buf, ByteVector &arena)
{
  // fixed length rows do not need any packing
  if (row_is_fixed_length(table)) {
    ups_record_t r = ups_make_record(buf,
                    (uint32_t)table->s->stored_rec_length);
    return r;
  }

  // but if rows have variable length (i.e. due to a VARCHAR field) we
  // pack them to save space
  return pack_record(table, buf, arena);
}

static inline uint16_t
dbname(int index)
{
  return index + 2;
}

static inline bool
key_exists(ups_db_t *db, ups_key_t *key)
{
  ups_record_t record = ups_make_record(0, 0);
  ups_status_t st = ups_db_find(db, 0, key, &record, 0);
  return st == 0;
}

static inline MaxKeyCache *
initialize_max_key_cache(ups_db_t *db, uint32_t key_type)
{
  CursorProxy cp(db);
  ups_key_t key = ups_make_key(0, 0);
  ups_status_t st = ups_cursor_move(cp.cursor, &key, 0, UPS_CURSOR_LAST);

  switch (key_type) {
    case UPS_TYPE_UINT8:
      return new MaxKeyCachePod<uint8_t>(st == 0 ? &key : 0);
    case UPS_TYPE_UINT16:
      return new MaxKeyCachePod<uint16_t>(st == 0 ? &key : 0);
    case UPS_TYPE_UINT32:
      return new MaxKeyCachePod<uint32_t>(st == 0 ? &key : 0);
    case UPS_TYPE_UINT64:
      return new MaxKeyCachePod<uint64_t>(st == 0 ? &key : 0);
    case UPS_TYPE_REAL32:
      return new MaxKeyCachePod<float>(st == 0 ? &key : 0);
    case UPS_TYPE_REAL64:
      return new MaxKeyCachePod<double>(st == 0 ? &key : 0);
    default:
      return new DisabledMaxKeyCache();
  }
}

static inline uint64_t
initialize_autoinc(const char *name, KEY_PART_INFO *key_part, ups_db_t *db)
{
  // if the database is empty: read from the cached create_info object
  CreateInfo *create_info = fetch_create_info(name);
  if (create_info)
    return create_info->autoinc_value > 0 ? create_info->autoinc_value - 1 : 0;

  // if the database is not empty: read the maximum value
  if (db) {
    CursorProxy cp(db, 0);
    if (unlikely(cp.cursor == 0))
      return 0;

    ups_key_t key = ups_make_key(0, 0);
    ups_status_t st = ups_cursor_move(cp.cursor, &key, 0, UPS_CURSOR_LAST);
    if (st == 0) {
      uint8_t *p = (uint8_t *)key.data;
      // the auto-increment key is always at the beginning of |p|
      if (key_part->length == 1)
        return *p;
      if (key_part->length == 2)
        return *(uint16_t *)p;
      if (key_part->length == 3)
        return (p[0] << 16) | (p[1] << 8) | p[0];
      if (key_part->length == 4)
        return *(uint32_t *)p;
      if (key_part->length == 8)
        return *(uint64_t *)p;
      assert(!"shouldn't be here");
      return 0;
    }
  }

  return 0;
}

static ups_status_t
delete_all_databases(UpscaledbShare *share, TABLE *table)
{
  ups_status_t st;
  uint16_t names[1024];
  uint32_t length = 1024;
  st = ups_env_get_database_names(share->env, names, &length);
  if (unlikely(st)) {
    log_error("ups_env_get_database_names", st);
    return 1;
  }

  if (share->autoidx.db) {
    ups_db_close(share->autoidx.db, 0);
    share->autoidx.db = 0;
  }

  for (size_t i = 0; i < share->dbmap.size(); i++) {
    ups_db_close(share->dbmap[i].db, 0);
  }
  share->dbmap.clear();

  for (uint32_t i = 0; i < length; i++) {
    st = ups_env_erase_db(share->env, names[i], 0);
    if (unlikely(st)) {
      log_error("ups_env_erase_db", st);
      return 1;
    }
  }

  return 0;
}

static ups_status_t
create_all_databases(UpscaledbShare *share, TABLE *table,
                Configuration *config)
{
  ups_db_t *db;
  int num_indices = table->s->keys;
  bool has_primary_index = false;

  // key info for the primary key
  std::pair<uint32_t, uint32_t> primary_type_info;

  assert(share->dbmap.empty());

  ups_register_compare(CUSTOM_COMPARE_NAME, custom_compare_func);

  // foreach indexed field: create a database which stores this index
  KEY *key_info = table->key_info;
  for (int i = 0; i < num_indices; i++, key_info++) {
    Field *field = key_info->key_part->field;

    std::pair<uint32_t, uint32_t> type_info;
    type_info = table_key_info(key_info);
    uint32_t key_type = type_info.first;
    uint32_t key_size = type_info.second;

    bool enable_duplicates = true;
    bool is_primary_key = false;

    if (0 == ::strcmp("PRIMARY", key_info->name))
      is_primary_key = true;

    if (key_info->actual_flags & HA_NOSAME)
      enable_duplicates = false;

    // If there is only one key then pretend that it's the primary key
    if (num_indices == 1)
      is_primary_key = true;

    if (is_primary_key)
      has_primary_index = true;

    // enable duplicates for all indices but the first
    uint32_t flags = 0;
    if (enable_duplicates)
      flags |= UPS_ENABLE_DUPLICATE_KEYS;

    int p = 1;
    ups_parameter_t params[] = {
        {UPS_PARAM_KEY_TYPE, key_type},
        {0, 0},
        {0, 0},
        {0, 0},
        {0, 0},
        {0, 0},
        {0, 0},
        {0, 0}
    };

    // set a key size if the key is CHAR(n) or BINARY(n)
    if (key_size != 0 && key_type == UPS_TYPE_BINARY) {
      params[p].name = UPS_PARAM_KEY_SIZE;
      params[p].value = key_size;
      p++;
    }

    // this index requires a custom compare callback function?
    if (key_type == UPS_TYPE_CUSTOM) {
      params[p].name = UPS_PARAM_CUSTOM_COMPARE_NAME;
      params[p].value = (uint64_t)CUSTOM_COMPARE_NAME;
      p++;
    }

    // for secondary indices: set the record type to the same type as
    // the primary key
    if (!is_primary_key) {
      params[p].name = UPS_PARAM_RECORD_TYPE;
      params[p].value = primary_type_info.first;
      p++;
      if (primary_type_info.second != 0) {
        params[p].name = UPS_PARAM_RECORD_SIZE;
        params[p].value = primary_type_info.second;
        p++;
      }
    }

    // primary key: if a record has fixed length then set a parameter
    if (is_primary_key && row_is_fixed_length(table)) {
      params[p].name = UPS_PARAM_RECORD_SIZE;
      params[p].value = table->s->stored_rec_length;
      p++;
    }

    // is record compression enabled?
    if (is_primary_key && config->record_compression) {
      params[p].name = UPS_PARAM_RECORD_COMPRESSION;
      params[p].value = config->record_compression;
      p++;
    }

    ups_status_t st = ups_env_create_db(share->env, &db, dbname(i),
                                flags, params);
    if (unlikely(st != 0)) {
      log_error("ups_env_create_db", st);
      return st;
    }

    DbDesc dbdesc(db, field, enable_duplicates, is_primary_key);
    dbdesc.max_key_cache = initialize_max_key_cache(db, key_type);
    share->dbmap.push_back(dbdesc);
  }

  // no primary index? then create a record-number database
  if (!has_primary_index) {
    ups_parameter_t params[] = {
      {0, 0},
      {0, 0}
    };

    if (config->record_compression) {
      params[0].name = UPS_PARAM_RECORD_COMPRESSION;
      params[0].value = config->record_compression;
    }

    ups_status_t st = ups_env_create_db(share->env, &db, 1,
                                UPS_RECORD_NUMBER32, &params[0]);
    if (unlikely(st != 0)) {
      log_error("ups_env_create_db", st);
      return st;
    }
    share->autoidx = DbDesc(db, 0, false, true);
  }

  return 0;
}

static int
attach_to_server(ups_env_t *env, const char *path, Configuration *config)
{
  ups_status_t st;
  assert(config->is_server_enabled);

  boost::mutex::scoped_lock lock(servers_mutex);
  ups_srv_t *srv = servers[config->server_port];
  if (!srv) {
    ups_srv_config_t cfg;
    ::memset(&cfg, 0, sizeof(cfg));
    cfg.port = config->server_port;

    st = ups_srv_init(&cfg, &srv);
    if (unlikely(st != 0)) {
      log_error("ups_srv_init", st);
      return 1;
    }
    servers[config->server_port] = srv;
  }
  st = ups_srv_add_env(srv, env, path);
  if (unlikely(st != 0)) {
    log_error("ups_srv_add_env", st);
    return 1;
  }
  return 0;
}

// |name|: full qualified name, i.e. "/database/table"
// |table|: table info, field definitions
// |create_info|: additional information about client, connection etc
int
UpscaledbHandler::create(const char *name, TABLE *table,
                HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("UpscaledbHandler::create");

  assert(share == 0);

  // clean up any remaining handles
  // TODO really?
  // close_and_remove_share(name, share);

  UpscaledbShare tmpshare;
  std::string env_name = format_environment_name(name);

  // parse the configuration settings in the table's COMMENT
  tmpshare.config = Configuration(false);
  if (create_info->comment.length > 0) {
    if (!parse_comment_list(create_info->comment.str, tmpshare.config)) {
      sql_print_error("Invalid \"CREATE TABLE\" comment");
      DBUG_RETURN(1);
    }
  }

  // write the settings to the configuration file; the user can then
  // modify them
  write_configuration_settings(env_name, create_info->comment.str,
                  tmpshare.config);

  ups_status_t st = ups_env_create(&tmpshare.env, env_name.c_str(),
                            tmpshare.config.flags, 0644,
                            tmpshare.config.params.empty() == false
                                ? &tmpshare.config.params[0]
                                : 0);
  if (st != 0) {
    log_error("ups_env_create", st);
    DBUG_RETURN(1);
  }

  // create a database for each index.
  st = create_all_databases(&tmpshare, table, &tmpshare.config);

  // close Environment and all databases - it will be opened again in
  // open()
  (void)ups_env_close(tmpshare.env, UPS_AUTO_CLEANUP);

  // store the create_info object; might be required later
  if (likely(st == 0))
    store_create_info(name, new CreateInfo(create_info));

  DBUG_RETURN(st ? 1 : 0);
}

int
UpscaledbHandler::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("UpscaledbHandler::open");

  ups_register_compare(CUSTOM_COMPARE_NAME, custom_compare_func);
  ups_set_committed_flush_threshold(30);
  record_arena.resize(table->s->rec_buff_length);
  ref_length = 0;

  assert(share == 0);
  UpscaledbTableShare *table_share = allocate_or_get_share();
  share = table_share->share;
  if (share) { // already initialized?
    ref_length = share->ref_length;
    DBUG_RETURN(0);
  }
  thr_lock_data_init(&table_share->lock, &lock_data, NULL);

  //close_and_remove_share(name, share);
  boost::mutex::scoped_lock lock1(shares_mutex);

  // it's possible that a different thread has opened the Environment in
  // the meantime
  share = shares[name];
  if (share) {
    ref_length = share->ref_length;
    DBUG_RETURN(0);
  }

  if (!share)
    share = new UpscaledbShare;

  std::string env_name = format_environment_name(name);

  // parse the configuration settings in the table's .cnf file
  share->config = Configuration(true);
  if (!parse_file(env_name + ".cnf", share->config)) {
    sql_print_error("Invalid configuration file '%s.cnf'", env_name.c_str());
    DBUG_RETURN(1);
  }

  // Temporary databases are opened in memory; read-only databases are
  // opened in read-only mode
  if (mode & O_RDONLY)
    share->config.flags |= UPS_READ_ONLY;
  if (mode & HA_OPEN_TMP_TABLE)
    share->config.flags |= UPS_ENABLE_TRANSACTIONS | UPS_IN_MEMORY;
  else
    share->config.flags |= UPS_ENABLE_TRANSACTIONS | UPS_AUTO_RECOVERY;

  // open the Environment
  recovery_table = table;
  ups_status_t st = ups_env_open(&share->env, env_name.c_str(),
                                share->config.flags,
                                share->config.params.empty() == false
                                    ? &share->config.params[0]
                                    : 0);
  if (unlikely(st != 0)) {
    log_error("ups_env_open", st);
    DBUG_RETURN(1);
  }

  store_share_locked(name, share);

  ups_db_t *db;

  int num_indices = table->s->keys;
  bool has_primary_index = false;

  // foreach indexed field: open the database which stores this index
  KEY *key_info = table->key_info;
  for (int i = 0; i < num_indices; i++, key_info++) {
    Field *field = key_info->key_part->field;

    bool is_primary_key = false;
    bool enable_duplicates = true;
    if (0 == ::strcmp("PRIMARY", key_info->name))
      is_primary_key = true;

    if (key_info->actual_flags & HA_NOSAME)
      enable_duplicates = false;

    // If there is only one key then pretend that it's the primary key!
    if (num_indices == 1)
      is_primary_key = true;

    if (is_primary_key) {
      has_primary_index = true;
      share->ref_length = ref_length = table->key_info[0].key_length;
    }

    st = ups_env_open_db(share->env, &db, dbname(i), 0, 0);
    if (st != 0) {
      log_error("ups_env_open_db", st);
      DBUG_RETURN(1);
    }

    // This leaks an object, but they're so tiny. Ignored for now.
    ups_set_context_data(db, new CustomCompareState(&table->key_info[i]));

    std::pair<uint32_t, uint32_t> type_info;
    type_info = table_key_info(key_info);
    uint32_t key_type = type_info.first;

    DbDesc dbdesc(db, field, enable_duplicates, is_primary_key);
    dbdesc.max_key_cache = initialize_max_key_cache(db, key_type);
    share->dbmap.push_back(dbdesc);

    // is this an auto-increment field? if the database is filled then read
    // the maximum value, otherwise initialize from the cached create_info 
    if (field == table->found_next_number_field) {
      for (uint32_t k = 0; k < key_info->user_defined_key_parts; k++) {
        if (key_info->key_part[k].field == field) {
          share->autoinc_value = initialize_autoinc(name,
                          &key_info->key_part[k], db);
          share->initial_autoinc_value = share->autoinc_value + 1;
          break;
        }
      }
    }
  }

  // no primary index? then create a record-number database
  if (!has_primary_index) {
    st = ups_env_open_db(share->env, &db, 1, 0, 0);
    if (unlikely(st != 0)) {
      log_error("ups_env_open_db", st);
      DBUG_RETURN(1);
    }

    share->ref_length = ref_length = sizeof(uint32_t);
    share->autoidx = DbDesc(db, 0, false, true);
  }

  if (share->config.is_server_enabled)
    DBUG_RETURN(attach_to_server(share->env, name + 1, // skip leading "."
                            &share->config));

  DBUG_RETURN(0);
}

int
UpscaledbHandler::close()
{
  DBUG_ENTER("UpscaledbHandler::close");

  if (cursor)
    ups_cursor_close(cursor);

  DBUG_RETURN(0);
}

static inline int
insert_auto_index(UpscaledbShare *share, TABLE *table, uint8_t *buf,
                ups_txn_t *txn, ByteVector &key_arena, ByteVector &record_arena)
{
  ups_key_t key = ups_make_key(0, 0);
  ups_record_t record = record_from_row(table, buf, record_arena);

  ups_status_t st = ups_db_insert(share->autoidx.db, txn, &key, &record, 0);
  if (unlikely(st != 0)) {
    log_error("ups_db_insert", st);
    return 1;
  }

  // Need to copy the key in the ByteVector - it will be required later
  key_arena.resize(key.size);
  ::memcpy(key_arena.data(), key.data, key.size);
  return 0;
}

static inline int
insert_primary_key(DbDesc *dbdesc, TABLE *table, uint8_t *buf,
                ups_txn_t *txn, ByteVector &key_arena, ByteVector &record_arena)
{
  ups_key_t key = key_from_row(table, buf, 0, key_arena);
  ups_record_t record = record_from_row(table, buf, record_arena);

  // check if the key is greater than the current maximum. If yes then the
  // key is unique, and we can specify the flag UPS_OVERWRITE (which is much
  // faster)
  uint32_t flags = 0;
  if (likely(dbdesc->enable_duplicates))
    flags = UPS_DUPLICATE;
  if (dbdesc->max_key_cache->compare_and_update(&key)) {
    assert(!key_exists(dbdesc->db, &key));
    flags = UPS_OVERWRITE;
  }

  ups_status_t st = ups_db_insert(dbdesc->db, txn, &key, &record, flags);

  if (unlikely(st == UPS_DUPLICATE_KEY))
    return HA_ERR_FOUND_DUPP_KEY;
  if (unlikely(st != 0)) {
    log_error("ups_db_insert", st);
    return 1;
  }

  // Need to copy the key in the ByteVector - it will be required later
  // TODO it's copied *from* key_arena *to* key_arena - says valgrind
  // TODO looks like this is not required
  key_arena.resize(key.size);
  ::memmove(key_arena.data(), key.data, key.size);
  return 0;
}

static inline int
insert_secondary_key(DbDesc *dbdesc, TABLE *table, int index,
                uint8_t *buf, ups_txn_t *txn, ByteVector &key_arena,
                ByteVector &primary_key)
{
  // The record of the secondary index is the primary key of the row
  ups_record_t record = ups_make_record(primary_key.data(),
                  (uint16_t)primary_key.size());

  // The actual key is the column's value
  ups_key_t key = key_from_row(table, buf, index, key_arena);

  uint32_t flags = 0;
  if (likely(dbdesc->enable_duplicates))
    flags = UPS_DUPLICATE;
  else if (dbdesc->max_key_cache->compare_and_update(&key)) {
    assert(!key_exists(dbdesc->db, &key));
    flags = UPS_OVERWRITE;
  }

  ups_status_t st = ups_db_insert(dbdesc->db, txn, &key, &record, flags);
  if (unlikely(st == UPS_DUPLICATE_KEY))
    return HA_ERR_FOUND_DUPP_KEY;
  if (unlikely(st != 0)) {
    log_error("ups_db_insert", st);
    return 1;
  }
  return 0;
}

static inline int
insert_multiple_indices(UpscaledbShare *share, TABLE *table, uint8_t *buf,
                ups_txn_t *txn, ByteVector &key_arena, ByteVector &record_arena)
{
  // is this an automatically generated index?
  if (share->autoidx.db) {
    int rc = insert_auto_index(share, table, buf, txn, key_arena, record_arena);
    if (unlikely(rc))
      return rc;
  }

  for (int index = 0; index < (int)share->dbmap.size(); index++) {
    // is this the primary index?
    if (share->dbmap[index].is_primary_index) {
      assert(index == 0);
      int rc = insert_primary_key(&share->dbmap[index], table, buf, txn,
                      key_arena, record_arena);
      if (unlikely(rc))
        return rc;
    }
    // is this a secondary index? the last parameter is a ByteVector with
    // the primary key.
    else {
      int rc = insert_secondary_key(&share->dbmap[index], table, index,
                      buf, txn, record_arena, key_arena);
      if (unlikely(rc))
        return rc;
    }
  }

  return 0;
}

static inline ups_cursor_t *
locate_secondary_key(ups_db_t *db, ups_txn_t *txn, ups_key_t *key,
                ups_record_t *primary_record)
{
  CursorProxy cp(db, txn);
  if (unlikely(cp.cursor == 0))
    return 0;

  ups_record_t record = ups_make_record(0, 0);

  ups_status_t st = ups_cursor_find(cp.cursor, key, &record, 0);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_find", st);
    return 0;
  }

  // Locate the duplicate with the primary key
  do {
    if (likely(record.size == primary_record->size
            && !::memcmp(record.data, primary_record->data, record.size)))
      break;

    st = ups_cursor_move(cp.cursor, 0, &record,
                    UPS_ONLY_DUPLICATES | UPS_CURSOR_NEXT);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_move", st);
      return 0;
    }
  } while (true);

  return cp.detach();
}

static inline int
delete_from_secondary(ups_db_t *db, TABLE *table, int index, const uint8_t *buf,
                ups_txn_t *txn, ups_key_t *primary_key,
                ByteVector &key_arena)
{
  ups_key_t key = key_from_row(table, buf, index, key_arena);
  ups_record_t primary_record = ups_make_record(primary_key->data,
                primary_key->size);
  CursorProxy cp(locate_secondary_key(db, txn, &key, &primary_record));
  if (unlikely(cp.cursor == 0))
    return 1;

  // delete this duplicate
  ups_status_t st = ups_cursor_erase(cp.cursor, 0);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_erase", st);
    return 1;
  }

  return 0;
}

static inline int
delete_multiple_indices(ups_cursor_t *cursor, UpscaledbShare *share,
                TABLE *table, const uint8_t *buf, ByteVector &key_arena)
{
  ups_status_t st;

  TxnProxy txnp(share->env);
  if (unlikely(!txnp.txn))
    return 1;

  ups_key_t primary_key;

  // The cursor is positioned on the primary key. If the index was auto-
  // generated then fetch the key, otherwise extract the key from |buf|
  if (share->autoidx.db) {
    st = ups_cursor_move(cursor, &primary_key, 0, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_erase", st);
      return 1;
    }
  }
  else {
    ups_key_t key = key_from_row(table, buf, 0, key_arena);
    primary_key = key;
  }

  for (int index = 0; index < (int)share->dbmap.size(); index++) {
    Field *field = share->dbmap[index].field;
    assert(field->m_indexed == true && field->stored_in_db != false);
    (void)field;

    // is this the primary index?
    if (share->dbmap[index].is_primary_index) {
      // TODO can we use |cursor|? no, because it's not part of the txn :-/
      assert(index == 0);
      st = ups_db_erase(share->dbmap[index].db, txnp.txn, &primary_key, 0);
      if (unlikely(st != 0)) {
        log_error("ups_db_erase", st);
        return 1;
      }
    }
    // is this a secondary index?
    else {
      int rc = delete_from_secondary(share->dbmap[index].db, table, index,
                    buf, txnp.txn, &primary_key, key_arena);
      if (unlikely(rc != 0))
        return rc;
    }
  }

  st = txnp.commit();
  if (unlikely(st != 0))
    return 1;

  return 0;
}

static inline bool
are_keys_equal(ups_key_t *lhs, ups_key_t *rhs)
{
  if (lhs->size != rhs->size)
    return false;
  return 0 == ::memcmp(lhs->data, rhs->data, lhs->size);
}

static inline int
ups_status_to_error(TABLE *table, const char *msg, ups_status_t st)
{
  if (likely(st == 0)) {
    table->status = 0;
    return 0;
  }

  if (st == UPS_KEY_NOT_FOUND) {
    table->status = STATUS_NOT_FOUND;
    return HA_ERR_END_OF_FILE;
  }
  if (st == UPS_DUPLICATE_KEY) {
    table->status = STATUS_NOT_FOUND;
    return HA_ERR_FOUND_DUPP_KEY;
  }

  log_error(msg, st);
  return HA_ERR_GENERIC;
}

static ups_key_t
extract_key(const uint8_t *keybuf, KEY *key_info, ByteVector &key_arena)
{
  ups_key_t key = ups_make_key(0, 0);
  KEY_PART_INFO *key_part = key_info->key_part;
  uint32_t key_parts = key_info->user_defined_key_parts;

  // if this is not a multi-part key AND it has fixed length then we can
  // simply use the existing |keybuf| pointer for the lookup
  if (key_parts == 1 && !encoded_length_bytes(key_part->type)) {
    key.data = key_part->null_bit ? (void *)(keybuf + 1) : (void *)keybuf;
    key.size = key_part->length;
  }
  // otherwise we have to unpack the row and transform it into the
  // correct format
  else {
    const uint8_t *p = keybuf;
    key_arena.clear();

    for (uint32_t i = 0; i < key_parts; i++, key_part++) {
      // skip null byte, if it exists
      if (key_part->null_bit)
        p++;

      uint32_t length;
      switch (encoded_length_bytes(key_part->type)) {
        case 0:
          length = key_part->length;
          break;
        case 1:
          length = *(uint16_t *)p;
          // append the length if it's a multi-part key
          if (key_parts > 1) {
            key_arena.push_back((uint8_t)length);
            key.size += 1;
          }
          p += 2;
          break;
        case 2:
          length = *(uint16_t *)p;
          // append the length if it's a multi-part key
          if (key_parts > 1) {
            key_arena.insert(key_arena.end(), p, p + 2);
            key.size += 2;
          }
          p += 2;
          break;
      }

      // append the key data
      key_arena.insert(key_arena.end(), p, p + length);
      key.size += length;
      p += key_part->length;
    }

    key.data = key_arena.data();
  }
  return key;
}

static ups_key_t
extract_first_keys(const uint8_t *keybuf, TABLE *table,
                KEY *key_info, ByteVector &key_arena)
{
  ups_key_t key = ups_make_key(0, 0);
  KEY_PART_INFO *key_part = key_info->key_part;
  assert(key_info->user_defined_key_parts > 1);

  const uint8_t *p = keybuf;
  key_arena.clear();

  for (uint32_t i = 0; i < table->reginfo.qep_tab->ref().key_parts;
                  i++, key_part++) {
    // skip null byte, if it exists
    if (key_part->null_bit)
      p++;

    uint32_t length;
    switch (encoded_length_bytes(key_part->type)) {
      case 0:
        length = key_part->length;
        break;
      case 1:
        length = *(uint16_t *)p;
        // always append the length for multi-part keys
        key_arena.push_back((uint8_t)length);
        p += 2;
        break;
      case 2:
        length = *(uint16_t *)p;
        // always append the length for multi-part keys
        key_arena.insert(key_arena.end(), p, p + 2);
        p += 2;
        break;
    }
  
    // append the key data
    key_arena.insert(key_arena.end(), p, p + length);
    p += length;
  }
  
  key.size = key_arena.size();
  key.data = key_arena.data();
  return key;
}

// write_row() inserts a row. No extra() hint is given currently if a bulk load
// is happening. buf() is a byte array of data. You can use the field
// information to extract the data from the native byte array type.
//
// See the note for update_row() on auto_increments. This case also applies to
// write_row().
int
UpscaledbHandler::write_row(uchar *buf)
{
  ups_status_t st;

  duplicate_error_index = (uint32_t)-1;

  DBUG_ENTER("UpscaledbHandler::write_row");

  // no index? then use the one which was automatically generated
  if (share->dbmap.empty())
    DBUG_RETURN(insert_auto_index(share, table, buf, 0,
                            key_arena, record_arena));

  // auto-incremented index? then get a new value
  if (table->next_number_field && buf == table->record[0]) {
    int rc = update_auto_increment();
    if (unlikely(rc))
      DBUG_RETURN(rc);
  }

  // only one index? then use a temporary transaction
  if (share->dbmap.size() == 1 && !share->autoidx.db) {
    int rc = insert_primary_key(&share->dbmap[0], table, buf, 0,
                            key_arena, record_arena);
    if (unlikely(rc == HA_ERR_FOUND_DUPP_KEY))
      duplicate_error_index = 0;
    DBUG_RETURN(rc);
  }


  // multiple indices? then create a new transaction and update
  // all indices
  TxnProxy txnp(share->env);
  if (unlikely(!txnp.txn))
    DBUG_RETURN(1);

  int rc = insert_multiple_indices(share, table, buf, txnp.txn,
                  key_arena, record_arena);
  if (unlikely(rc == HA_ERR_FOUND_DUPP_KEY))
    duplicate_error_index = 0;
  if (unlikely(rc))
    DBUG_RETURN(rc);

  st = txnp.commit();
  DBUG_RETURN(st == 0 ? 0 : 1);
}

// Yes, update_row() does what you expect, it updates a row. old_buf will have
// the previous row record in it, while new_buf will have the newest data in
// it.
// Keep in mind that the server can do updates based on ordering if an ORDER BY
// clause was used. Consecutive ordering is not guaranteed.
//
// Currently new_buf will not have an updated auto_increament record.
//
//  if (table->next_number_field && record == table->record[0])
//    update_auto_increment();
int
UpscaledbHandler::update_row(const uchar *old_buf, uchar *new_buf)
{
  DBUG_ENTER("UpscaledbHandler::update_row");

  ups_status_t st;

  ups_record_t record = record_from_row(table, new_buf, record_arena);

  // fastest code path: if there's no index then use the cursor to overwrite
  // the record
  if (share->dbmap.empty()) {
    assert(cursor != 0);
    st = ups_cursor_overwrite(cursor, &record, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_overwrite", st);
      DBUG_RETURN(1);
    }
    DBUG_RETURN(0);
  }

  // build a map of the keys that are updated
  // TODO we're extracting keys over and over in the remaining code.
  // it might make sense to cache the old and new keys
  bool changed[MAX_KEY];
  ByteVector tmp1;
  ByteVector tmp2;
  for (size_t i = 0; i < share->dbmap.size(); i++) {
    ups_key_t oldkey = key_from_row(table, (uchar *)old_buf, i, tmp1);
    ups_key_t newkey = key_from_row(table, new_buf, i, key_arena);
    changed[i] = !are_keys_equal(&oldkey, &newkey);
  }

  // fast code path: if there's just one primary key then try to overwrite
  // the record, or re-insert if the key was modified
  if (share->dbmap.size() == 1 && share->autoidx.db == 0) {
    // if both keys are equal: simply overwrite the record of the
    // current key
    if (!changed[0]) {
      if (likely(cursor != 0)) {
        assert(ups_cursor_get_database(cursor) == share->dbmap[0].db);
        st = ups_cursor_overwrite(cursor, &record, 0);
      }
      else {
        ups_key_t key = key_from_row(table, (uchar *)old_buf, 0, tmp1);
        st = ups_db_insert(share->dbmap[0].db, 0, &key, &record, UPS_OVERWRITE);
      }
      if (unlikely(st != 0)) {
        log_error("ups_cursor_overwrite", st);
        DBUG_RETURN(1);
      }
      DBUG_RETURN(0);
    }

    ups_key_t oldkey = key_from_row(table, (uchar *)old_buf, 0, tmp1);
    ups_key_t newkey = key_from_row(table, new_buf, 0, key_arena);

    // otherwise delete the old row, insert the new one. Inserting can fail if
    // the key is not unique, therefore both operations are wrapped
    // in a single transaction.
    TxnProxy txnp(share->env);
    if (unlikely(!txnp.txn))
      DBUG_RETURN(1);

    // TODO can we use the cursor?? no - not in the transaction :-/
    ups_db_t *db = share->dbmap[0].db;
    st = ups_db_erase(db, txnp.txn, &oldkey, 0);
    if (unlikely(st != 0))
      DBUG_RETURN(1);

    uint32_t flags = 0;
    if (share->dbmap[0].max_key_cache->compare_and_update(&newkey))
      flags = UPS_OVERWRITE;
    st = ups_db_insert(db, txnp.txn, &newkey, &record, flags);
    if (unlikely(st == UPS_DUPLICATE_KEY))
      DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    if (unlikely(st != 0))
      DBUG_RETURN(1);

    st = txnp.commit();
    DBUG_RETURN(st == 0 ? 0 : 1);
  }

  // More than one index? Update all indices that were changed.
  // Again wrap all of this in a single transaction, in case we fail
  // to insert the new row.
  TxnProxy txnp(share->env);
  if (unlikely(!txnp.txn))
    DBUG_RETURN(1);

  ups_key_t new_primary_key = key_from_row(table, new_buf, 0, record_arena);

  // Are we overwriting an auto-generated index?
  if (share->autoidx.db) {
    ups_key_t k = ups_make_key(ref, sizeof(uint32_t));
    st = ups_db_insert(share->autoidx.db, txnp.txn, &k, &record, UPS_OVERWRITE);
    if (unlikely(st != 0))
      DBUG_RETURN(1);
  }

  // Primary index:
  // 1. Was the key modified? then re-insert it
  // 2. Otherwise overwrite the record
  // TODO can we use the cursor? -> no, it's not in the txn :-/
  if (changed[0]) {
    ups_key_t oldkey = key_from_row(table, (uchar *)old_buf, 0, key_arena);
    st = ups_db_erase(share->dbmap[0].db, txnp.txn, &oldkey, 0);
    if (unlikely(st != 0))
      DBUG_RETURN(1);
  }
  st = ups_db_insert(share->dbmap[0].db, txnp.txn, &new_primary_key, &record,
                        UPS_OVERWRITE);
  if (unlikely(st != 0))
    DBUG_RETURN(1);

  // All secondary indices:
  // 1. if the primary key was changed then their record has to be
  //    overwritten
  // 2. if the secondary key was changed then re-insert it
  for (size_t i = 1; i < share->dbmap.size(); i++) {
    ups_record_t new_primary_record = ups_make_record(new_primary_key.data,
            new_primary_key.size);
    ups_key_t old_primary_key = key_from_row(table, old_buf, 0, tmp1);
    ups_key_t newkey = key_from_row(table, (uchar *)new_buf, i, tmp2);

    if (changed[i]) {
      int rc = delete_from_secondary(share->dbmap[i].db, table, i,
                            old_buf, txnp.txn, &old_primary_key, key_arena);
      if (unlikely(rc != 0))
        DBUG_RETURN(rc);
      uint32_t flags = 0;
      if (likely(share->dbmap[i].enable_duplicates))
        flags = UPS_DUPLICATE;
      else if (share->dbmap[i].max_key_cache->compare_and_update(&newkey)) {
        assert(!key_exists(share->dbmap[i].db, &newkey));
        flags = UPS_OVERWRITE;
      }
      st = ups_db_insert(share->dbmap[i].db, txnp.txn, &newkey,
                            &new_primary_record, flags);
      if (unlikely(st == UPS_DUPLICATE_KEY))
        DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
      if (unlikely(st != 0))
        DBUG_RETURN(1);
    }
    else if (changed[0]) {
      ups_key_t oldkey = key_from_row(table, (uchar *)old_buf, i, tmp1);
      ups_record_t old_primary_record = ups_make_record(old_primary_key.data,
              old_primary_key.size);
      CursorProxy cp(locate_secondary_key(share->dbmap[i].db, txnp.txn,
                                &oldkey, &old_primary_record));
      assert(cp.cursor != 0);
      st = ups_cursor_overwrite(cp.cursor, &new_primary_record, 0);
      if (unlikely(st != 0))
        DBUG_RETURN(1);
    }
  }

  st = txnp.commit();
  if (unlikely(st != 0))
    DBUG_RETURN(1);

  DBUG_RETURN(0);
}

// This will delete a row. buf will contain a copy of the row to be deleted.
// The server will call this right after the current row has been called (from
// either a previous rnd_next() or index call).
//
// If you keep a pointer to the last row or can access a primary key it will
// make doing the deletion quite a bit easier. Keep in mind that the server does
// not guarantee consecutive deletions. ORDER BY clauses can be used.
int
UpscaledbHandler::delete_row(const uchar *buf)
{
  ups_status_t st;
  DBUG_ENTER("UpscaledbHandler::delete_row");

  // fast code path: if there's just one index then use the cursor to
  // delete the record
  if (share->dbmap.size() <= 1) {
    if (likely(cursor != 0))
      st = ups_cursor_erase(cursor, 0);
    else {
      assert(share->dbmap.size() == 1);
      ups_key_t key = key_from_row(table, buf, 0, key_arena);
      st = ups_db_erase(share->dbmap[0].db, 0, &key, 0);
    }

    if (unlikely(st != 0)) {
      log_error("ups_cursor_erase", st);
      DBUG_RETURN(1);
    }
    DBUG_RETURN(0);
  }

  // otherwise (if there are multiple indices) then delete the key from
  // each index
  int rc = delete_multiple_indices(cursor, share, table, buf, key_arena);
  DBUG_RETURN(rc);
}

int
UpscaledbHandler::index_init(uint idx, bool sorted)
{
  DBUG_ENTER("UpscaledbHandler::index_init");

  active_index = idx;

  assert(share != 0);

  // from which index are we reading?
  ups_db_t *db = share->dbmap[idx].db;

  // if there's a cursor then make sure it's for the correct index database
  if (cursor && ups_cursor_get_database(cursor) == db)
    DBUG_RETURN(0);

  if (cursor)
    ups_cursor_close(cursor);

  ups_status_t st = ups_cursor_create(&cursor, db, 0, 0);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_create", st);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

int 
UpscaledbHandler::index_end()
{
  active_index = MAX_KEY;
  return rnd_end();
}

// Positions an index cursor to the index specified in the handle. Fetches the
// row if available. If the key value is null, begin at the first key of the
// index.
int
UpscaledbHandler::index_read(uchar *buf, const uchar *keybuf,
                uint32_t keylen, enum ha_rkey_function find_flag)
{
  DBUG_ENTER("UpscaledbHandler::index_read");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);

  bool read_primary_index = (active_index == 0 || active_index == MAX_KEY)
                                && share->autoidx.db == 0;

  // when reading from the primary index: directly fetch record into |buf| 
  // if the row has fixed length
  ups_record_t record = ups_make_record(0, 0);
  if (read_primary_index && row_is_fixed_length(table)) {
    record.data = buf;
    record.flags = UPS_RECORD_USER_ALLOC;
  }

  ups_status_t st;

  if (keybuf == 0) {
    st = ups_cursor_move(cursor, 0, &record, UPS_CURSOR_FIRST);
  }
  else {
    ups_key_t key = extract_key(keybuf, &table->key_info[active_index],
                            key_arena);

    switch (find_flag) {
      case HA_READ_KEY_EXACT:
        if (likely(table->key_info[active_index].user_defined_key_parts == 1))
          st = ups_cursor_find(cursor, &key, &record, 0);
        else {
          st = ups_cursor_find(cursor, &key, &record, UPS_FIND_GEQ_MATCH);
          // if this was an approx. match: verify that the key part is really
          // identical!
          if (ups_key_get_approximate_match_type(&key) != 0) {
            ups_key_t first = extract_first_keys(keybuf, table,
                                &table->key_info[active_index], key_arena);
            if (::memcmp(key.data, first.data, first.size))
              st = UPS_KEY_NOT_FOUND;
          }
        }
        break;
      case HA_READ_KEY_OR_NEXT:
      case HA_READ_PREFIX:
        st = ups_cursor_find(cursor, &key, &record, UPS_FIND_GEQ_MATCH);
        break;
      case HA_READ_KEY_OR_PREV:
      case HA_READ_PREFIX_LAST_OR_PREV:
        st = ups_cursor_find(cursor, &key, &record, UPS_FIND_LEQ_MATCH);
        break;
      case HA_READ_AFTER_KEY:
        st = ups_cursor_find(cursor, &key, &record, UPS_FIND_GT_MATCH);
        break;
      case HA_READ_BEFORE_KEY:
        st = ups_cursor_find(cursor, &key, &record, UPS_FIND_LT_MATCH);
        break;
      case HA_READ_INVALID: // (last)
        st = ups_cursor_move(cursor, 0, &record, UPS_CURSOR_LAST);
        break;
      default:
        assert(!"shouldn't be here");
        st = UPS_INTERNAL_ERROR;
        break;
    }
  }

  if (likely(st == 0)) {
    // Did we fetch from the primary index? then we have to unpack the record
    if (read_primary_index && !row_is_fixed_length(table))
      record = unpack_record(table, &record, buf);

    // Or is this a secondary index? then use the primary key (in the record)
    // to fetch the row
    else if (!read_primary_index) {
      // Auto-generated index? Then store the internal row-id (which is a 32bit
      // record number)
      if (share->autoidx.db != 0) {
        assert(ref_length == sizeof(uint32_t));
        assert(record.size == sizeof(uint32_t));
        *(uint32_t *)ref = *(uint32_t *)record.data;
      }

      ups_key_t key = ups_make_key(record.data, (uint16_t)record.size);
      ups_record_t rec = ups_make_record(0, 0);
      if (row_is_fixed_length(table)) {
        rec.data = buf;
        rec.flags = UPS_RECORD_USER_ALLOC;
      }
      st = ups_db_find(share->autoidx.db
                        ? share->autoidx.db
                        : share->dbmap[0].db, 0, &key, &rec, 0);
      if (likely(st == 0) && !row_is_fixed_length(table))
        rec = unpack_record(table, &rec, buf);
    }
  }

  int rc = ups_status_to_error(table, "ups_db_find", st);

  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

int
UpscaledbHandler::index_operation(uchar *keybuf, uint32_t keylen,
                uchar *buf, uint32_t flags)
{
  ups_status_t st;

  // when reading from the primary index: directly fetch record into |buf| 
  // if the row has fixed length
  ups_record_t record = ups_make_record(0, 0);
  if ((active_index == 0 || active_index == MAX_KEY)
          && row_is_fixed_length(table)) {
    record.data = buf;
    record.flags = UPS_RECORD_USER_ALLOC;
  }

  // if flags are 0: lookup the current key, but do not move the cursor!
  if (unlikely(flags == 0)) {
    assert(first_call_after_position == true);
  /*
    // skip the null-byte
    KEY_PART_INFO *key_part = table->key_info[active_index].key_part;
    if (key_part->null_bit) {
      keybuf++;
      keylen--;
    }
    ups_key_t key = ups_make_key((void *)keybuf, (uint16_t)keylen);
    */
    ups_key_t key = ups_make_key((void *)last_position_key.data(),
                            (uint16_t)last_position_key.size());
    st = ups_cursor_find(cursor, &key, &record, 0);
  }
  // otherwise move forward or backwards (or whatever the caller requested)
  else {
    // if we fetched the record from an auto-generated index then store the
    // row id; it will be required in ::position()
    if (share->autoidx.db != 0) {
      ups_key_t key = ups_make_key(&recno_row_id, sizeof(recno_row_id));
      key.flags = UPS_KEY_USER_ALLOC;
      st = ups_cursor_move(cursor, &key, &record, flags);
    }
    // if we move to the next duplicate of a multipart index then check if the
    // first (!) key of the multipart key is still identical to the previous
    // one.
    else if ((flags & UPS_ONLY_DUPLICATES)
            && table->key_info[active_index].user_defined_key_parts > 1) {
      ups_key_t key = ups_make_key(0, 0);
      st = ups_cursor_move(cursor, &key, &record, flags & ~UPS_ONLY_DUPLICATES);
      if (likely(st == 0 && keybuf != 0)) {
        ups_key_t first = extract_first_keys(keybuf, table,
                                &table->key_info[active_index], key_arena);
        if (::memcmp(key.data, first.data, first.size))
          st = UPS_KEY_NOT_FOUND;
      }
    }
    // otherwise simply move into the requested direction
    else {
      st = ups_cursor_move(cursor, 0, &record, flags);
    }
  }

  if (unlikely(st != 0))
    return ups_status_to_error(table, "ups_cursor_move", st);

  // if we fetched the record from a secondary index: lookup the actual row
  // from the primary index
  if (!share->dbmap.empty() && (active_index > 0 && active_index < MAX_KEY)) {
    ups_key_t key = ups_make_key(record.data, (uint16_t)record.size);
    ups_record_t rec = ups_make_record(0, 0);
    if (row_is_fixed_length(table)) {
      rec.data = buf;
      rec.flags = UPS_RECORD_USER_ALLOC;
    }
    st = ups_db_find(share->autoidx.db ? share->autoidx.db : share->dbmap[0].db,
                        0, &key, &rec, 0); // or autoidx.db??
    if (unlikely(st != 0))
      return ups_status_to_error(table, "ups_cursor_move", st);

    record.data = rec.data;
    record.size = rec.size;
  }

  // if necessary then unpack the row
  if (!row_is_fixed_length(table))
    unpack_record(table, &record, buf);

  return ups_status_to_error(table, "ups_cursor_move", 0);
}

// Used to read forward through the index.
int
UpscaledbHandler::index_next(uchar *buf)
{
  DBUG_ENTER("UpscaledbHandler::index_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  int rc = index_operation(0, 0, buf, UPS_CURSOR_NEXT);

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

// Used to read backwards through the index.
int
UpscaledbHandler::index_prev(uchar *buf)
{
  DBUG_ENTER("UpscaledbHandler::index_prev");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  int rc = index_operation(0, 0, buf, UPS_CURSOR_PREVIOUS);

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

// index_first() asks for the first key in the index.
int
UpscaledbHandler::index_first(uchar *buf)
{
  DBUG_ENTER("UpscaledbHandler::index_first");

  int rc = index_operation(0, 0, buf, UPS_CURSOR_FIRST);

  MYSQL_READ_ROW_DONE(rc);
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

// index_last() asks for the last key in the index.
int
UpscaledbHandler::index_last(uchar *buf)
{
  DBUG_ENTER("UpscaledbHandler::index_last");

  int rc = index_operation(0, 0, buf, UPS_CURSOR_LAST);

  MYSQL_READ_ROW_DONE(rc);
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

// Moves the cursor to the next row with the specified key
int
UpscaledbHandler::index_next_same(uchar *buf, const uchar *keybuf, uint keylen)
{
  DBUG_ENTER("UpscaledbHandler::index_next_same");

  int rc = 0;

  if (first_call_after_position) {

    // locate the first key
    rc = index_operation((uchar *)keybuf, keylen, buf, 0);
    // and immediately try to move to the next key
    if (likely(rc == 0))
      rc = index_operation(0, 0, buf, UPS_ONLY_DUPLICATES | UPS_CURSOR_NEXT);

    first_call_after_position = false;
  }
  else {
    rc = index_operation((uchar *)keybuf, keylen, buf,
                    UPS_ONLY_DUPLICATES | UPS_CURSOR_NEXT);
  }

  MYSQL_READ_ROW_DONE(rc);
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

// rnd_init() is called when the system wants the storage engine to do a table
// scan.
int
UpscaledbHandler::rnd_init(bool scan)
{
  DBUG_ENTER("UpscaledbHandler::rnd_init");

  assert(share != 0);

  if (cursor)
    ups_cursor_close(cursor);

  ups_db_t *db = share->autoidx.db;
  if (!db)
    db = share->dbmap[0].db;

  ups_status_t st = ups_cursor_create(&cursor, db, 0, 0);
  if (unlikely(st != 0)) {
    log_error("ups_cursor_create", st);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

int
UpscaledbHandler::rnd_end()
{
  DBUG_ENTER("UpscaledbHandler::rnd_end");

  assert(cursor != 0);
  ups_cursor_close(cursor);
  cursor = 0;

  DBUG_RETURN(0);
}

// This is called for each row of the table scan. When you run out of records
// you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
// The Field structure for the table is the key to getting data into buf
// in a manner that will allow the server to understand it.
int
UpscaledbHandler::rnd_next(uchar *buf)
{
  DBUG_ENTER("UpscaledbHandler::rnd_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str, TRUE);

  int rc = index_operation(0, 0, buf, UPS_CURSOR_NEXT);

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

// Returns an interval of reserved auto-increment values
void
UpscaledbHandler::get_auto_increment(ulonglong offset, ulonglong increment,
                        ulonglong nb_desired_values, ulonglong *first_value,
                        ulonglong *nb_reserved_values)
{
  *nb_reserved_values = nb_desired_values;
  *first_value = share->autoinc_value + 1;
  share->autoinc_value += nb_desired_values;
}

void
UpscaledbHandler::update_create_info(HA_CREATE_INFO *create_info)
{
  // called from ALTER TABLE ... AUTO_INCREMENT=XX? then don't overwrite
  // the new value!
  if (create_info->auto_increment_value > 0) {
    share->initial_autoinc_value = create_info->auto_increment_value;
    share->autoinc_value = create_info->auto_increment_value;
    return;
  }

  if (unlikely(next_insert_id != 0))
    create_info->auto_increment_value = next_insert_id;
  else
    create_info->auto_increment_value = share->initial_autoinc_value;
}

// This function is used to perform a look-up on a secondary index. It
// retrieves the primary key, and copies it into |ref|.
void
UpscaledbHandler::position(const uchar *buf)
{
  DBUG_ENTER("UpscaledbHandler::position");

  // Auto-generated index? Then store the internal row-id (which is a 32bit
  // record number)
  if (share->autoidx.db != 0) {
    assert(ref_length == sizeof(uint32_t));
    *(uint32_t *)ref = recno_row_id;
    DBUG_VOID_RETURN;
  }

  // Store the PRIMARY key as the reference in |ref|
  KEY *key_info = table->key_info;
  assert(ref_length == key_info->key_length);
  key_copy(ref, (uchar *)buf, key_info, key_info->key_length);

  // Same (index) key as in the last call? then return immediately (otherwise
  // ups_cursor_find would reset the cursor to the first duplicate, and the
  // following call to UpscaledbHandler::index_next_same() would always
  // return the same row)
  ups_key_t key = key_from_row(table, buf,
                        active_index == MAX_KEY ? 0 : active_index, key_arena);
  if (key.size == last_position_key.size()
        && !::memcmp(key.data, last_position_key.data(), key.size))
    DBUG_VOID_RETURN;

  // otherwise store a copy of the last key
  last_position_key.resize(key.size);
  ::memcpy(last_position_key.data(), key.data, key.size);

  first_call_after_position = true;

  DBUG_VOID_RETURN;
}

// This is like rnd_next, but you are given a position to use
// to determine the row. The position will be of the type that you stored in
// ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
// or position you saved when position() was called.
int
UpscaledbHandler::rnd_pos(uchar *buf, uchar *pos)
{
  ups_status_t st;

  DBUG_ENTER("UpscaledbHandler::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);

  assert(share != 0);
  assert(active_index == MAX_KEY);

  bool is_fixed_row_length = row_is_fixed_length(table);
  ups_record_t rec = ups_make_record(0, 0);
  ups_key_t key = ups_make_key(0, 0);

  ups_db_t *db = share->autoidx.db;
  if (!db)
    db = share->dbmap[0].db;

  key.data = pos;
  key.size = table->key_info
                ? (uint16_t)table->key_info[0].key_length
                : (uint16_t)sizeof(uint32_t); // recno

  // when reading from the primary index: directly fetch record into |buf| 
  // if the row has fixed length
  if (is_fixed_row_length) {
    rec.data = buf;
    rec.flags = UPS_RECORD_USER_ALLOC;
  }

  st = ups_cursor_find(cursor, &key, &rec, 0);

  // did we fetch from the primary index? then we have to unpack the record
  if (st == 0 && !is_fixed_row_length)
    rec = unpack_record(table, &rec, buf);

  int rc = ups_status_to_error(table, "ups_cursor_find", st);

  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

// ::info() is used to return information to the optimizer. See my_base.h for
// the complete description.
//
//
// Currently this table handler doesn't implement most of the fields really
// needed. SHOW also makes use of this data.
//
// You will probably want to have the following in your code:
// @code
//  if (records < 2)
//    records = 2;
// @endcode
// The reason is that the server will optimize for cases of only a single
// record. If, in a table scan, you don't know the number of records, it
// will probably be better to set records to two so you can return as many
// records as you need. Along with records, a few more variables you may wish
// to set are:
//   records
//   deleted
//   data_file_length
//   index_file_length
//   delete_length
//   check_time
// Take a look at the public variables in handler.h for more information.
// TODO TODO TODO
int
UpscaledbHandler::info(uint flag)
{
  DBUG_ENTER("UpscaledbHandler::info");

  if (flag & HA_STATUS_AUTO)
    stats.auto_increment_value = share->initial_autoinc_value;

  if (flag & HA_STATUS_ERRKEY)
    errkey = duplicate_error_index;

  DBUG_RETURN(0);
}

// extra() is called whenever the server wishes to send a hint to
// the storage engine. The myisam engine implements the most hints.
// ha_innodb.cc has the most exhaustive list of these hints.
// TODO TODO TODO
int
UpscaledbHandler::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("UpscaledbHandler::extra");
  DBUG_RETURN(0);
}

// Track how often this is used; implement a custom delete function (resetting
// root page, moving all pages to the freelist - or dropping the database
// and re-creating it - but this is currently not transactional)
int
UpscaledbHandler::delete_all_rows()
{
  DBUG_ENTER("UpscaledbHandler::delete_all_rows");

  close(); // closes the cursor

  ups_status_t st = delete_all_databases(share, table);
  if (unlikely(st)) {
    log_error("delete_all_databases", st);
    DBUG_RETURN(1);
  }

  st = create_all_databases(share, table, &share->config);
  if (unlikely(st)) {
    log_error("create_all_databases", st);
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

// deletes all rows
int
UpscaledbHandler::truncate()
{
  DBUG_ENTER("UpscaledbHandler::truncate");
  int err = delete_all_rows();
  if (unlikely(err))
    DBUG_RETURN(err);

  share->autoinc_value = 0;

  DBUG_RETURN(0);
}

// This create a lock on the table. If you are implementing a storage engine
// that can handle transacations look at ha_berkely.cc to see how you will
// want to go about doing this. Otherwise you should consider calling flock()
// here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
// this.
// TODO TODO TODO
int
UpscaledbHandler::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("UpscaledbHandler::external_lock");
  DBUG_RETURN(0);
}

/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for upscaledb, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
// TODO TODO TODO
THR_LOCK_DATA **
UpscaledbHandler::store_lock(THD *thd, THR_LOCK_DATA **to,
                enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock_data.type == TL_UNLOCK)
    lock_data.type = lock_type;
  *to++ = &lock_data;
  return to;
}

int
UpscaledbHandler::delete_table(const char *name)
{
  DBUG_ENTER("UpscaledbHandler::delete_table");

  // remove the environment from the global cache
  close_and_remove_share(name, share);

  // delete all files
  std::string env_name = format_environment_name(name);
  (void)boost::filesystem::remove(env_name);
  (void)boost::filesystem::remove(env_name + ".jrn0");
  (void)boost::filesystem::remove(env_name + ".jrn1");
  (void)boost::filesystem::remove(env_name + ".cnf");

  DBUG_RETURN(0);
}

int
UpscaledbHandler::rename_table(const char *from, const char *to)
{
  DBUG_ENTER("UpscaledbHandler::rename_table ");
  close();

  close_and_remove_share(from, share);
  rename_create_info(from, to);

  std::string from_name = format_environment_name(from);
  std::string to_name = format_environment_name(to);
  (void)boost::filesystem::rename(from_name, to_name);
  (void)boost::filesystem::rename(from_name + ".jrn0", to_name + ".jrn0");
  (void)boost::filesystem::rename(from_name + ".jrn1", to_name + ".jrn1");
  (void)boost::filesystem::rename(from_name + ".cnf", to_name + ".cnf");

  DBUG_RETURN(0);
}

// TODO errors are not propagated properly
int
UpscaledbHandler::records(ha_rows *num_rows)
{
  DBUG_ENTER("UpscaledbHandler::records");

  *num_rows = records_in_range(0, 0, 0);
  DBUG_RETURN(0);
}

// Given a starting key and an ending key, estimate the number of rows that
// will exist between the two keys.
ha_rows
UpscaledbHandler::records_in_range(uint index, key_range *min_key,
                key_range *max_key)
{
  DBUG_ENTER("UpscaledbHandler::records_in_range");
  DBUG_RETURN(10); // TODO remove me - the code below fails when creating
                   // a cursor!
#if 0
  ups_status_t st;
  CursorProxy min;
  CursorProxy max;

  active_index = index;
  KEY *key_info = table->key_info + index;
  Field *field = key_info->key_part->field;

  assert(share != 0);

  ups_record_t record = ups_make_record(0, 0);

  if (min_key) {
    st = ups_cursor_create(&min.cursor, share->dbmap[index].db, 0, 0);
    if (unlikely(st)) {
      log_error("ups_cursor_create", st);
      DBUG_RETURN(10);
    }

    ups_key_t key = ups_make_key((void *)min_key->key,
                        (uint16_t)min_key->length);

    st = ups_cursor_find(cursor, &key, &record, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_find", st);
      DBUG_RETURN(10);
    }
  }

  if (max_key) {
    st = ups_cursor_create(&max.cursor, share->dbmap[index].db, 0, 0);
    if (unlikely(st)) {
      log_error("ups_cursor_create", st);
      DBUG_RETURN(10);
    }

    ups_key_t key = ups_make_key((void *)max_key->key,
                        (uint16_t)max_key->length);

    st = ups_cursor_find(cursor, &key, &record, 0);
    if (unlikely(st != 0)) {
      log_error("ups_cursor_find", st);
      DBUG_RETURN(10);
    }
  }

  char query[64];
  snprintf(query, sizeof(query), "COUNT($key) FROM DATABASE %d", dbname(field));

  uqi_result_t *result;
  st = uqi_select_range(share->env, query, min.cursor, max.cursor, &result);
  if (unlikely(st != 0)) {
    log_error("uqi_select_range", st);
    DBUG_RETURN(10);
  }

  uqi_result_get_record(result, 0, &record);
  uint64_t rv = *(uint64_t *)record.data;
  uqi_result_close(result);

  DBUG_RETURN(rv);
#endif
}

UpscaledbTableShare *
UpscaledbHandler::allocate_or_get_share()
{
  DBUG_ENTER("UpscaledbHandler::allocate_or_get_share");

  lock_shared_ha_data();

  UpscaledbTableShare *table_share = (UpscaledbTableShare *)get_ha_share_ptr();
  if (unlikely(!table_share))
    table_share = new UpscaledbTableShare;

  set_ha_share_ptr(table_share);
  unlock_shared_ha_data();

  DBUG_RETURN(table_share);
}

struct st_mysql_storage_engine upscaledb_storage_engine = {
  MYSQL_HANDLERTON_INTERFACE_VERSION
};

mysql_declare_plugin(upscaledb)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &upscaledb_storage_engine,
  "UPSCALEDB",
  "Christoph Rupp, chris@crupp.de",
  "upscaledb storage engine",
  PLUGIN_LICENSE_GPL,
  upscaledb_init_func,             /* Plugin Init */
  NULL,                            /* Plugin Deinit */
  0x0001,                          /* Version 0.1 */
  NULL,                            /* status variables */
  NULL,                            /* system variables */
  NULL,                            /* config options */
  0,                               /* flags */
}
mysql_declare_plugin_end;
