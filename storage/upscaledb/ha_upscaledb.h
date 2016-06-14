/* Copyright (c) 2004, 2014, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <limits>
#include <vector>

#include <ups/upscaledb.h>

#include "my_global.h"                   /* ulonglong */
#include "thr_lock.h"                    /* THR_LOCK, THR_LOCK_DATA */
#include "handler.h"                     /* handler */
#include "my_base.h"                     /* ha_rows */

typedef std::vector<uint8_t> ByteVector;

struct MaxKeyCache {
  virtual bool compare_and_update(ups_key_t *key) = 0;
};

template<typename T>
struct MaxKeyCachePod : MaxKeyCache {
  MaxKeyCachePod(ups_key_t *key) {
    if (key)
      oldkey = *(T *)key->data;
    else
      oldkey = 0;
  }

  virtual bool compare_and_update(ups_key_t *key) {
    assert(key->size == sizeof(T));
    T newkey = *(T *)key->data;
    if (newkey > oldkey) {
      oldkey = newkey;
      return true;
    }
    return false;
  }

  T oldkey;
};

struct DisabledMaxKeyCache : MaxKeyCache {
  virtual bool compare_and_update(ups_key_t *) {
    return false;
  }
};

struct DbDesc {
  DbDesc(ups_db_t *db_ = 0, Field *field_ = 0, bool enable_duplicates_ = false,
                  bool is_primary_key_ = false)
    : db(db_), field(field_), enable_duplicates(enable_duplicates_),
      is_primary_key(is_primary_key_), max_key_cache(0) {
  }

  ups_db_t *db;
  Field *field;
  bool enable_duplicates; 
  bool is_primary_key; 
  MaxKeyCache *max_key_cache;
};

struct UpscaledbShare : public Handler_share {
  UpscaledbShare()
    : env(0) {
    thr_lock_init(&lock);
  }

  ~UpscaledbShare() {
    thr_lock_delete(&lock);
  }

  THR_LOCK lock;

  // the upscaledb Environment
  ups_env_t *env;

  // a list of all databases in this Environment
  std::vector<DbDesc> dbmap;

  // if no index is specified: create one
  DbDesc autoidx;
};

struct UpscaledbHandler : handler {
  // Constructor
  UpscaledbHandler(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg), share(0), cursor(0),
      first_call_after_position(false) {
  }

  // The name that will be used for display purposes.
  const char *table_type() const {
    return "UPSCALEDB";
  }

  // The name of the index type that will be used for display.
  const char *index_type(uint inx) {
    return "BTREE";
  }

  // The file extensions.
  const char **bas_ext() const {
    static const char *ext[] = { ".ups", ".jrn0", ".jrn1", NullS };
    return ext;
  }

  // This is a list of flags that indicate what functionality the storage engine
  // implements. The current table flags are documented in handler.h.
  // TODO fix this
  ulonglong table_flags() const {
    return HA_NO_TRANSACTIONS
            | HA_TABLE_SCAN_ON_INDEX
            | HA_CAN_INDEX_BLOBS    // blobs can be indexed
            | HA_PRIMARY_KEY_IN_READ_INDEX
            | HA_FILE_BASED
            // | HA_HAS_OWN_BINLOGGING ??
            // | HA_READ_BEFORE_WRITE_REMOVAL???
            // | HA_NO_READ_LOCAL_LOCK??
            // | HA_GENERATED_COLUMNS??
            // | HA_AUTO_PART_KEY auto-increment in multi-part keys
            | HA_BINLOG_FLAGS
            | HA_PRIMARY_KEY_REQUIRED_FOR_DELETE;
  }

  // This is a bitmap of flags that indicates how the storage engine
  // implements indexes. The current index flags are documented in
  // handler.h. If you do not implement indexes, just return zero here.
  //
  // |part| is the key part to check. First key part is 0.
  // If |all_parts| is set, MySQL wants to know the flags for the combined
  // index, up to and including 'part'.
  ulong index_flags(uint inx, uint part, bool all_parts) const {
    return HA_READ_NEXT
            | HA_READ_PREV
            // HA_DO_INDEX_COND_PUSHDOWN ???
            | HA_READ_ORDER
            | HA_READ_RANGE;
  }

  // returns the max record size
  uint max_supported_record_length() const {
    return HA_MAX_REC_LENGTH;
  }

  // max. number of supported keys in a database
  uint max_supported_keys() const {
    return MAX_KEY;
  }

  // TODO understand this, implement it
  uint max_supported_key_parts() const {
    return MAX_REF_PARTS;
  }

  // Max. length of a key part
  uint max_supported_key_part_length() const {
    return 3072; // same as innodb
  }

  // max. length of a key - 16 bits!
  uint max_supported_key_length() const {
    return std::numeric_limits<uint16_t>::max();
  }

  // Returns an estimated "cost" for a scan
  virtual double scan_time() {
    return (double) stats.records / 20.0 + 10;
  }

  // Returns an estimated "cost" for reading |rows| rows
  virtual double read_time(uint, uint, ha_rows rows) {
    return (double) rows / 20.0 + 1;
  }

  // Creates the table
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);

  // Opens the table
  int open(const char *name, int mode, uint test_if_locked);

  // Closes the table
  int close(void);

  // INSERTs a new row
  int write_row(uchar *buf);

  // UPDATEs an existing row
  int update_row(const uchar *old_data, uchar *new_data);

  // DELETEs an existing row
  int delete_row(const uchar *buf);

  // Helper function which moves the cursor in the direction specified in
  // |flags|, and retrieves the row
  int index_operation(uchar *buf, uint32_t flags);

  // Positions an index cursor to the index specified in the handle. Fetches the
  // row if available. If the key value is null, begin at the first key of the
  // index.
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                  enum ha_rkey_function find_flag);

  // Moves the cursor to the next row
  int index_next(uchar *buf);

  // Moves the cursor to the previous row
  int index_prev(uchar *buf);

  // Moves the cursor to the first row
  int index_first(uchar *buf);

  // Moves the cursor to the last row
  int index_last(uchar *buf);

  // Moves the cursor to the next row with the specified key
  int index_next_same(uchar *buf, const uchar *key, uint keylen);

  // Unlike index_init(), rnd_init() can be called two consecutive times
  // without rnd_end() in between (it only makes sense if scan=1). In this
  // case, the second call should prepare for the new table scan (e.g if
  // rnd_init() allocates the cursor, the second call should position the
  // cursor to the start of the table; no need to deallocate and allocate
  // it again. This is a required method.
  int rnd_init(bool scan);

  // Signals an end of a random access
  int rnd_end();

  int rnd_next(uchar *buf);                                     ///< required

  int rnd_pos(uchar *buf, uchar *pos);                          ///< required

  virtual int index_init(uint idx, bool sorted);

  virtual int index_end();

  void position(const uchar *record);                           ///< required

  int info(uint);                                               ///< required
  
  int extra(enum ha_extra_function operation);

  int external_lock(THD *thd, int lock_type);                   ///< required

  // Used to delete all rows in a table, including cases of truncate and
  // cases where the optimizer realizes that all rows will be removed as a
  // result of an SQL statement. Not implemented.
  int delete_all_rows();

  // Used for handler specific truncate table.  The table is locked in
  // exclusive mode and handler is responsible for reseting the auto-
  // increment counter. Not implemented.
  int truncate();

  // Returns the exact number of records that this client can see using this
  // handler object.
  // TODO is this ever called? needs a special flag for table_flags()?
  int records(ha_rows *num_rows);

  // Given a starting key and an ending key, estimate the number of rows that
  // will exist between the two keys.
  // TODO is this ever called? needs a special flag for table_flags()?
  ha_rows records_in_range(uint index, key_range *min_key, key_range *max_key);

  // Deletes a table
  int delete_table(const char *from);

  // Renames a table from one name to another via an alter table call
  int rename_table(const char *from, const char *to);

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type);     ///< required

  // Allocates or returns the shared data
  UpscaledbShare *allocate_or_get_share();

  // Shared data between all handlers which access this table
  UpscaledbShare *share;

  // Mutexes for locking (seem to be required)
  THR_LOCK_DATA lock_data;

  // A database cursor
  ups_cursor_t *cursor;

  // Is this the first call of |index_next_same()| after |position()|?
  bool first_call_after_position;

  // For caching the key in |position()|
  ByteVector last_position_key;

  // A memory buffer, to avoid frequent memory allocations
  ByteVector key_arena;
  ByteVector record_arena;
};
