# upscaledb-mysql
Upscaledb storage engine for MySQL

(C) Christoph Rupp, chris@crupp.de; https://upscaledb.com

Early release - not yet ready for production.

## Features

All features are supported, although some are not yet optimized.

## Configuration

You can set the cache size through an environment variable. The default is
128mb, but you can use less and still get good performance.

    UPSCALEDB_CACHE_SIZE      sets the cache size (in bytes)

## Installation

1. Install upscaledb version 2.2.1

        git clone https://github.com/cruppstahl/upscaledb.git
        cd upscaledb
        git checkout topic/2.2.1
        ./configure && make -j 5 && sudo make install

2. Clone the mysql-server repository

        git clone https://github.com/mysql/mysql-server.git
        cd mysql-server
        git checkout mysql-5.7.12

3. Copy the upscaledb storage engine from this repository to the MySQL
    server sources (note that you will have to adapt the paths)

        cp -r ~/upscaledb-mysql/storage/upscaledb ~/mysql-server/storage

4. Build MySQL

        cd ~/mysql-server
        cmake -DCMAKE_BUILD_TYPE=Release
        make -j 5

    If cmake fails because boost is not installed then try this:

        cmake -DCMAKE_BUILD_TYPE=Release -DDOWNLOAD_BOOST=1 -DWITH_BOOST=boost

5. Now you can either install MySQL on your system, or only copy the
    storage engine plugin to your MySQL installation directory.

    To install your fresh build of MySQL:

        sudo make install

    Alternatively, to copy the upscaledb storage engine to an existing
    MySQL installation directory (the path depends on the linux distribution
    you use):

        sudo cp storage/upscaledb/ha_upscaledb.so /usr/local/mysql/lib/plugin

6. Restart mysqld

        service mysql restart

7. Register the new storage engine

        mysqld --user=root <database>
        mysql> INSTALL  PLUGIN upscaledb SONAME 'ha_upscaledb.so';

8. Create your tables

        mysql> CREATE TABLE test (id INTEGER PRIMARY KEY) ENGINE=upscaledb;

## Benchmarking

Please use sysbench 0.5 (or 1.0) for benchmarking, not sysbench 0.4x. You
can download sysbench from https://github.com/akopytov/sysbench.
I run it with the following parameters:

    ./sysbench --test=tests/db/oltp.lua --mysql-socket=/tmp/mysql.sock --oltp-table-size=1000000 --mysql-db=test --mysql-user=root --mysql-password= --mysql-table-engine=upscaledb prepare 

    ./sysbench --test=tests/db/oltp.lua --mysql-socket=/tmp/mysql.sock  --oltp-table-size=1000000 --mysql-db=test --mysql-user=root --mysql-password= --mysql-table-engine=upscaledb --max-time=60 --max-requests=1000000 run

## TODO

- run more tests
- improve SELECT performance (i.e. by using item condition pushdown)
- sort VARCHAR/CHAR/\*TEXT columns based on encoding/collation
- temporary tables: disable journalling
- enable crc32 (done)
- enable different page sizes (done)
- enable record compression
- enable integer key compression
- ...


