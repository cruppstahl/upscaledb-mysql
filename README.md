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

The installation is described here:
https://github.com/cruppstahl/upscaledb-mysql/wiki/Installation

## Benchmarking

Please use sysbench 1.0 for benchmarking, not sysbench 0.4x. You
can download sysbench from https://github.com/akopytov/sysbench.
I run it with the following parameters:

    ./sysbench --test=tests/db/oltp.lua --mysql-socket=/tmp/mysql.sock --oltp-table-size=1000000 --mysql-db=test --mysql-user=root --mysql-password= --mysql-table-engine=upscaledb prepare 

    ./sysbench --test=tests/db/oltp.lua --mysql-socket=/tmp/mysql.sock  --oltp-table-size=1000000 --mysql-db=test --mysql-user=root --mysql-password= --mysql-table-engine=upscaledb --max-time=60 --max-requests=1000000 run

## TODO

- run more tests
- support BEGIN/COMMIT transactions
- improve SELECT performance (i.e. by using item condition pushdown)
- sort VARCHAR/CHAR/\*TEXT columns based on encoding/collation
- temporary tables: disable journalling
- enable integer key compression
- and many other things...


