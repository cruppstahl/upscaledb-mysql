# upscaledb-mysql
Upscaledb storage engine for MySQL

(C) Christoph Rupp, chris@crupp.de; http://www.upscaledb.com

Early release - not yet ready for production.

## Installation

1. Install upscaledb version 2.2.1

    git clone https://github.com/cruppstahl/upscaledb.git
    cd upscaledb
    git checkout topic/2.2.1
    ./configure && make -j 5 && sudo make install

2. Clone the mysql-server repository

    git clone https://github.com/mysql/mysql-server.git
    cd mysql-server
    git checkout mysql-5.7-12

3. Copy the upscaledb storage engine

    cp -r ~/upscaledb-mysql/storage/upscaledb ~/mysql-server/storage

4. Build MySQL

    cd ~/mysql-server
    make -DCMAKE_BUILD_TYPE=Release
    make -j 5

5. Copy the storage engine plugin to your MySQL installation directory
    (the installation directory depends on the linux distribution
    you use)

    sudo cp storage/upscaledb/ha_upscaledb.so /usr/local/mysql/lib/plugin

6. Restart mysqld

    service mysql restart

7. Register the new storage engine

    mysqld --user=root <database>
    mysql> INSTALL  PLUGIN upscaledb SONAME 'ha_upscaledb.so';

8. Create your tables

    mysql> CREATE TABLE test (id INTEGER PRIMARY KEY) ENGINE=upscaledb;

## Benchmarking

Please use sysbench 0.5 for benchmarking, not sysbench 0.4x. You an download
sysbench from https://github.com/akopytov/sysbench.
I run it with the following parameters:

    ./sysbench --test=tests/db/oltp.lua --mysql-socket=/tmp/mysql.sock --oltp-table-size=1000000 --mysql-db=test --mysql-user=root --mysql-password= --mysql-table-engine=upscaledb --mysql-engine-trx=no prepare 

    ./sysbench --test=tests/db/oltp.lua --mysql-socket=/tmp/mysql.sock  --oltp-table-size=1000000 --mysql-db=test --mysql-user=root --mysql-password= --mysql-table-engine=upscaledb --mysql-engine-trx=no --max-time=60 run

