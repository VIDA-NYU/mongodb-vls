# MongoDB-VLS

MongoDB-VLS is a prototype implementation of VLS (Virtual Lightweight Snapshots) in [MongoDB v.2.5.5](https://github.com/mongodb/mongo/releases/tag/r2.5.5). VLS is a mechanism that enables consistent analytics without blocking incoming updates in NoSQL stores.

## Links and References

For more detailed information about the VLS technique, please refer to our ICDE paper:

*Virtual Lightweight Snapshots for Consistent Analytics in NoSQL Stores, F. Chirigati, J. Siméon, M. Hirzel, and J. Freire. In Proceedings of the 32nd International Conference on Data Engineering (ICDE), 2016*

The team includes:

* [Fernando Chirigati][fc] (New York University)
* [Jérôme Siméon][js] (IBM Watson Research)
* [Martin Hirzel][mh] (IBM Watson Research)
* [Juliana Freire][jf] (New York University)

[fc]: http://bigdata.poly.edu/~fchirigati/
[js]: http://researcher.watson.ibm.com/researcher/view.php?person=us-simeon
[mh]: http://hirzels.com/martin/
[jf]: http://vgc.poly.edu/~juliana/

## How To Install

Please note that MongoDB-VLS has been only tested on CentOS and Fedora 23 Server.

### MongoDB-VLS

First, build and install TBB 4.2 Update 3; the source code and build instructions are available [here](https://www.threadingbuildingblocks.org/download#stable-releases). Make sure you add TBB's `lib` directory to the `LD_LIBRARY_PATH` environment variable.

Next, build and install MongoDB-VLS, which follows the same instructions on installing MongoDB (you will need [SCons](http://www.scons.org/)):

    $ cd vls
    $ scons
    $ scons --prefix=/opt/mongo install

We recommend using GCC 4.7+. More information about how to build and install MongoDB is available [here](https://github.com/mongodb/mongo/blob/5f2ad3f6411cb1c727e4b836798b8ef06de25f2d/docs/building.md).

We recommend creating one data directory (e.g.: `/mongodb/data/`) and a location for the log (e.g.: `/mongodb/log`).

### YCSB

We extended [YCSB 0.1.4](https://github.com/brianfrankcooper/YCSB/releases/tag/0.1.4) to include aggregate queries (using both full and index scans). You can build it with [Maven](https://maven.apache.org/) as follows:

    $ cd ycsb
    $ mvn clean package

## How To Use

### MongoDB-VLS

To start the MongoDB server, please refer to [`experiments/start_mongod_server`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/start_mongod_server) (to start the server for full scans) and [`experiments/start_mongod_server_index`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/start_mongod_server_index) (to start the server for index scans). We use the [`numactl` command](https://docs.mongodb.org/manual/administration/production-notes/#configuring-numa-on-linux) to start the server.

To stop the server, please refer to [`experiments/stop_mongod_server`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/stop_mongod_server).

For more information on MongoDB, please refer to the [documentation](https://docs.mongodb.org/v2.4/).

### YCSB

To load data into MongoDB-VLS using YCSB, use the following command:

    $ ./experiments/scripts/ycsb-scripts/ycsb_load_data db_name n_records
    
where `db_name` is the name of MongoDB's collection and `n_records` is the number of records to be loaded into `db_name`.

For more information on YCSB, please refer to the [documentation](https://github.com/brianfrankcooper/YCSB/wiki).

## Reproducing the Results

This section shows how to reproduce the results published in our ICDE paper. Here, we assume that a directory called `experiments/results/` has been created for the results.

### Query Execution Time Results

To generate the results for full scan aggregates, run the following script:

    $ ./experiments/scripts/ycsb-scripts/query_execution_time db_name n_records
    
To generate the results for index scan aggregates, run the following script:

    $ ./experiments/scripts/ycsb-scripts/index_query_execution_time db_name n_records

### Update Throughput and Latency Results

To generate the results for full scan aggregates, run the following script:

    $ ./experiments/scripts/ycsb-scripts/query_updates db_name n_records
    
To generate the results for index scan aggregates, run the following script:

    $ ./experiments/scripts/ycsb-scripts/index_query_updates db_name n_records
    
### Remset Size Results

The scripts are the same, but to get information about remset size, debugging code from [`collection.h`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/vls/src/mongo/db/structure/collection.h) and [`collection_scan.cpp`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/vls/src/mongo/db/exec/collection_scan.cpp) must be uncommented.

### Varying the Size of Bit Vectors

The scripts are the same, but the code must be changed to support 128-bit vectors, e.g., [`collection.h`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/vls/src/mongo/db/structure/collection.h#L182) must be changed to support `std::bitset<128>`, rather than `uint64_t`.
    
### Plots

All plots were implemented using [matplotlib](http://matplotlib.org/).

* Figures [7a](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/scan_duration.png) and [7b](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/scan_duration_updates.png) (query execution time): [`experiments/plots/query_execution_time.py`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/query_execution_time.py)
* Figure [8a](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/tbb_scan_updates_throughput_mongodb.png) (update throughput for full scans): [`experiments/plots/full_scan_update_throughput.py`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/full_scan_update_throughput.py)
* Figure [8b](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/tbb_scan_updates_95_percentile_mongodb_log.png) (update latency for full scans): [`experiments/plots/full_scan_update_latency.py`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/full_scan_update_latency.py)
* Figures [9a](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/tbb_index_scan_updates_throughput_mongodb_5.0.png), [9b](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/tbb_index_scan_updates_throughput_mongodb_10.0.png), and [9c](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/tbb_index_scan_updates_throughput_mongodb_25.0.png) (update throughput for index scans): [`experiments/plots/index_scan_update_throughput.py`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/index_scan_update_throughput.py)
* Figures [10a](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/tbb_index_scan_updates_95_percentile_mongodb_5.0.png), [10b](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/tbb_index_scan_updates_95_percentile_mongodb_10.0.png), and [10c](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/tbb_index_scan_updates_95_percentile_mongodb_25.0.png) (update latency for index scans): [`experiments/plots/index_scan_update_latency.py`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/index_scan_update_latency.py)
* Figures [11a](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/queue_size_testdb_100m_10000_8.png), [11b](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/queue_size_testdb_100m_100000_8.png), [11c](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/queue_size_testdb_100m_10000_16.png), and [11d](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/results/queue_size_testdb_100m_100000_16.png) (remset size): [`experiments/plots/remset_size.py`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/plots/remset_size.py)

We have made available [ReproZip](https://vida-nyu.github.io/reprozip/) packages ([`experiments/reprozip`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/reprozip)) to reproduce the generation of these plots. For instance, to generate Figure 7:

    $ cd experiments/reprozip
    $ reprounzip vagrant setup query_execution_time.rpz query_execution_time
    $ reprounzip vagrant run query_execution_time
    $ reprounzip vagrant download query_execution_time scan_duration.png:scan_duration.png
    $ reprounzip vagrant download query_execution_time scan_duration_updates.png:scan_duration_updates.png

Please refer to [ReproZip's documentation](https://vida-nyu.github.io/reprozip/) for more information on how to install and use the tool.

## Limitations

* The prototype implementation does not support indexed records to be deleted (only inserted/updated).
* For now, the server needs to be started either for full scan aggregates ([`experiments/start_mongod_server`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/start_mongod_server)) or index scan aggregates ([`experiments/start_mongod_server_index`](https://github.com/ViDA-NYU/mongodb-vls/blob/master/experiments/start_mongod_server_index)) through the use of the flag `--index`. Both aggregate types cannot be executed in the same server instance due to some locking issues.