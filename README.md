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

Please note that MongoDB-VLS has been only tested on CentOS.

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

To start the MongoDB server, please refer to `experiments/start_mongod_server` (to start the server for full scans) and `experiments/start_mongod_server_index` (to start the server for index scans). We use the [`numactl` command](https://docs.mongodb.org/manual/administration/production-notes/#configuring-numa-on-linux) to start the server.

To stop the server, please refer to `experiments/stop_mongod_server`.

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

### Varying the Size of Bit Vectors
    
### Plots

* Figure 7 (query execution time): `experiments/plots/query_execution_time.py`
* Figure 8a (update throughput for full scans): `experiments/plots/full_scan_update_throughput.py`
* Figure 8b (update latency for full scans): `experiments/plots/full_scan_update_latency.py`
* Figure 9 (update throughput for index scans): `experiments/plots/index_scan_update_throughput.py`
* Figure 10 (update latency for index scans): `experiments/plots/index_scan_update_latency.py`
* Figure 11 (remset size): `experiments/plots/remset_size.py`

## Limitations

* The prototype implementation does not support indexed records to be deleted (only inserted/updated).
* For now, the server needs to be started either for full scan aggregates (`experiments/start_mongod_server`) or index scan aggregates (`experiments/start_mongod_server_index`) through the use of the flag `--index`. Both aggregate types cannot be executed in the same server instance due to some locking issues.