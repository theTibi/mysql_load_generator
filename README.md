# What is mysql_load_generator?

It is reading and parsing the Slow Query log file, from that it creates LUA files what Sysbench can use. 
Because it is based on the slow query log it is going to use real life production queries.

**It is not a benchmark tool!** The purpose of the tool is to generate a steady workload with your production queries.

As it is parsing the queries it is going to collect and store the actual values used in those type if queries.
The LUA scripts are going to use these real production values in the queries.

You can easily change/update these values by yourself as well. 

The output of the script is three LUA file which are very similar to the standard OLTP Sysbench LUA files.
This is on purpose because Sysbench is a well known tool and easy to use and easy to modify the script if it is necessary. 

# Example

We have the following data in the slow query log:

```
# Time: 2020-01-21T16:55:13.250404Z
# User@Host: sbuser[sbuser] @  [127.0.0.1]  Id:    43
# Schema: sysbench  Last_errno: 0  Killed: 0
# Query_time: 0.000085  Lock_time: 0.000030  Rows_sent: 1  Rows_examined: 1  Rows_affected: 0
# Bytes_sent: 197  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
# InnoDB_trx_id: 0
# QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# Filesort: No  Filesort_on_disk: No  Merge_passes: 0
#   InnoDB_IO_r_ops: 0  InnoDB_IO_r_bytes: 0  InnoDB_IO_r_wait: 0.000000
#   InnoDB_rec_lock_wait: 0.000000  InnoDB_queue_wait: 0.000000
#   InnoDB_pages_distinct: 2
SELECT c FROM sbtest1 WHERE id=51;
# Time: 2020-01-21T16:55:13.250404Z
# User@Host: sbuser[sbuser] @  [127.0.0.1]  Id:    44
# Schema: sysbench  Last_errno: 0  Killed: 0
# Query_time: 0.000085  Lock_time: 0.000030  Rows_sent: 1  Rows_examined: 1  Rows_affected: 0
# Bytes_sent: 197  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
# InnoDB_trx_id: 0
# QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# Filesort: No  Filesort_on_disk: No  Merge_passes: 0
#   InnoDB_IO_r_ops: 0  InnoDB_IO_r_bytes: 0  InnoDB_IO_r_wait: 0.000000
#   InnoDB_rec_lock_wait: 0.000000  InnoDB_queue_wait: 0.000000
#   InnoDB_pages_distinct: 2
SELECT c FROM sbtest1 WHERE id=10;
# User@Host: sbuser[sbuser] @  [127.0.0.1]  Id: 90387777
# Schema: sysbench  Last_errno: 0  Killed: 0
# Query_time: 0.000029  Lock_time: 0.000000  Rows_sent: 0  Rows_examined: 0  Rows_affected: 0
# Bytes_sent: 11  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
# QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# Filesort: No  Filesort_on_disk: No  Merge_passes: 0
# No InnoDB statistics available for this query
SET timestamp=1521634383;
BEGIN;
# User@Host: sbuser[sbuser] @  [127.0.0.1]  Id: 90387777
# Schema: sysbench  Last_errno: 0  Killed: 0
# Query_time: 0.000029  Lock_time: 0.000000  Rows_sent: 0  Rows_examined: 0  Rows_affected: 0
# Bytes_sent: 11  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
# QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# Filesort: No  Filesort_on_disk: No  Merge_passes: 0
# No InnoDB statistics available for this query
SET timestamp=1521634383;
SELECT c FROM sbtest1 WHERE id=200;
# User@Host: sbuser[sbuser] @  [127.0.0.1]  Id: 90387777
# Schema: sysbench  Last_errno: 0  Killed: 0
# Query_time: 0.000029  Lock_time: 0.000000  Rows_sent: 0  Rows_examined: 0  Rows_affected: 0
# Bytes_sent: 11  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
# QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# Filesort: No  Filesort_on_disk: No  Merge_passes: 0
# No InnoDB statistics available for this query
SET timestamp=1521634383;
commit;
```

There are two queries and one transaction here. The tool is as parsing the slow query log it creates fingerprints from the queries.

From this query:
```
SELECT c FROM sbtest1 WHERE id=51;
```
Will be this:
```
SELECT c FROM sbtest1 WHERE id=$<v1>;
```

It is going to store `51` as a variable for this query. As reading the slow query log and if there is another query which has 
the same fingerprint it is going to merge the variables.
In this case `$<v1> = "51,10"`

But you could ask why not: `$<v1> = "51,10,200"`
Because the third query is in a transaction and going to handle that separately. 

# How to Run it?

It only needs the slow query log file.

```
python3 pyLoad.py -i query.slow.log
```
Help menu:

```
Usage:
    Process SlowLog File: pyLoad.py -i <slowlogFile> 
    -d,--debug = debug
    -o,--outputFile = Creates CSV files for debug purposes.
    -t,testrun = Going to run on the "test_file.source.log"
    
```

# LUA scripts

The output of the script is 3 LUA file.

**run_common.lua**

It is very similar to the `oltp_common.lua` in Sysbench.

```
require("run_variables")

function interp(s, tab)
  return (s:gsub('($%b<>)', function(w) return tab[w:sub(3, -2)] or w end))
end

getmetatable("").__mod = interp

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: run")
end

sysbench.cmdline.options = {
    point_selects = {"Number of point SELECT queries to run", 5},
    skip_trx = {"Do not use BEGIN/COMMIT; Use global auto_commit value", false},
     q1 = {"q1", 1},
     q4 = {"q4", 1},
}


function execute_q1()
   for i = 1, sysbench.opt.q1 do
            
            con:query("SELECT c FROM sbtest1 WHERE id= $<v1>;" % { v1 = get_random(q1_v1) })
   end
end
function execute_q4()
   for i = 1, sysbench.opt.q4 do
            
            con:query("SELECT c FROM sbtest1 WHERE id= $<v1>;" % { v1 = get_random(q4_v1) })
   end
end

function execute_selects()
    -- loop for however many the user wants to execute
    for i = 1, sysbench.opt.point_selects do
        con:query(string.format(randQuery, id))
    end
end


-- Called by sysbench to initialize script
function thread_init()

    -- globals for script
    drv = sysbench.sql.driver()
    con = drv:connect()
end


-- Called by sysbench when tests are done
function thread_done()

    con:disconnect()
end

```

Name of the queries are going to be `q1` and `q2`.

Contains all the basic information and settings, all the queries will run once in a cycle but we can increase that one
if we would like to put more pressure to a table or test hotspots etc.. 


**run_variables.lua**

This contains all the variables and values.

```
function get_random(var)
    i = math.random(1, #var )
    return var[i]
end

q1_v1={10, 51}
q4_v1={200}
```

Basically we just list all the possible values for that variable. 
If you want to keep it up to date you can just select a few hundred or thousand values from your table and insert it here.

**run_pyload.lua**

This one is very similar to `oltp_read_write.lua` from Sysbench.

```
require("run_common")

function event()
        if not sysbench.opt.skip_trx then
            con:query("BEGIN")
        end

           execute_q4()

        if not sysbench.opt.skip_trx then
            con:query("COMMIT")
        end

    execute_q1()

end

```

`q4` is going to run inside a transaction because it was running in a transaction.
But `q1` is running outside of the transaction.

# How to start sysbench?

Just like a normal Sysbench command:

```
sysbench run_pyload.lua --mysql-port=18923 --mysql-host=127.0.0.1 --mysql-user=msandbox --mysql-password=msandbox  --mysql-db=sysbench --report-interval=1 run
```

We have to use the `run_pyload.lua` script and we have to define port,host, user and database.

We will see the same output as with the traditional Sybench scripts.
```
sysbench 1.0.19 (using bundled LuaJIT 2.1.0-beta2)

Running the test with following options:
Number of threads: 1
Report intermediate results every 1 second(s)
Initializing random number generator from current time


Initializing worker threads...

Threads started!

[ 1s ] thds: 1 tps: 2408.01 qps: 9635.01 (r/w/o: 4817.01/0.00/4818.00) lat (ms,95%): 0.95 err/s: 0.00 reconn/s: 0.00
[ 2s ] thds: 1 tps: 1940.31 qps: 7760.23 (r/w/o: 3880.62/0.00/3879.61) lat (ms,95%): 1.16 err/s: 0.00 reconn/s: 0.00
[ 3s ] thds: 1 tps: 2405.19 qps: 9620.74 (r/w/o: 4810.37/0.00/4810.37) lat (ms,95%): 0.95 err/s: 0.00 reconn/s: 0.00
[ 4s ] thds: 1 tps: 1972.70 qps: 7890.80 (r/w/o: 3945.40/0.00/3945.40) lat (ms,95%): 1.16 err/s: 0.00 reconn/s: 0.00
[ 5s ] thds: 1 tps: 2473.22 qps: 9891.87 (r/w/o: 4945.43/0.00/4946.43) lat (ms,95%): 0.92 err/s: 0.00 reconn/s: 0.00
[ 6s ] thds: 1 tps: 1923.05 qps: 7692.20 (r/w/o: 3846.10/0.00/3846.10) lat (ms,95%): 1.18 err/s: 0.00 reconn/s: 0.00
[ 7s ] thds: 1 tps: 2339.68 qps: 9360.74 (r/w/o: 4680.37/0.00/4680.37) lat (ms,95%): 0.99 err/s: 0.00 reconn/s: 0.00
[ 8s ] thds: 1 tps: 1914.49 qps: 7657.97 (r/w/o: 3828.98/0.00/3828.98) lat (ms,95%): 1.16 err/s: 0.00 reconn/s: 0.00
[ 9s ] thds: 1 tps: 2447.50 qps: 9789.99 (r/w/o: 4895.00/0.00/4895.00) lat (ms,95%): 0.99 err/s: 0.00 reconn/s: 0.00
SQL statistics:
    queries performed:
        read:                            43334
        write:                           0
        other:                           43334
        total:                           86668
    transactions:                        21667  (2166.26 per sec.)
    queries:                             86668  (8665.04 per sec.)
    ignored errors:                      0      (0.00 per sec.)
    reconnects:                          0      (0.00 per sec.)

General statistics:
    total time:                          10.0006s
    total number of events:              21667

Latency (ms):
         min:                                    0.24
         avg:                                    0.46
         max:                                   16.96
         95th percentile:                        1.06
         sum:                                 9953.61

Threads fairness:
    events (avg/stddev):           21667.0000/0.00
    execution time (avg/stddev):   9.9536/0.00
```

# Known errors during Sysbench execution

Probably the most common issue is going to be the `Duplicate key Error`, because the script is working on a set of values 
it might going to try to insert the same row multiple times.

What can we do? We can ignore these errors:

```--mysql-ignore-errors=[LIST,...] list of errors to ignore, or "all" [1213,1020,1205]```

Duplicate key errors are still going to happen but Sysbench will not stop now.

Another option is to manually add a `Delete` query before the `Insert`. 
You will not get errors anymore but you will introduce an additional query what you do not have in production.

# Known Issues

Creating finger prints and grabbing the values from the queries required a lot of regexp and sting parsing.
I have tested it on many different queries but I am sure there are going to be queries which would cause issues for the script.
If you find such a queries please share them with me so I can update the script.

