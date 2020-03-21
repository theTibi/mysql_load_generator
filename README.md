# What is mysql_load_generator?

It is reading and parsing the Slow Query log file, from that it creates LUA files what Sysbench can use. 
Because it is based on the slow query log it is going to use real life production queries.

**It is not a benchmark tool!** The purpose of the tool is to generate a steady workload with your production queries.

As it is parsing the queries it is going to collect and store the actual values used in those type if queries.
The LUA scripts are going to use these real production values in the queries.

You can easily change/update these values by yourself as well. 

The output of the script is three LUA file which are very similar to the standard OLTP Sysbench LUA files.
This is on purpose because Sysbench is a well known tool and easy to use and easy to modify the script if it is necessary. 

#Example

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