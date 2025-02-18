-- Step 1 
-- Set these properties

Set hive.support.concurrency = true;
Set hive.enforce.bucketing = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.compactor.initiator.on = true;
set hive.compactor.worker.threads =1;


-- Step 2
-- Rules to create a Acid Table
-- 1: Table should be an internal table
-- 2: Table must be a Bucketed table
-- 3: Storage format ORC
-- 4: Enable 'transactional'='true' in Table properties

create table acid_example  
(sno int, name string, city string) 
clustered by (city) into 3 buckets row format delimited fields terminated by ',' lines terminated by '\n' stored as orc TBLPROPERTIES ('transactional'='true') ;


select * from acid_example;


-- Step 3

insert into acid_example (sno,name,city) values (1,'shyam','pune');

select * from acid_example;

-- Step 4

update acid_example set name='ram' where sno=1;

select * from acid_example;


-- Step 5

delete from acid_example where sno=1;

select * from acid_example;
