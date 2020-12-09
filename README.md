
### Run

    python main.py [FULL|LOG] port [instance_name]

### SQL

```sql
-- on primary server
-- generate sql to run logshipping backup jobs
select 'exec msdb..sp_start_job @job_name=''' + name + ''';' from msdb..sysjobs where name like 'LSBackup_%';

-- on secondary server
-- generate sql to run logshipping copy jobs
select 'exec msdb..sp_start_job @job_name=''' + name + ''';' from msdb..sysjobs where name like 'LSCopy_%';

-- on secondary server
-- generate sql to run logshipping restore jobs
select 'exec msdb..sp_start_job @job_name=''' + name + ''';' from msdb..sysjobs where name like 'LSRestore_%';

-- on secondary server
-- generate sql to convert logshipping databases to read-write
select 'RESTORE LOG ' + name + ' WITH RECOVERY;' from sys.databases where database_id>4 and (state_desc='RESTORING' or is_in_standby=1);
```
