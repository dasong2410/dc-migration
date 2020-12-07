
### Run

    python main.py [FULL|LOG] port instance_name

### SQL

```sql
select 'RESTORE LOG ' + name + ' WITH RECOVERY;' from sys.databases where database_id>4 and state_desc='RESTORING';
```
