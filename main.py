import configparser
import datetime
import glob
import logging
import os
import pathlib
import platform
import sys
from logging.handlers import TimedRotatingFileHandler

import _mssql
import pymssql

# Script root dir
ROOT_DIR = os.path.split(os.path.realpath(__file__))[0]

# Need a param to judge which kind of files we need to copy
# FULL or LOG
if len(sys.argv) < 2:
    print("Please give me a param[FULL|LOG], so I can figure out what you want me to do...")
    sys.exit(1)

# FULL or LOG
item_type = sys.argv[1]

port = 1435
# db port
if len(sys.argv) == 3:
    port = int(sys.argv[2])

# db instance name
instance_name = ""
instance_log_id = ""
if len(sys.argv) == 4:
    instance_name = "$" + sys.argv[3]
    instance_log_id = "_" + sys.argv[3]

config = configparser.ConfigParser()
config.read(ROOT_DIR + "/cfg/conf.cfg")
log_level = config.get("General", "LOG_LEVEL")
backup_file_dir_pattern = config.get(item_type, "BACKUP_FILE_DIR_PATTERN")
backup_file_dir_pattern = backup_file_dir_pattern.replace("[HOST_NAME]", platform.node())
backup_file_dir_pattern = backup_file_dir_pattern.replace("[INSTANCE_NAME]", instance_name)
backup_suffix = config.get(item_type, "BACKUP_SUFFIX")

my_logger = None


# setup logger
def set_logger():
    global my_logger

    my_logger = logging.getLogger(item_type + instance_log_id)
    my_logger.setLevel(log_level)
    log_file = '%s/log/stat%s.log' % (ROOT_DIR, instance_log_id)
    fh = TimedRotatingFileHandler(log_file, 'midnight', 1, 7)
    formatter = logging.Formatter('%(asctime)s %(message)s')
    fh.setFormatter(formatter)
    my_logger.addHandler(fh)


sql_drop_db = """
declare @DBExist int
select @DBExist = count(1) from sys.databases where name='{db_name}' and state_desc='ONLINE'

if @DBExist = 1
    ALTER DATABASE {db_name} SET SINGLE_USER WITH ROLLBACK IMMEDIATE

DROP DATABASE if exists {db_name}
"""

sql_restore_db = """
restore database {0} from disk='{1}' with norecovery
"""

sql_restore_log = """
RESTORE LOG {0} FROM DISK = '{1}' WITH NORECOVERY
"""

sql_db_file_dir = """
declare @Command varchar(1024)
declare @FileList table
(
  LogicalName          nvarchar(128),
  PhysicalName         nvarchar(260),
  Type                 char(1),
  FileGroupName        nvarchar(128),
  Size                 numeric(20, 0),
  MaxSize              numeric(20, 0),
  FileID               bigint,
  CreateLSN            numeric(25, 0),
  DropLSN              numeric(25, 0),
  UniqueID             uniqueidentifier,
  ReadOnlyLSN          numeric(25, 0),
  ReadWriteLSN         numeric(25, 0),
  BackupSizeInBytes    bigint,
  SourceBlockSize      int,
  FileGroupID          int,
  LogGroupGUID         uniqueidentifier,
  DifferentialBaseLSN  numeric(25, 0),
  DifferentialBaseGUID uniqueidentifier,
  IsReadOnly           bit,
  IsPresent            bit,
  TDEThumbprint        varbinary(32),
  SnapshotURL          nvarchar(360)
)

--set @Command = 'restore filelistonly from disk='{0}'
set @Command = 'restore filelistonly from disk=''{0}'''

insert into @FileList
exec (@Command)
select left(PhysicalName, len(PhysicalName) - charindex('\\', reverse(PhysicalName) + '\\')) data_file_dir from @FileList
"""

sys_dbs = ['master', 'model', 'msdb']
db_name_parent_dir = backup_file_dir_pattern.split("[DATABASE_NAME]")[0]
db_names = os.listdir(db_name_parent_dir)


def msg(msg):
    t = datetime.datetime.now()
    print("%s - %s" % (t, msg))


def restore_db(port):
    cnxn = pymssql.connect(server='localhost', database='master', port=port, autocommit=True)
    cursor = cnxn.cursor()
    with cnxn:
        for dbName in db_names:
            msg("Restoring database[%s]: %s..." % (dbName, item_type))

            if dbName.lower() not in sys_dbs:
                backup_file_dir = backup_file_dir_pattern.replace("[DATABASE_NAME]", dbName)
                backup_files = glob.glob("%s/*.%s" % (backup_file_dir, backup_suffix))

                # skip if no backup file exist
                if len(backup_files) > 0:
                    # drop database
                    msg("-> Drop database if it exists")
                    cursor.execute(sql_drop_db.format(db_name=dbName))

                    # create data file dirs
                    msg("-> Create data file dirs if they don't exist")
                    backup_files.sort(reverse=True)
                    for f in backup_files[:1]:
                        # print(f)
                        cursor.execute(sql_db_file_dir.format(f))
                        row = cursor.fetchone()
                        while row:
                            db_file_dir = row[0]
                            if not os.path.exists(db_file_dir):
                                pathlib.Path(db_file_dir).mkdir(parents=True, exist_ok=True)
                            else:
                                msg("   -> Dir exists: %s" % db_file_dir)
                            row = cursor.fetchone()

                    # restore database
                    msg("-> Restore database[%s -> %s]" % (dbName, f))
                    cursor.execute(sql_restore_db.format(dbName, f))

                    msg("-> Done\n")
                    my_logger.info("  [Done] -> [%s] -> Database restored: %s" % (dbName, f))
                else:
                    msg("-> No backup file exist[%s], skip" % dbName)
            else:
                msg("-> System database[%s], don't need to restore\n" % dbName)


def restore_log(port):
    cnxn = pymssql.connect(server='localhost', database='master', port=port, autocommit=True)
    cursor = cnxn.cursor()
    with cnxn:
        for dbName in db_names:
            msg("Restoring database[%s]: %s" % (dbName, item_type))

            if dbName.lower() not in sys_dbs:
                backup_file_dir = backup_file_dir_pattern.replace("[DATABASE_NAME]", dbName)
                backup_files = glob.glob("%s/*.%s" % (backup_file_dir, backup_suffix))
                for f in backup_files:
                    try:
                        cursor.execute(sql_restore_log.format(dbName, f))
                        msg("  -> [Done] -> [%s] -> Log restored: %s" % (dbName, f))
                        my_logger.info("  [Done] -> [%s] -> Log restored: %s" % (dbName, f))
                    except _mssql.MSSQLDatabaseException as e1:
                        msg("  -> [Error]:[MSSQLDriverException] -> %s -> %s" % (f, e1))
                    except pymssql.OperationalError as e2:
                        msg("  -> [Error]:[MSSQLDatabaseException] -> %s -> %s" % (f, e2))
            else:
                msg("  -> System database[%s], don't need to restore" % dbName)


def main():
    set_logger()

    if item_type == "FULL":
        restore_db(port)
    elif item_type == "LOG":
        restore_log(port)
    else:
        print("Wrong paramter")


if __name__ == '__main__':
    main()
