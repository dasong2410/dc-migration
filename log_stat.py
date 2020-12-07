import glob
import os

# Script root dir
ROOT_DIR = os.path.split(os.path.realpath(__file__))[0]

db_dict = {}

log_files = glob.glob('%s/log/stat*.log*' % ROOT_DIR)
log_files.reverse()
for lf in log_files:
    log_filename = os.path.basename(lf)
    inst_name = log_filename.split(".log")[0]
    # print(inst_name, log_filename, lf)

    log_file = open(lf, "r")
    for line in log_file:
        line_fields = line.split(" -> ")
        db_name = line_fields[1]
        db_name = db_name[1:len(db_name)-1]
        db_full_name = "%s -> %s" % (inst_name, db_name)
        trans_logs = db_dict.get(db_full_name)
        if trans_logs is None:
            trans_logs = []
            db_dict[db_full_name] = trans_logs

        trans_log_file = line_fields[2]
        start_idx = trans_log_file.rindex("\\") + 1
        end_idx = len(trans_log_file) - 1
        trans_log_file = trans_log_file[start_idx:end_idx]
        trans_logs.append(trans_log_file)

        # print(db_name, trans_log_file)
    log_file.close()

for k in sorted(db_dict.keys()):
    v = db_dict.get(k)
    k_fields = k.split(" -> ")
    i_name = k_fields[0]
    if i_name == "LOG":
        i_name = "MSSQLSERVER"
    else:
        i_name = i_name.replace("LOG_", "")
    d_name = k_fields[1]
    v.reverse()
    print("%s -> %s: " % (i_name.ljust(15, " "), d_name.ljust(25, " ")), v[0])
