#!/bin/bash

CSV_PATH=$1
DEL_PATH=$2

user="root"

MYSQL_PWD='abcd1234' mysql --local-infile=1 -u"$user" <<EOF
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';

LOAD DATA LOCAL INFILE '$CSV_PATH'
INTO TABLE history_db.tmp_cmd_usage
CHARACTER SET latin1
FIELDS
        TERMINATED BY ','
        ENCLOSED BY '^'
        ESCAPED BY '\b'
LINES TERMINATED BY '\n';
EOF
