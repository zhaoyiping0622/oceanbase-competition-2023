#!/bin/bash

function run_tests(){
  obd test sysbench obcluster --time=14 --threads=5 --tables=4 --table-size=10863 --database=test --tenant=test --script-name=oltp_point_select
  obd test tpcc obcluster --tenant=test --database=test --terminals=10 --run-mins=1 --warehouses=1 --optimization=0
  mysql -S /home/zhaoyiping/ob-deploy/run/sql.sock -uroot@test -e "SET GLOBAL secure_file_priv = '/'"
  mysql -S /home/zhaoyiping/ob-deploy/run/sql.sock -uroot@test -e "SET GLOBAL ob_query_timeout = 60000000"
  obd test tpch obcluster --tenant=test --database=test --remote-tbl-dir=/data/obcluster/tbl --scale-factor=1 --optimization=0
}

run_tests
obd cluster restart obcluster
run_tests
