#!/bin/bash -x

POSITIONAL_ARGS=()
KILL=1

record_time=60
wake_time=1000000

while [[ $# -gt 0 ]]; do
  case $1 in
    --no-kill)
      KILL=0
      shift
      ;;
    --record-time)
      record_time=$2
      shift
      shift
      ;;
    --wake-time)
      wake_time=$2
      shift
      shift
      ;;
    *)
      echo unknown args $1
      exit 1
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters

base_dir=/home/zhaoyiping/ob-deploy/

function gen_flame_graph() {
  /home/zhaoyiping/FlameGraph/flamegraph.pl --height 32 --title $1 svg/$1.stack > svg/$1.svg
}

function gen_flame_graph_reverse() {
  /home/zhaoyiping/FlameGraph/flamegraph.pl --height 32 --title $1 --reverse svg/$1.stack > svg/$1-r.svg
}

ps a | grep 'python3 deploy.py' | grep -v grep | awk '{ print $1 }' | xargs kill -9
ps a | grep '/usr/share/bcc/tools/profile' | grep -v grep | awk '{ print $1 }' | xargs kill -9
ps a | grep '/usr/share/bcc/tools/offcputime' | grep -v grep | awk '{ print $1 }' | xargs kill -9
python3 deploy.py --cluster-home-path $base_dir --clean
python3 deploy.py --cluster-home-path $base_dir &
python_pid=$!
sleep 1
ob_pid=`ps aux | grep "bin/observer" | grep -v grep | awk '{ print $2 }'`
/usr/share/bcc/tools/profile -p $ob_pid -f -F 99 $record_time > svg/profile.stack &
profile_pid=$!
/usr/share/bcc/tools/offcputime -p $ob_pid -f $record_time -U > svg/offcputime.stack &
offcputime_pid=$!
/usr/share/bcc/tools/offwaketime -m $wake_time -p $ob_pid $record_time > svg/offwaketime.stack &
offwaketime_pid=$!
wait $python_pid
wait $profile_pid
wait $offcputime_pid
wait $offwaketime_pid
gen_flame_graph offwaketime
gen_flame_graph profile
gen_flame_graph offcputime 
gen_flame_graph_reverse offcputime 
if [[ $KILL -eq 1 ]];then
  kill -9 $ob_pid
fi
