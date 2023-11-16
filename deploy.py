import pymysql as mysql
import argparse
import time
import datetime
import subprocess
import os
import sys
import shutil
import logging
import traceback

_logger = logging.getLogger('DeployDemo')
begin_time = datetime.datetime.now()

def param_check(args):
    # TODO
    return True

def __data_path(cluster_home_path:str) -> str:
    return os.path.join(cluster_home_path, 'store')

def __observer_bin_path(cluster_home_path:str) -> str:
    return os.path.join(cluster_home_path, 'bin', 'observer')

def __build_env(cluster_home_path:str) -> None:
    paths_to_create = ['clog', 'sstable', 'slog']
    data_path = __data_path(cluster_home_path)
    for path in paths_to_create:
        path_to_create = os.path.join(data_path, path)
        os.makedirs(path_to_create, exist_ok=True)

def __clear_env(cluster_home_path:str) -> None:
    observer_bin_path = __observer_bin_path(cluster_home_path)
    pid = subprocess.getoutput(f'pidof {observer_bin_path}')
    if pid:
        subprocess.run(f'kill -9 {pid}', shell=True)

    paths_to_clear = ['audit', 'etc', 'etc2', 'etc3', 'log', 'run']
    for path in paths_to_clear:
        path_to_clear = os.path.join(cluster_home_path, path)
        shutil.rmtree(path_to_clear, ignore_errors=True)

def __try_to_connect(host, mysql_port:int, *, timeout_seconds=60):
    error_return = None
    for _ in range(0, timeout_seconds):
        try:
            return mysql.connect(host=host, user="root", port=mysql_port, passwd="")
        except mysql.err.Error as error:
            error_return = error
            time.sleep(1)

    _logger.info('failed to connect to observer fater %f seconds', timeout_seconds)
    raise error_return

def __create_tenant(cursor, *,
                    cpu:int,
                    memory_size:int,
                    unit_name:str='test_unit',
                    resource_pool_name:str='test_pool',
                    zone_name:str='zone1',
                    tenant_name:str='test') -> None:
    create_unit_sql = f'CREATE RESOURCE UNIT {unit_name} max_cpu={cpu},min_cpu={cpu}, memory_size={memory_size};'
    create_resource_pool_sql = f"CREATE RESOURCE POOL {resource_pool_name} unit='{unit_name}', unit_num=1,ZONE_LIST=('{zone_name}');"
    create_tenant_sql = f"CREATE TENANT IF NOT EXISTS {tenant_name} resource_pool_list = ('{resource_pool_name}') set ob_tcp_invited_nodes = '%';"

    cursor.execute(create_unit_sql)
    _logger.info(f'unit create done: {create_unit_sql}, time {(datetime.datetime.now()-begin_time).total_seconds()*1000} ms')

    cursor.execute(create_resource_pool_sql)
    _logger.info(f'resource pool create done: {create_resource_pool_sql}, time {(datetime.datetime.now()-begin_time).total_seconds()*1000} ms')

    cursor.execute(create_tenant_sql)
    _logger.info(f'tenant create done: {create_tenant_sql}, time {(datetime.datetime.now()-begin_time).total_seconds()*1000} ms')


if __name__ == "__main__":
    log_level = logging.INFO
    log_format = "%(asctime)s.%(msecs)03d [%(levelname)-5s] - %(message)s " \
                "(%(name)s [%(funcName)s@%(filename)s:%(lineno)s] [%(threadName)s] P:%(process)d T:%(thread)d)"
    log_date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(format=log_format, level=log_level, datefmt=log_date_format, stream=sys.stdout)

    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster-home-path", dest='cluster_home_path', type=str, help="the path of sys log / config file / sql.sock / audit info")
    parser.add_argument("--only_build_env", action='store_true', help="build env & start observer without bootstrap and basic check")
    parser.add_argument('--clean', action='store_true', help='clean deploy directory and exit')
    parser.add_argument("-p", dest="mysql_port", type=str, default="2881")
    parser.add_argument("-P", dest="rpc_port", type=str, default="2882")
    parser.add_argument("-z", dest="zone", type=str, default="zone1")
    parser.add_argument("-c", dest="cluster_id", type=str, default="1")
    parser.add_argument("-i", dest="devname", type=str, default="lo")
    parser.add_argument("-I", dest="ip", type=str, default="127.0.0.1")
    parser.add_argument("-o", dest="opt_str", type=str, default="__min_full_resource_pool_memory=1073741824,datafile_size=10G,datafile_next=10G,datafile_maxsize=20G,log_disk_size=40G,memory_limit=10G,system_memory=1G,cpu_count=24,cache_wash_threshold=1G,workers_per_cpu_quota=10,schema_history_expire_time=1d,net_thread_count=4,syslog_io_bandwidth_limit=10G")

    tenant_group = parser.add_argument_group('tenant', 'tenant options')
    tenant_group.add_argument('--tenant-name', dest='tenant_name', type=str, default='test')
    tenant_group.add_argument('--tenant-resource-pool-name', dest='tenant_resource_pool_name', type=str, default='test_pool')
    tenant_group.add_argument('--tenant-unit-name', dest='tenant_unit_name', type=str, default='test_unit')
    tenant_group.add_argument('--tenant-cpu', dest='tenant_cpu', type=str, default='18')
    tenant_group.add_argument('--tenant-memory', dest='tenant_memory', type=str, default='8589934592')

    args = parser.parse_args()

    if not param_check(args):
        _logger.error("param check failed")
        exit(1)

    home_abs_path = os.path.abspath(args.cluster_home_path)
    bin_abs_path = __observer_bin_path(home_abs_path)
    data_abs_path = os.path.abspath(__data_path(args.cluster_home_path))

    if args.clean:
        __clear_env(home_abs_path)
        exit(0)

    __build_env(home_abs_path)

    rootservice = f'{args.ip}:{args.rpc_port}'
    observer_args = f"-p {args.mysql_port} -P {args.rpc_port} -z {args.zone} -c {args.cluster_id} -d {data_abs_path} -i {args.devname} -r {rootservice} -I {args.ip} -o {args.opt_str}"

    os.chdir(args.cluster_home_path)
    observer_cmd = f"{bin_abs_path} {observer_args}"
    _logger.info(observer_cmd)
    shell_result = subprocess.run(observer_cmd, shell=True)
    _logger.info('deploy done. returncode=%d', shell_result.returncode)

    time.sleep(60)
    try:
        db = __try_to_connect(args.ip, int(args.mysql_port))
        cursor = db.cursor(cursor=mysql.cursors.DictCursor)
        _logger.info(f'connect to server success! host={args.ip}, port={args.mysql_port}')

        _logger.info(f'before bootstrap {(datetime.datetime.now()-begin_time).total_seconds()*1000} ms')
        print(f"ALTER SYSTEM BOOTSTRAP ZONE '{args.zone}' SERVER '{rootservice}'")
        # cursor.execute(f"ALTER SYSTEM BOOTSTRAP ZONE '{args.zone}' SERVER '{rootservice}'")
        _logger.info('bootstrap success: %s ms' % ((datetime.datetime.now() - begin_time).total_seconds() * 1000))
        # checkout server status
        cursor.execute("select * from oceanbase.__all_server")
        server_status = cursor.fetchall()
        if len(server_status) != 1 or server_status[0]['status'] != 'ACTIVE':
            _logger.info("get server status failed")
            exit(1)
        _logger.info('checkout server status ok')
        # ObRootService::check_config_result

        # __create_tenant(cursor,
        #                 cpu=args.tenant_cpu,
        #                 memory_size=args.tenant_memory,
        #                 unit_name=args.tenant_unit_name,
        #                 resource_pool_name=args.tenant_resource_pool_name,
        #                 zone_name=args.zone,
        #                 tenant_name=args.tenant_name)
        _logger.info('create tenant done')

    except mysql.err.Error as e:
        with open(f"{data_abs_path}/../log/observer.log.wf", "r") as f:
            print("".join(f.readlines()))
        _logger.info("deploy observer failed. ex=%s", str(e))
        _logger.info(traceback.format_exc())
        exit(1)
    except Exception as ex:
        _logger.info('exception: %s', str(ex))
        _logger.info(traceback.format_exc())
        exit(1)
