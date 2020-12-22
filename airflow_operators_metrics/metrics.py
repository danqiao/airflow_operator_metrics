import logging
import typing as t

import psutil
from prometheus_client import Gauge, Summary

logger = logging.getLogger(__name__)

COLLECT_TIME = Summary('airflow_collecting_stats_seconds',
                       'Time spent processing collecting stats')


class ProcessMetrics(t.NamedTuple):
    dag: str
    operator: str
    exec_date: str
    is_local: bool
    is_raw: bool

    mem_rss: int
    mem_vms: int
    mem_shared: int
    mem_text: int
    mem_data: int
    mem_lib: int
    mem_uss: int
    mem_pss: int
    mem_swap: int

    # cpu_num: int
    cpu_percent: float
    cpu_times_user: float
    cpu_times_system: float


class MetricsContainer:
    def __init__(self, prefix=None,
                 global_labels: t.Optional[t.Dict[str, str]]=None):
        self._prefix = prefix
        self._global_labels = global_labels
        labels = ('name', 'dag', 'operator', 'exec_date')
        if global_labels:
            labels = labels + tuple(global_labels.keys())

        def gauge(name, documentation, labelnames=(), *args, **kwargs):
            if prefix:
                name = f'{prefix}_{name}'
            labelnames += labels
            return Gauge(name, documentation, labelnames, *args, **kwargs)

        self._mem_rss = gauge('airflow_process_mem_rss',
                              'Non-swapped physical memory')
        self._mem_vms = gauge('airflow_process_mem_vms',
                              'Amount of virtual memory')
        self._mem_shared = gauge('airflow_process_mem_shared',
                                 'Amount of shared memory')
        self._mem_text = gauge('airflow_process_mem_text',
                               'Devoted to executable code')
        self._mem_lib = gauge('airflow_process_mem_lib', 'Used by shared libraries')
        self._mem_uss = gauge('airflow_process_mem_uss',
                              'Mem unique to a process and which would be freed '
                              'if the process was terminated right now')
        self._mem_swap = gauge('airflow_process_mem_swap',
                               'Amount of swapped memory')
        self._mem_pss = gauge('airflow_process_mem_pss',
                              'Shared with other processes, accounted in a way that '
                              'the amount is divided evenly between processes '
                              'that share it')

        # self._cpu_num = gauge('airflow_process_mem_swap',
        # 'Amount of swapped memory')
        self._cpu_percent = gauge('airflow_process_cpu_percent',
                                  'System-wide CPU utilization as a percentage '
                                  'of the process')
        self._cpu_times_user = gauge('airflow_process_cpu_times_user',
                                     'CPU times user')
        self._cpu_times_system = gauge('airflow_process_cpu_times_system',
                                       'CPU times system')

    @COLLECT_TIME.time()
    def collect(self):
        handled = 0
        self._reset()
        for process_metrics in _get_processes_metrics():
            self._handle_process_metrics(process_metrics)
            handled += 1
        logger.info(f'Gathered metrics from {handled} processes')

    def _handle_process_metrics(self, metrics: ProcessMetrics):
        name = _get_process_name(metrics)
        labels = {'name': name, 'dag': metrics.dag,
                  'operator': metrics.operator, 'exec_date': metrics.exec_date}
        if self._global_labels:
            labels.update(self._global_labels)

        self._mem_rss.labels(**labels).set(metrics.mem_rss)
        self._mem_vms.labels(**labels).set(metrics.mem_vms)
        self._mem_shared.labels(**labels).set(metrics.mem_shared)
        self._mem_text.labels(**labels).set(metrics.mem_text)
        self._mem_uss.labels(**labels).set(metrics.mem_uss)
        self._mem_swap.labels(**labels).set(metrics.mem_swap)
        self._mem_pss.labels(**labels).set(metrics.mem_pss)

        self._cpu_percent.labels(**labels).set(metrics.cpu_percent)
        self._cpu_times_user.labels(**labels).set(metrics.cpu_times_user)
        self._cpu_times_system.labels(**labels).set(metrics.cpu_times_system)

    def _reset(self):
        self._mem_rss._metrics = {}
        self._mem_vms._metrics = {}
        self._mem_shared._metrics = {}
        self._mem_text._metrics = {}
        self._mem_uss._metrics = {}
        self._mem_swap._metrics = {}
        self._mem_pss._metrics = {}

        self._cpu_percent._metrics = {}
        self._cpu_times_user._metrics = {}
        self._cpu_times_system._metrics = {}


def _get_processes_metrics() -> t.Iterator[ProcessMetrics]:
    for process in psutil.process_iter():
        try:
            airflow_data = get_airflow_data(process)
            if not airflow_data:
                continue
            mem = process.memory_full_info()
            cpu_times = process.cpu_times()
            cpu_percent = process.cpu_percent()
        except psutil.NoSuchProcess:
            continue

        yield ProcessMetrics(
            dag=airflow_data['dag'],
            operator=airflow_data['operator'],
            exec_date=airflow_data['exec_date'],
            is_local=airflow_data['is_local'],
            is_raw=airflow_data['is_raw'],

            mem_rss=mem.rss,
            mem_vms=mem.vms,
            mem_shared=mem.shared,
            mem_text=mem.text,
            mem_data=mem.data,
            mem_lib=mem.lib,
            mem_uss=mem.uss,
            mem_pss=mem.pss,
            mem_swap=mem.swap,

            cpu_percent=cpu_percent,
            cpu_times_user=cpu_times.user,
            cpu_times_system=cpu_times.system,
        )


def _get_process_name(metrics: ProcessMetrics):
    dag, operator = metrics.dag, metrics.operator
    if dag not in operator:
        name_parts = [f'{dag}.{operator}']
    else:
        name_parts = [operator]
    name_parts.append(metrics.exec_date)
    if metrics.is_local:
        name_parts.append('local')
    if metrics.is_raw:
        name_parts.append('is_raw')
    return '_'.join(name_parts)


#用Python来编写脚本简化日常的运维工作是Python的一个重要用途。
# 在Linux下，有许多系统命令可以让我们时刻监控系统运行的状态，如ps，top，free等等。要获取这些系统信息，
# Python可以通过subprocess模块调用并获取结果。但这样做显得很麻烦，尤其是要写很多解析代码。
# 在Python中获取系统信息的另一个好办法是使用psutil这个第三方模块。
# 顾名思义，psutil = process and system utilities，
# 它不仅可以通过一两行代码实现系统监控，还可以跨平台使用，支持Linux／UNIX／OSX／Windows等，
# 是系统管理员和运维小伙伴不可或缺的必备模块。

def get_airflow_data(process: psutil.Process) -> t.Optional[t.Dict[str, t.Union[str, bool]]]:
    cmdline = process.cmdline()
    logger.info(f'process: {cmdline}')
    if not cmdline or not cmdline[0].startswith('/usr/bin/python'):
        return None

    for cmd_arg in cmdline:
        if 'airflow run' not in cmd_arg:
            continue

        airflow_args = cmd_arg.split()
        dag = airflow_args[3]
        operator = airflow_args[4]
        exec_date = airflow_args[5][5:25]
        is_local = any([i == '--local' for i in airflow_args])
        is_raw = any([i == '--raw' for i in airflow_args])

        return {
            'dag': dag,
            'operator': operator,
            'exec_date': exec_date,
            'is_local': is_local,
            'is_raw': is_raw,
        }
