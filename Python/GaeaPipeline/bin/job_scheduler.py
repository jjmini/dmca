#!/usr/bin/env python
# encoding: utf-8
import os
import subprocess
import sys
from Queue import Queue
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter
from threading import Thread, Condition

import time

from gaeautils import Logger, printtime
from gaeautils.bundle import bundle
from gaeautils.parseConfig import ParseConfig

__all__ = []
__version__ = '0.1'
__date__ = '2017-05-20'
__updated__ = '2017-05-20'

_sentinel = object()

class Task(Thread):
    def __init__(self, state, queue, ne_queue):
        super(Task, self).__init__()
        self.queue = queue
        self.ne_queue = ne_queue
        self.state = state
        self.other_queue_steps = set()
        self.sample_fail = False  # those tasks in this sample is failed

    def is_exclusive_task(self, task_name):
        for dep_step in self.state.analysisDict[task_name].depend:
            if dep_step in self.other_queue_steps:
                return False

        if 'exclusive_task' not in self.state[task_name]:
            return True

        def astrcmp(x, y):
            return x.lower() == y.lower()

        if astrcmp(self.state[task_name].exclusive_task, 'false') or self.state[task_name].exclusive_task is False:
            return False
        return True

    def _check_log(self, err):
#        print err
        status = 'done'
        return status

    def run_task(self, sample, task_name):

        print self.__class__.__name__, sample, task_name
        script = self.state.results[task_name].script[sample]
        out = '{}.o'.format(script)
        err = '{}.e'.format(script)
        subprocess.call('sh {sc} >{out} 2>{err}'.format(sc=script, out=out, err=err), shell=True)
        if 'status' not in self.state.results[task_name]:
            self.state.results[task_name].status = {}
        status = self._check_log(err)
        self.state.results[task_name].status[sample] = status
        if status: 
            self.state.logger.info('%s - %s complete' % (sample, task_name))
        else:
            self.state.logger.warning('%s - %s failed' % (sample, task_name))

    def run(self):
        while True:
            data = self.queue.get()
            if data is _sentinel:
                self.queue.put(_sentinel)
                break

            (sample, task_name, is_last_task) = data

            if self.is_exclusive_task(task_name):
                self.run_task(sample, task_name)
            else:
                self.ne_queue.put(data)
                self.other_queue_steps.add(task_name)

            if is_last_task:
                self.other_queue_steps = set()
            self.queue.task_done()


class NonExclusiveTask(Task):
    def __init__(self, state, queue, ne_queue):
        super(NonExclusiveTask, self).__init__(state, queue, ne_queue)

    def is_exclusive_task(self, task_name):
        for dep_step in self.state.analysisDict[task_name].depend:
            if dep_step in self.other_queue_steps:
                return True

        if 'exclusive_task' not in self.state[task_name]:
            return True

        def astrcmp(x, y):
            return x.lower() == y.lower()

        if astrcmp(self.state[task_name].exclusive_task, 'false') or self.state[task_name].exclusive_task is False:
            return False
        return True

    def run(self):
        while True:
            data = self.ne_queue.get()
            if data is _sentinel:
                self.ne_queue.put(_sentinel)
                break
            (sample, task_name, is_last_task) = data

            if self.is_exclusive_task(task_name):
                time.sleep(1)
                self.queue.put(data)
                self.other_queue_steps.add(task_name)
            else:
                self.run_task(sample, task_name)

            if is_last_task:
                self.other_queue_steps = set()

            self.ne_queue.task_done()


class Scheduler(object):
    def __init__(self, state):
        self.state = state
        self.samples = []
        self.task_lists = []
        self.task_t = None
        self.nx_task_t = None

    def parse_rerun(self, rerun_list_file):
        with open(rerun_list_file, 'r') as f:
            for n, line in enumerate(f):
                (sample, tasks) = line.strip().split('\t')
                task_list = tasks.split(',')
                self.samples.append(sample)
                self.task_lists.append(task_list)

    def start(self):
        non_exclusive_task_queue = Queue()
        task_queue = Queue()
        self.task_t = Task(self.state, task_queue, non_exclusive_task_queue)
        # self.task_t.setDaemon(True)
        self.task_t.start()

        self.nx_task_t = NonExclusiveTask(self.state, task_queue, non_exclusive_task_queue)
        # self.nx_task_t.setDaemon(True)
        self.nx_task_t.start()

        for sample, task_list in zip(self.samples, self.task_lists):
            if not self.task_t.is_exclusive_task(task_list[0]):
               non_exclusive_task_queue.join()
               for task_name in task_list[:-1]:
                   non_exclusive_task_queue.put((sample, task_name, False))
               non_exclusive_task_queue.put((sample, task_list[-1], True))
            else:
               task_queue.join()
               for task_name in task_list[:-1]:
                   task_queue.put((sample, task_name, False))
               task_queue.put((sample, task_list[-1], True))

        time.sleep(2)   # 任务会在两个队列中传递，要确保所有任务均执行完毕
        task_queue.join()
        non_exclusive_task_queue.join()
        task_queue.join()
        non_exclusive_task_queue.join()
        task_queue.put(_sentinel)
        non_exclusive_task_queue.put(_sentinel)
        time.sleep(2)


def main():
    program_name = os.path.basename(sys.argv[0])
    program_license = '''{0}
      Created by huangzhibo on {1}.
      Last updated on {2}.
      Copyright 2017 BGI bigData. All rights reserved.
    USAGE'''.format(" v".join([program_name, __version__]), str(__date__), str(__updated__))

    parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-s", "--state", dest="state", help="state file,[default: %(default)s]", required=True)
    parser.add_argument("-r", "--rerun", dest="rerun", help="rerun file,[default: %(default)s]", required=True)

    if len(sys.argv) == 1:
        parser.print_help()
        exit(1)

    # Process arguments
    args = parser.parse_args()
    if not os.path.exists(args.state):
        printtime('ERROR: (--state: %s) - No such file or directory' % args.state)
        return 1
    if not os.path.exists(args.rerun):
        printtime('ERROR: (--state: %s) - No such file or directory' % args.state)
        return 1

    state = ParseConfig(args.state).parseState()
    if 'bamSort' in state:
        state.bamSort.exclusive_task = 'False'
    if 'bamSort_M' in state:
        state.bamSort_M.exclusive_task = 'False'
    state.init.exclusive_task = 'False'
    if 'bamindex' in state:
        state.bamindex.exclusive_task = 'False'

    logger = Logger(os.path.join(state.scriptsDir,'log'),'1','job_scheduler',False).getlog()
    state.logger = logger
    sched = Scheduler(state)
    sched.parse_rerun(args.rerun)
    sched.start()

    with open(os.path.join(state.stateDir,'success'), 'w') as f:
       f.write('done!')

    return 0


if __name__ == "__main__":
    sys.exit(main())
