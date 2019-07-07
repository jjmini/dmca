#!/usr/bin/env python
# encoding: utf-8
import os
import subprocess
import sys
import glob
from argparse import ArgumentParser
from argparse import RawDescriptionHelpFormatter

import time

from gaeautils import printtime
from gaeautils.bundle import bundle
from gaeautils.parseConfig import ParseConfig

__all__ = []
__version__ = '0.1'
__date__ = '2017-05-20'
__updated__ = '2017-05-20'


def result_check(sampleName, state, steptag, size_threshold=10):
    status = True
    if state.option.multi:
        for sample in state.results[steptag].output:
            data = state.results[steptag].output[sample]
            if not os.path.exists(data):
                status = False
            elif os.path.getsize(data) / 1024 < size_threshold:
                status = False
    else:
        data = state.results[steptag].output[sampleName]
        if not os.path.exists(data):
            status = False
        elif os.path.getsize(data) / 1024 < size_threshold:
            status = False
    return status


def update_db_state(sample, status_tag, message=None):
    db_update = "/hwfssz1/ST_HEALTH/WGS/F16ZQSB1SY2582/personalgenome/lib/genome_api_for_gaea.pl"
    message_param = ''
    if message:
        message_param = '-message {}'.format(message)
    cmd = 'ssh 192.168.60.11 /hwfssz1/ST_MCHRI/CLINIC/SOFTWARES/bin/perl {} -sample_no {} -status {} {}'.format(
        db_update, sample, status_tag, message_param)
    printtime('INFO: {}'.format(cmd))
    subprocess.call(cmd, shell=True)


def update_local_state(sample, state_dir, status_tag, message=None):
    print status_tag
    state_file_list = glob.glob(os.path.join(state_dir, '{}.*'.format(sample)))
    for f in state_file_list:
        os.remove(f)
    state_file = os.path.join(state_dir, '{}.{}'.format(sample, status_tag))
    print state_file
    with open(state_file, 'w') as fw:
        if message:
            fw.write(message)
        else:
            fw.write(status_tag)


def main():
    program_name = os.path.basename(sys.argv[0])
    program_license = '''{0}
      Created by huangzhibo on {1}.
      Last updated on {2}.
      Copyright 2017 BGI bigData. All rights reserved.
    USAGE'''.format(" v".join([program_name, __version__]), str(__date__), str(__updated__))

    parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-s", "--state", dest="state", help="state file,[default: %(default)s]", required=True)
    parser.add_argument("-n", "--sample_name", dest="sample_name", help="sample_name,[default: %(default)s]",
                        required=True)
    parser.add_argument("-t", "--step", dest="step", help="step,[default: %(default)s]")
    parser.add_argument("-d", "--db_state", action="store_true", help="update db state,[default: %(default)s]")

    if len(sys.argv) == 1:
        parser.print_help()
        exit(1)

    # Process arguments
    args = parser.parse_args()
    if not os.path.exists(args.state):
        printtime('ERROR: (--state: %s) - No such file or directory' % args.state)
        return 1

    state = ParseConfig(args.state).parseState()
    state_dir = os.path.join(state.stateDir, 'sample_state')
    if not os.path.exists(state_dir):
        os.mkdir(state_dir)

    status_tag = 'done'

    if args.step:
        steps = args.step.split(',')

        for step in steps:
            status = result_check(args.sample_name, state, step)
            if not status:
                printtime('ERROR: (step: %s) - No such file or directory' % step)
                if args.db_state:
                    update_local_state(args.sample_name, state_dir, 'error', 'No results for step: {}'.format(step))
                    update_db_state(args.sample_name, 'error', 'No results for step: {}'.format(step))
                else:
                    update_local_state(args.sample_name, state_dir, 'error', 'No results for step: {}'.format(step))
                return 1
            else:
                printtime('INFO:  (step: %s) - completed' % step)
    else:
        status_tag = 'running'
    print status_tag
    if args.db_state:
        update_local_state(args.sample_name, state_dir, status_tag)
        update_db_state(args.sample_name, status_tag)
    else:
        update_local_state(args.sample_name, state_dir, status_tag)
    return 0


if __name__ == "__main__":
    sys.exit(main())
