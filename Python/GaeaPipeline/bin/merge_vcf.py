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
__date__ = '2017-07-06'
__updated__ = '2017-07-06'

gvcf_suffix = '.g.vcf.gz'
vcf_suffix = '.hc.vcf.gz'

def result_check(data, size_threshold=3):
    if not os.path.exists(data) or os.path.getsize(data)/1024 < size_threshold:
        return False
    return True


def check_part_vcf(bed_prefix, part_vcf_dir):
    status = True
    for p in bed_prefix:
        #tmp_gvcf = os.path.join(part_vcf_dir, p+gvcf_suffix)
        #gvcf_status = result_check(tmp_gvcf)
        tmp_vcf = os.path.join(part_vcf_dir, p+vcf_suffix)
        vcf_status = result_check(tmp_vcf)
        if not vcf_status:
            status = False
            break
    return status

def merge_vcf(bed_prefix, part_vcf_dir, out, gvcf=False):
    part_vcf_list = os.path.join(part_vcf_dir, 'part_vcf.list')
    suffix = vcf_suffix
    if gvcf:
        suffix = gvcf_suffix
        part_vcf_list = os.path.join(part_vcf_dir, 'part_gvcf.list')
    with open(part_vcf_list, 'w') as wf:
        for p in bed_prefix:
            part_vcf = os.path.join(part_vcf_dir, p+suffix)
            wf.write(part_vcf)
            wf.write('\n')

    cmd = '/hwfssz1/BIGDATA_COMPUTING/software/bin/bcftools concat --threads 24 -O z -a -f {} -o {}'.format(part_vcf_list, out)
    printtime('INFO: {}'.format(cmd))
    subprocess.call(cmd, shell=True)

def main():
    program_name = os.path.basename(sys.argv[0])
    program_license = '''{0}
      Created by huangzhibo on {1}.
      Last updated on {2}.
      Copyright 2017 BGI bigData. All rights reserved.
    USAGE'''.format(" v".join([program_name, __version__]), str(__date__), str(__updated__))

    parser = ArgumentParser(description=program_license, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-b", "--bedlist", help="sample_name,[default: %(default)s]", required=True)
    parser.add_argument("-o", "--output", help="output file path,[default: %(default)s]", required=True)
    parser.add_argument("-g", "--gvcf_out", help="gvcf output file path,[default: %(default)s]", )
    parser.add_argument("-p", "--part_vcf_dir", help="part_vcf_dir,[default: %(default)s]", required=True)

    if len(sys.argv) == 1:
        parser.print_help()
        exit(1)
    
    # Process arguments
    args = parser.parse_args()
    if not os.path.exists(args.bedlist):
        printtime('ERROR: (--bedlist: %s) - No such file or directory' % args.state)
        return 1

    if not os.path.exists(args.part_vcf_dir):
        printtime('ERROR: (--part_vcf_dir: %s) - No such file or directory' % args.state)
        return 1

    bed_prefix = []
    with open(args.bedlist, 'r') as beds:
        for bed in beds:
            bed_prefix.append(os.path.splitext(os.path.basename(bed))[0])
 
    i = 30
    while i:
        status = check_part_vcf(bed_prefix, args.part_vcf_dir) 
        if status:
            break
        time.sleep(5)
        i -= 1 
   
    if status:
        print "part_vcf_dir is good!" 
        merge_vcf(bed_prefix, args.part_vcf_dir, args.output)
        if args.gvcf_out:
             merge_vcf(bed_prefix, args.part_vcf_dir, args.gvcf_out, True)
    else:
        print "part_vcf_dir is bad!"

    return 0


if __name__ == "__main__":
    sys.exit(main())
