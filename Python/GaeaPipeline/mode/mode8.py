from gaeautils import Logger
from gaeautils.bundle import bundle
import glob
import os
import json

logger = Logger('log.txt','2',"mode8",True).getlog()
def parse_sample(sampleList):
    sample_lane_counter = 0
    total_number = 0

    with open(sampleList,'r') as sampleFile:
        sampleInfo = bundle()
        gender = 'male'
        thetype = ''
        pool = ''
        for line in sampleFile:
            line = line.strip()
            #sample_name, gender, family, type, pool, fq1, fq2, insert_size, fq1s
            field = line.split()
            field_num = len(field)
            sample_name = field[0]
            family = field[0]
            fq1 = field[1]
            fq2 = field[2]

            tmp = os.path.basename(fq1).split("_")
            rg_ID = tmp[0]
            rg_PU = total_number
            rg_LB = total_number
            rg = "@RG\\tID:%s\\tPL:illumina\\tPU:%s\\tLB:%s\\tSM:%s\\tCN:BGI" % (rg_ID,rg_PU,rg_LB,sample_name)
            fq_lib_name = rg_ID
            total_number += 1

            if sample_name not in sampleInfo:
                sampleInfo[sample_name] = bundle()
                sample_lane_counter = 0
            else:
                sample_lane_counter = len(sampleInfo[sample_name])

            dataTag = 'data'+str(sample_lane_counter)
            if dataTag not in sampleInfo[sample_name]:
                sampleInfo[sample_name][dataTag] = bundle()

            sampleInfo[sample_name][dataTag]['fq1'] = fq1
            sampleInfo[sample_name][dataTag]['fq2'] = fq2
            sampleInfo[sample_name][dataTag]['adp1'] = 'null'
            sampleInfo[sample_name][dataTag]['adp2'] = 'null'
            sampleInfo[sample_name][dataTag]['gender'] = gender
            sampleInfo[sample_name][dataTag]['family'] = family
            sampleInfo[sample_name][dataTag]['type'] = thetype
            sampleInfo[sample_name][dataTag]['pool'] = pool
            sampleInfo[sample_name][dataTag]['rg'] = rg
            sampleInfo[sample_name][dataTag]['libname'] = fq_lib_name

    return sampleInfo
