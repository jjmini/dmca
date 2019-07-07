from gaeautils import bundle, Logger
import glob
import os


logger = Logger('log.txt','2',"mode6",True).getlog()
def parse_sample(sampleList):
    total_number = 0
    sampleInfo = bundle()
    with open(sampleList,'r') as f:
        for line in f:
            fq1s = []
            line = line.strip()
            field = line.split()
            rg_LB = field[2]
            rg_PU = field[3]
            sampleName = field[1]

            if field[3].find(',') != -1:
                fq1s.append(field[3].split(',')[0])
                rg_LB = field[1]
                rg_PU = field[2]
                sampleName = field[0]
            else:
                fq_dir = field[-1].strip()
                fq1s = glob.glob("%s/*1.fq.gz" % fq_dir)
                if not fq1s:
                    fq1s = glob.glob("%s/*/*1.fq.gz" % fq_dir)

            if len(fq1s) == 0 or not os.path.exists(fq1s[0]):
                logger.error("fq1 under %s don't exists." % sampleName)
                exit(3)

            for fq1 in fq1s:
                total_number += 1
                if not sampleInfo.has_key(sampleName):
                    sampleInfo[sampleName] = bundle()
                    sample_lane_counter = 0
                else:
                    sample_lane_counter = len(sampleInfo[sampleName])
                # fq_name = os.path.basename(fq1)
                # fq_dir = os.path.abspath(os.path.dirname(fq1))

                # slideID_laneID_barcode
                # CL100035764_L02_33_1.fq.gz
                # tmp = fq_name.split("_")
                # rg_PU = tmp[0] + "_" + tmp[1] + "_" + tmp[2]

                rg_ID = "{}_{}".format(sampleName, sample_lane_counter)
                rg = "@RG\\tID:%s\\tPL:COMPLETE\\tPU:%s\\tLB:%s\\tSM:%s\\tCN:BGI" % (rg_ID, rg_PU, rg_LB, sampleName)
                fq_lib_name = rg_ID


                dataTag = 'data'+str(sample_lane_counter)
                if not sampleInfo[sampleName].has_key(dataTag):
                    sampleInfo[sampleName][dataTag] = bundle()

                sampleInfo[sampleName][dataTag]['fq1'] = fq1

                #find fq2
                fq2 = fq1
                fq2 = fq2.replace("1.fq.gz", "2.fq.gz")
                if os.path.exists(fq2):
                    sampleInfo[sampleName][dataTag]['fq2'] = fq2
                else:
                    logger.warning("%s of line: %d is SE data!" % (sampleName,total_number))

                sampleInfo[sampleName][dataTag]['rg'] = rg
                sampleInfo[sampleName][dataTag]['libname'] = fq_lib_name
                sampleInfo[sampleName][dataTag]['gender'] = 'male'
            
    return sampleInfo
