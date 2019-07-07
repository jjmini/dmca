from gaeautils import bundle, Logger
import glob
import os


logger = Logger('log.txt','2',"mode6",True).getlog()
def parse_sample(sampleList):
    
    total_number = 0
    with open(sampleList,'r') as f:
        sampleInfo = bundle()
        for line in f:
            total_number += 1
            line = line.strip()
            field = line.split()
            sampleName = field[0]
            rg_LB = field[2]
            rg_ID = "{}-{}".format(sampleName, field[3])
            fq_dir = field[-1].strip()
            fq1s = glob.glob("%s/*1.fq.gz" % fq_dir)
            fq1 = ''
            fq2 = ''
            if fq1s:
                fq1 = fq1s[0].strip()
            else:
                logger.error("fq1 under %s don't exists." % sampleName)
                exit(3)
        
            rg = "@RG\\tID:%s\\tPL:COMPLETE\\tLB:%s\\tSM:%s\\tCN:BGI" % (rg_ID,rg_LB,sampleName)
            fq_lib_name = rg_ID
            
            if not sampleInfo.has_key(sampleName):
                    sampleInfo[sampleName] = bundle()
                    sample_lane_counter = 0
            else:
                sample_lane_counter = len(sampleInfo[sampleName])
                    
            dataTag = 'data'+str(sample_lane_counter)
            if not sampleInfo[sampleName].has_key(dataTag):
                sampleInfo[sampleName][dataTag] = bundle()
                
            #find adp1
            sampleInfo[sampleName][dataTag]['fq1'] = fq1
                
            #find fq2 and adp2
            fq2 = fq1
            fq2 = fq2.replace("1.fq.gz", "2.fq.gz")
            if os.path.exists(fq2):
                sampleInfo[sampleName][dataTag]['fq2'] = fq2
            else:
                logger.warning("%s of line: %d is SE data!" % (sampleName,total_number))
                
            sampleInfo[sampleName][dataTag]['rg'] = rg
            #sampleInfo[sampleName][dataTag]['libname'] = fq_lib_name
            sampleInfo[sampleName][dataTag]['gender'] = 'male'
            
    return sampleInfo
