# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class bamSort(Workflow):
    """ bamSort """

    INIT = bundle(bamSort=bundle())
    INIT.bamSort.program = "hadoop-bam-x.7.0.jar"
    INIT.bamSort.picard = "picard.x.1.jar"
    INIT.bamSort.index_program = ''
    INIT.bamSort.output_format = 'file'
    INIT.bamSort.reducer_num = 0
    # INIT.bamSort.bamindex = False
    
        
    def run(self, impl, dependList):
        impl.log.info("step: bamSort!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(),script=bundle())
        
        #extend program path
        self.bamSort.program = self.expath('bamSort.program')
        self.bamSort.picard = self.expath('bamSort.picard')
        self.bamSort.index_program = self.expath('bamSort.index_program', False)

        hadoop_parameter = ''
        if self.hadoop.get('queue'):
            hadoop_parameter += '-D mapreduce.job.queuename={} '.format(self.hadoop.queue)
        hadoop_parameter += '-libjars {} '.format(self.bamSort.picard)

        reducer = self.hadoop.reducer_num
        redeuce_per_node = 10
        reducer = int(int(self.hadoop.reducer_num)/redeuce_per_node)
        if self.option.multiSample:
            # if self.hadoop.is_at_TH:
            #     redeuce_per_node = 5
            if redeuce_per_node > len(self.sample):
                redeuce_per_node = len(self.sample)
            reducer = int(int(self.hadoop.reducer_num)/redeuce_per_node)
        if self.bamSort.reducer_num > 0:
            reducer = int(self.bamSort.reducer_num)
            
        #global param
        ParamDict = {
                "PROGRAM": "%s jar %s %s" % (self.hadoop.bin, self.bamSort.program, hadoop_parameter),
                "REDUCERNUM":reducer
            }

        #script template    
        fs_cmd = self.fs_cmd
        cmd = []
        cmd.append("%s ${HDFSTMP}" % fs_cmd.delete)
        cmd.append("allparts=")
        cmd.append("%s ${INPUT}/part* |awk '{print $%d}' > ${BAMLIST}" % (fs_cmd.ls, (not self.hadoop.ishadoop2 and self.hadoop.is_at_TH) and 9 or 8))
        cmd.append('for i in `cat ${BAMLIST}`;do allparts="${allparts} $i";done')
        cmd.append("${PROGRAM} sort -F BAM -o ${OUTDIR} --reducers ${REDUCERNUM} ${HDFSTMP} ${allparts} ")

        if self.option.multiSample:
            outdir= impl.mkdir(self.option.workdir,"alignment",self.option.multiSampleName)
            for sample_name in self.sample:
                bam = os.path.join(outdir,"%s.sorted.bam" % sample_name)
                if self.bamSort.get('index_program'):
                    #cmd.append('if [ ! -e {}.bai ]\nthen'.format(bam))
                    cmd.append('{} index {}'.format(self.bamSort.index_program, bam))
                    #cmd.append('fi')
                result.output[sample_name] = bam
        else:
            if self.bamSort.get('index_program'):
                #cmd.append('if [ ! -e ${OUTPUT}.bai ]\nthen')
                cmd.append('%s index ${OUTPUT} -t 12' % self.bamSort.index_program)
                #cmd.append('fi')
        
        JobParamList = []
        for sampleName in inputInfo:
            hdfs_tmp = os.path.join(self.option.dirHDFS,sampleName,'tmp')
            tmp = impl.mkdir(self.option.workdir,"temp",sampleName)
            scriptsdir = impl.mkdir(self.gaeaScriptsDir,sampleName)
            outdir= impl.mkdir(self.option.workdir,"alignment",sampleName)
            
            if not self.option.multiSample:
                result.output[sampleName] = os.path.join(outdir,"%s.sorted.bam" % sampleName)

            #global param
            JobParamList.append({
                    "SAMPLE" : sampleName,
                    "SCRDIR" : scriptsdir,
                    "INPUT": inputInfo[sampleName],
                    "OUTPUT": result.output[sampleName] if not self.option.multiSample else '',
                    "OUTDIR": 'file://%s' % outdir,
                    "BAMLIST": os.path.join(tmp,"hadoop_bam.list"),
                    "HDFSTMP":hdfs_tmp
                })
            
                    
        #write script
        scriptPath = \
        impl.write_scripts(
                name = 'bamSort',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)
    
        #result
        result.script.update(scriptPath)     
        return result
