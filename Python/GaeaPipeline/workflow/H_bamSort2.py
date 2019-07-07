# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class bamSort2(Workflow):
    """ bamSort """

    INIT = bundle(bamSort=bundle())
    INIT.bamSort.program = "hadoop-bam.jar"
    INIT.bamSort.picard = "picard.x.1.jar"
    
        
    def run(self, impl, dependList):
        impl.log.info("step: bamSort2!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(),script=bundle())
        
        outdir = bundle()

        self.bamSort.program = self.expath('bamSort.program')
        self.bamSort.picard = self.expath('bamSort.picard')

        hadoop_parameter = ''
        if self.hadoop.get('queue'):
            hadoop_parameter += '-D mapreduce.job.queuename={} '.format(self.hadoop.queue)
        hadoop_parameter += '-libjars {} '.format(self.bamSort.picard)


        reducer = self.hadoop.reducer_num
        if self.option.multiSample:
            redeuce_per_node = 10
            if self.hadoop.is_at_TH:
                redeuce_per_node = 5
            if redeuce_per_node > len(self.sample):
                redeuce_per_node = len(self.sample)
            reducer = int(int(self.hadoop.reducer_num)/redeuce_per_node)
            
        
        for sampleName in inputInfo:
            hdfs_tmp = os.path.join(self.option.dirHDFS,sampleName,'tmp')
            tmp = impl.mkdir(self.option.workdir,"temp",sampleName)
            scriptsdir = impl.mkdir(self.gaeaScriptsDir,sampleName)
            outdir= impl.mkdir(self.option.workdir,"alignment",sampleName)
            
            if self.option.multiSample:
                for sample_name in self.sample:
                    result.output[sample_name] = os.path.join(outdir,"%s.sorted.bam" % sample_name)
            else:
                result.output[sampleName] = os.path.join(outdir,"%s.sorted.bam" % sampleName)
            
            #global param
            ParamDict = {
                    "PROGRAM": "%s jar %s %s" % (self.hadoop.bin, self.bamSort.program, hadoop_parameter),
                    "INPUT": inputInfo[sampleName],
                    "OUTDIR": 'file://%s' % outdir,
                    "BAMLIST": os.path.join(tmp,"hadoop_bam.list"),
                    "HDFSTMP":hdfs_tmp,
                    "REDUCERNUM":reducer
                }
            
            #script template    
            fs_cmd = self.fs_cmd
            cmd = []
            cmd.append("allparts=")
            cmd.append("%s ${INPUT}/part* |awk '{print $%d}' > ${BAMLIST}" % (fs_cmd.ls, self.hadoop.is_at_TH and 9 or 8))
            cmd.append('for i in `cat ${BAMLIST}`;do allparts="${allparts} $i";done')
            cmd.append("${PROGRAM} sort -F BAM -o ${OUTDIR} --reducers ${REDUCERNUM} ${HDFSTMP} ${allparts}")
                    
            #write script
            scriptPath = \
            impl.write_shell(
                    name = 'bamSort2',
                    scriptsdir = scriptsdir,
                    commands=cmd,
                    paramDict=ParamDict)
            
            #result
            result.script[sampleName] = scriptPath
        
        return result
