# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class rmdup(Workflow):
    """ rmdup """

    INIT = bundle(rmdup=bundle())
    INIT.rmdup.program = "gaea-1.0.0.jar"
    INIT.rmdup.parameter_SE = ' -S '
    INIT.rmdup.parameter = ''

    def run(self, impl,dependList):
        impl.log.info("step: rmdup!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(),script=bundle())
        
        #extend program path
        self.rmdup.program = self.expath('rmdup.program')
        
        if self.init.get('isSE'):
            self.rmdup.parameter = self.rmdup.parameter_SE
        hadoop_parameter = ''
        if self.hadoop.get('queue'):
            hadoop_parameter += '-D mapreduce.job.queuename={}'.format(self.hadoop.queue)
            
        #script template    
        fs_cmd = self.fs_cmd
        cmd = []
        cmd.append("%s ${OUTDIR}/" % fs_cmd.delete )
        cmd.append("%s ${INPUT}/*/_SUCCESS ${INPUT}/*/_logs" % fs_cmd.delete )
        cmd.append("${PROGRAM} %s -i ${INPUT} -o ${OUTDIR} -R ${REDUCERNUM} ${PARAM}" % hadoop_parameter)
            
        for sampleName in inputInfo:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir,sampleName)
            hdfs_outputPath = os.path.join(self.option.dirHDFS,sampleName,'rmdup_output')
            
            #global param
            ParamDict = {
                    "PROGRAM": "%s jar %s MarkDuplicate" % (self.hadoop.bin, self.rmdup.program),
                    "INPUT": inputInfo[sampleName],
                    "OUTDIR": hdfs_outputPath,
                    "REDUCERNUM":self.hadoop.reducer_num,
                    "PARAM":self.rmdup.parameter
                }
            
            #write script
            scriptPath = \
            impl.write_shell(
                    name = 'rmdup',
                    scriptsdir = scriptsdir,
                    commands=cmd,
                    paramDict=ParamDict)
            
            #result
            result.output[sampleName] = os.path.join(hdfs_outputPath,'Mark')
            result.script[sampleName] = scriptPath
        return result
                                
