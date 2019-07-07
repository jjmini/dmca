# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class hc(Workflow):
    """ hc """

    INIT = bundle(hc=bundle())
    INIT.hc.program = "gaea-1.0.0.jar"
    INIT.hc.parameter = "-w 1000 -x 300 -d 110 -e 50"

    def run(self, impl, dependList):
        impl.log.info("step: hc!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(),script=bundle())

        hadoop_parameter = ''
        if self.hadoop.get('queue'):
            hadoop_parameter += ' -D mapreduce.job.queuename={} '.format(self.hadoop.queue)
        
        #extend program path
        self.hc.program = self.expath('hc.program')
        
        if self.file.get("regionVariation"):
            self.hc.parameter += " -R file://%s " % self.file.regionVariation
        elif self.file.get("region"):
            self.hc.parameter += " -R file://%s " % self.file.region
            
        #global param
        ParamDict = self.file.copy()
        ParamDict.update({
                "PROGRAM": "%s jar %s HaplotypeCaller %s" % (self.hadoop.bin, self.hc.program, hadoop_parameter),
                "REF": "file://%s" % self.ref.normal.gaeaIndex,
                "REDUCERNUM":self.hadoop.reducer_num
            })
        
        #script template    
        fs_cmd = self.fs_cmd
        cmd = []
        cmd.append("%s ${INPUT}/_*" % fs_cmd.delete )
        cmd.append("%s ${OUTDIR}" % fs_cmd.delete )
        cmd.append("${PROGRAM} -i ${INPUT} -o ${OUTDIR} -r ${REF} -n ${REDUCERNUM} %s" % self.hc.parameter )
        
        JobParamList = []
        for sampleName in inputInfo:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir,sampleName)
            hdfs_outputPath = os.path.join(self.option.dirHDFS,sampleName,'hc_output')
            result.output[sampleName] = os.path.join(hdfs_outputPath, 'vcf')

            #global param
            JobParamList.append({
                    "SAMPLE" : sampleName,
                    "SCRDIR" : scriptsdir,
                    "INPUT": inputInfo[sampleName],
                    "OUTDIR": hdfs_outputPath
                })
            
    
        #write script
        scriptPath = \
        impl.write_scripts(
                name = 'hc',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)
    
        #result
        result.script.update(scriptPath)           
        return result
                                
