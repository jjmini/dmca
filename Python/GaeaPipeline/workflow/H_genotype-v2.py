# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class genotype(Workflow):
    """ genotype """

    INIT = bundle(genotype=bundle())
    INIT.genotype.program = "gaea-1.0.0.jar"
    INIT.genotype.parameter = "-glm BOTH  -standCallConf 30.0 -standEmitConf 10.0 -S"

    def run(self, impl, dependList):
        impl.log.info("step: genotype!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(),script=bundle())
        hadoop_parameter = ''
        if self.hadoop.get('queue'):
            hadoop_parameter += ' -D mapreduce.job.queuename={} '.format(self.hadoop.queue)
        
        #extend program path
        self.genotype.program = self.expath('genotype.program')
        
        if not self.option.multiSample:
            if self.genotype.parameter.find('-S') != -1:
                impl.log.warning("Pipeline is in single sample mode, disable -S. (deleted)")
                self.genotype.parameter = self.genotype.parameter.replace('-S','')
            if self.genotype.parameter.find('--single_sample_mode') != -1:
                impl.log.warning("Pipeline is in single sample mode, disable --single_sample_mode. (deleted)")
                self.genotype.parameter = self.genotype.parameter.replace('--single_sample_mode','')
                
        if self.file.get("regionVariation"):
            self.genotype.parameter += " -b file://%s " % self.file.regionVariation
        elif self.file.get("region"):
            self.genotype.parameter += " -b file://%s " % self.file.region
            
        #global param
        ParamDict = self.file.copy()
        ParamDict.update({
                "PROGRAM": "%s jar %s Genotyper %s" % (self.hadoop.bin, self.genotype.program, hadoop_parameter),
                "REF": "file://%s" % self.ref.normal.gaeaIndex,
                "REDUCERNUM":self.hadoop.reducer_num
            })
        
        #script template    
        fs_cmd = self.fs_cmd
        cmd = []
        cmd.append("%s ${INPUT}/_*" % fs_cmd.delete )
        cmd.append("%s ${OUTDIR}" % fs_cmd.delete )
        cmd.append("${PROGRAM} -i ${INPUT} -o ${OUTDIR} -r ${REF} -R ${REDUCERNUM} %s" %self.genotype.parameter )
        
        JobParamList = []
        for sampleName in inputInfo:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir,sampleName)
            hdfs_outputPath = os.path.join(self.option.dirHDFS,sampleName,'genotype_output')
            result.output[sampleName] = hdfs_outputPath
            
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
                name = 'genotype',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)
    
        #result
        result.script.update(scriptPath)           
        return result
                                
