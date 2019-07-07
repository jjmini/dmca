# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class clean(Workflow):
    """ clean """

    INIT = bundle(clean=bundle())
    INIT.clean.program = "/szhwfs1/ST_HEALTH/GENOME_APP/F16ZQSB1SY2582/personalgenome/lib/genome_api_for_gaea.pl"
    INIT.clean.parameter = ''

    def run(self, impl,dependList):
        impl.log.info("step: clean!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(),script=bundle())
        
        #extend program path
        #self.clean.program = self.expath('clean.program')

        #script template    
        fs_cmd = self.fs_cmd
        cmd = []
        cmd.append("source %s/bin/activate" % self.GAEA_HOME)
        cmd.append("check.py -s %s/state.json -n ${SAMPLE} -t %s %s" % (self.stateDir, ','.join(dependList), self.init.check_state_param))
        cmd.append("if [ $? = 0 ];then")
        cmd.append("%s %s/${SAMPLE}" % (fs_cmd.delete, self.option.dirHDFS))
        if self.init.check_state_param:
            cmd.append("${CPVCF}")
        cmd.append("fi")
            
        JobParamList = []
        for sampleName in self.sample:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir,sampleName)
            vcf = ''
            for step in dependList:
                vcf_tmp = self.results[step].output[sampleName]
                if os.path.basename(vcf_tmp).find('vcf') != -1:
                    vcf = vcf_tmp
                    break
            
            #global param
            JobParamList.append({
                    "SAMPLE" : sampleName,
                    "SCRDIR" : scriptsdir,
                    "CPVCF" : "cp %s /ldfssz1/ST_HEALTH/WGS/project/3000members_hg38/vcf/" % vcf if vcf else ''
                })
            
        #write script
        scriptPath = \
        impl.write_scripts(
                name = 'clean',
                commands=cmd,
                JobParamList=JobParamList)
    
        #result
        result.script.update(scriptPath)           
        return result
