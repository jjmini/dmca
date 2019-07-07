# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow

__updated__ = '2018-05-31'


class merge_vcf(Workflow):
    """ merge_vcf """

    INIT = bundle(merge_vcf=bundle())
    INIT.merge_vcf.program = "gaeatools.jar"
    INIT.merge_vcf.bcftools = ""
    INIT.merge_vcf.bcftools_param = "-t"
    INIT.merge_vcf.parameter = ""
    INIT.merge_vcf.uploadvcf = False
    INIT.merge_vcf.check_param = ""
    INIT.merge_vcf.bed_list = ""

    def run(self, impl, dependList):
        impl.log.info("step: merge_vcf!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())

        # extend program path
        self.merge_vcf.program = self.expath('merge_vcf.program')
        self.merge_vcf.bcftools = self.expath('merge_vcf.bcftools', False)

        # global param
        hadoop_parameter = ''
        if self.hadoop.get('queue'):
            hadoop_parameter += ' -D mapreduce.job.queuename={} '.format(self.hadoop.queue)

        ParamDict = {
            "PROGRAM": "%s jar %s" % (self.hadoop.bin, self.merge_vcf.program),
            "HADOOPPARAM": hadoop_parameter
        }

        JobParamList = []
        for sampleName in inputInfo:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
            outputPath = impl.mkdir(self.option.workdir, "variation", sampleName)
            result.output[sampleName] = os.path.join(outputPath, "{}.hc.vcf.gz".format(sampleName))

            # global param
            JobParamList.append({
                "SAMPLE": sampleName,
                "SCRDIR": scriptsdir,
                "INPUT": inputInfo[sampleName],
                "VCF": result.output[sampleName]
            })

        cmd = ["%s ${INPUT}/_*" % self.fs_cmd.delete,
               '${PROGRAM} SortVcf ${HADOOPPARAM} -input ${INPUT} -output file://${VCF}\n']

        if self.merge_vcf.bcftools:
            cmd.append("%s index %s ${VCF}" % (self.merge_vcf.bcftools, self.merge_vcf.bcftools_param))

        # write script
        scriptPath = \
            impl.write_scripts(
                name='merge_vcf',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)

        # result
        result.script.update(scriptPath)
        return result
