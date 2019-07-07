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

        if 'bed_list' in self.file:
            self.merge_vcf.bed_list = self.expath('file.bed_list')

        # extend program path
        self.merge_vcf.program = self.expath('merge_vcf.program')
        self.merge_vcf.bed_list = self.expath('merge_vcf.bed_list')
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
            upload_tmp = os.path.join(self.option.dirHDFS, sampleName, 'vcf_tmp')

            # global param
            JobParamList.append({
                "SAMPLE": sampleName,
                "SCRDIR": scriptsdir,
                "UPLOAD_TMP": upload_tmp,
                "DATALIST": os.path.join(scriptsdir, 'vcf_data.list'),
                "VCF_TMP": inputInfo[sampleName]['vcf'],
                "VCF": result.output[sampleName]
            })
            if self.merge_vcf.uploadvcf:
                vcf_suffix = ".hc.vcf.gz"
                dataParam = []
                with open(self.merge_vcf.bed_list, 'r') as beds:
                    for bed in beds:
                        basename = '{}{}'.format(os.path.splitext(os.path.basename(bed))[0], vcf_suffix)
                        dataParam.append({
                            "KEY": os.path.join(inputInfo[sampleName]['vcf'], basename)
                        })

                impl.write_file(
                    fileName='vcf_data.list',
                    scriptsdir=scriptsdir,
                    commands=["${KEY}"],
                    JobParamList=dataParam)

        cmd = ["source %s/bin/activate" % self.GAEA_HOME,
               'check_hc_part.py -b %s -p ${VCF_TMP} %s' % (self.merge_vcf.bed_list, self.merge_vcf.check_param),
               'if [ $? != 0 ]\nthen',
               '\texit 1',
               'fi'
               ]

        if self.merge_vcf.uploadvcf:
            cmd.extend([
                '%s ${UPLOAD_TMP}' % self.fs_cmd.delete,
                '${PROGRAM} GzUploader -i ${DATALIST} -o ${UPLOAD_TMP} -l',
                'if [ $? != 0 ]\nthen',
                '\texit 1',
                'fi',
                'wait',
                '${PROGRAM} SortVcf ${HADOOPPARAM} -R 400 -p /tmp/partitionFiles/vcfsort/reducer400_partitons.lst '
                '-input ${UPLOAD_TMP} -output file://${VCF}\n',
            ])
        else:
            cmd.extend([
                'rm ${VCF_TMP}/*tbi',
                'wait',
                '${PROGRAM} SortVcf ${HADOOPPARAM} -input file://${VCF_TMP} -output file://${VCF}\n'
            ])
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
