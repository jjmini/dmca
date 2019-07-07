# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow

__updated__ = '2018-05-31'


class merge_gvcf(Workflow):
    """ merge_gvcf """

    INIT = bundle(merge_gvcf=bundle())
    INIT.merge_gvcf.program = "gaeatools.jar"
    INIT.merge_gvcf.bcftools = ""
    INIT.merge_gvcf.bcftools_param = "-t"
    INIT.merge_gvcf.parameter = ""
    INIT.merge_gvcf.check_param = ""
    INIT.merge_gvcf.bed_list = ""

    def run(self, impl, dependList):
        impl.log.info("step: merge_gvcf!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())

        if 'bed_list' in self.file:
            self.merge_gvcf.bed_list = self.expath('file.bed_list')

        # extend program path
        self.merge_gvcf.program = self.expath('merge_gvcf.program')
        self.merge_gvcf.bed_list = self.expath('merge_gvcf.bed_list')
        self.merge_gvcf.bcftools = self.expath('merge_gvcf.bcftools', False)

        # global param
        hadoop_parameter = ''
        if self.hadoop.get('queue'):
            hadoop_parameter += ' -D mapreduce.job.queuename={} '.format(self.hadoop.queue)

        ParamDict = {
            "PROGRAM": "%s jar %s" % (self.hadoop.bin, self.merge_gvcf.program),
            "HADOOPPARAM": hadoop_parameter
        }

        JobParamList = []
        for sampleName in inputInfo:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
            outputPath = impl.mkdir(self.option.workdir, "variation", sampleName)
            result.output[sampleName] = os.path.join(outputPath, "{}.g.vcf.gz".format(sampleName))
            upload_tmp = os.path.join(self.option.dirHDFS, sampleName, 'gvcf_tmp')

            # global param
            JobParamList.append({
                "SAMPLE": sampleName,
                "SCRDIR": scriptsdir,
                "UPLOAD_TMP": upload_tmp,
                "DATALIST": os.path.join(scriptsdir, 'gvcf_data.list'),
                "GVCF_TMP": inputInfo[sampleName]['gvcf'],
                "GVCF": result.output[sampleName]
            })

            vcf_suffix = ".g.vcf.gz"
            dataParam = []
            with open(self.merge_gvcf.bed_list, 'r') as beds:
                for bed in beds:
                    basename = '{}{}'.format(os.path.splitext(os.path.basename(bed))[0], vcf_suffix)
                    dataParam.append({
                        "KEY": os.path.join(inputInfo[sampleName]['gvcf'], basename)
                    })

                impl.write_file(
                    fileName='gvcf_data.list',
                    scriptsdir=scriptsdir,
                    commands=["${KEY}"],
                    JobParamList=dataParam)

        cmd = ["source %s/bin/activate" % self.GAEA_HOME,
               'check_hc_part.py -b %s -p ${GVCF_TMP} %s -s .g.vcf.gz' % (self.merge_gvcf.bed_list,
                                                                          self.merge_gvcf.check_param),
               'if [ $? != 0 ]\nthen',
               '\texit 1',
               'fi',
               '%s ${UPLOAD_TMP}' % self.fs_cmd.delete,
               '${PROGRAM} GzUploader -i ${DATALIST} -o ${UPLOAD_TMP} -l',
               'if [ $? != 0 ]\nthen',
               '\texit 1',
               'fi',
               'wait',
               '${PROGRAM} SortVcf ${HADOOPPARAM} -R 400 -p /tmp/partitionFiles/vcfsort/reducer400_partitons.lst '
               '-input ${UPLOAD_TMP} -output file://${GVCF}\n',
               ]

        if self.merge_gvcf.bcftools:
            cmd.append("%s index %s ${GVCF}" % (self.merge_gvcf.bcftools, self.merge_gvcf.bcftools_param))

        # write script
        scriptPath = \
            impl.write_scripts(
                name='merge_gvcf',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)

        # result
        result.script.update(scriptPath)
        return result
