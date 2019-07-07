# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class haplotypeCaller(Workflow):
    """ haplotypeCaller """

    INIT = bundle(haplotypeCaller=bundle())
    INIT.haplotypeCaller.program = "GenomeAnalysisTK.jar"
    INIT.haplotypeCaller.bcftools = "bcftools"
    INIT.haplotypeCaller.parameter = ""
    INIT.haplotypeCaller.bed_list = ""
    INIT.haplotypeCaller.mapper_mem = "20480"

    def run(self, impl, dependList):
        impl.log.info("step: haplotypeCaller!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())
        bed_dir = impl.mkdir(self.option.workdir, "doc", "bed")

        if 'bed_list' in self.file:
            self.haplotypeCaller.bed_list = self.expath('file.bed_list')

        # extend program path
        self.haplotypeCaller.program = self.expath('haplotypeCaller.program')
        self.haplotypeCaller.bed_list = self.expath('haplotypeCaller.bed_list')
        self.haplotypeCaller.bcftools = self.expath('haplotypeCaller.bcftools')

        bed_num = 0
        with open(self.haplotypeCaller.bed_list, 'r') as bed:
            bed_num = len(bed.readlines())
        # global param
        MapperParamDict = self.file.copy()
        hadoop_parameter = ' -D mapred.job.name="haplotypeCaller" '
        if self.hadoop.get('queue'):
            hadoop_parameter += ' -D mapreduce.job.queuename={} '.format(self.hadoop.queue)
        hadoop_parameter += ' -D mapred.map.tasks={} '.format(bed_num)
        hadoop_parameter += ' -D mapred.reduce.tasks=0 '
        hadoop_parameter += ' -D mapreduce.map.memory.mb=%s ' % self.haplotypeCaller.mapper_mem
        hadoop_parameter += ' -D mapreduce.map.cpu.vcores=3 '
        hadoop_parameter += ' -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat '
        ParamDict = {
            "PROGRAM": "%s jar %s" % (self.hadoop.bin, self.hadoop.streamingJar),
            "INPUT": 'file://' + self.haplotypeCaller.bed_list,
            "HADOOPPARAM": hadoop_parameter
        }

        impl.paramRectify(self.alignment.parameter, '-I', True)

        mapper = ["#!/bin/sh",
                  "export PATH=/hwfssz1/BIGDATA_COMPUTING/software/bin:$PATH",
                  "while read LINE",
                  "do",
                  "\tif [[ -n $LINE ]];then",
                  "\t\techo $LINE",
                  "\t\tbed=`echo $LINE| awk '{print $2}'`",
                  "\t\tbase=`basename $bed .bed`",
                  '\t\tjava -jar %s -T HaplotypeCaller %s -I ${BAM} -L $bed -R %s -o ${VCF_TMP}/$base.hc.vcf.gz' %
                  (self.haplotypeCaller.program, self.haplotypeCaller.parameter, self.ref.normal.ref),
                  "\telse",
                  '\t\techo "Empty LANE"',
                  "\tfi",
                  "done"
                  ]

        JobParamList = []
        for sampleName in inputInfo:
            hdfs_tmp = os.path.join(self.option.dirHDFS, sampleName, 'hc_tmp')
            tmp = impl.mkdir(self.option.workdir, "temp", sampleName, 'hc')
            os.chmod(tmp, 0777)
            scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
            outputPath = impl.mkdir(self.option.workdir, "variation", 'haplotypeCaller', sampleName)
            result.output[sampleName] = os.path.join(outputPath, "{}.hc.vcf.gz".format(sampleName))
            gvcf = os.path.join(outputPath, "{}.g.vcf.gz".format(sampleName))

            MapperParamDict.update({"BAM":inputInfo[sampleName], "VCF_TMP":tmp})
            impl.write_file(
                fileName='hc_mapper.sh',
                scriptsdir=scriptsdir,
                commands=mapper,
                paramDict=MapperParamDict)

            #global param
            JobParamList.append({
                    "SAMPLE" : sampleName,
                    "SCRDIR" : scriptsdir,
                    "MAPPER": os.path.join(scriptsdir, "hc_mapper.sh"),
                    "OUTPUT": result.output[sampleName],
                    "GVCF": gvcf,
                    "VCF_TMP": tmp,
                    "OUTTEMP": hdfs_tmp
                })
   
        cmd = ["source %s/bin/activate" % self.GAEA_HOME,
               '%s ${OUTTEMP}' % self.fs_cmd.delete,
               '${PROGRAM} ${HADOOPPARAM} -input ${INPUT} -output ${OUTTEMP} -mapper "sh ${MAPPER}"',
               'merge_vcf.py -b %s -p ${VCF_TMP} -o ${OUTPUT}' % self.haplotypeCaller.bed_list]

        #write script
        scriptPath = \
        impl.write_scripts(
                name = 'haplotypeCaller',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)

        # result
        result.script.update(scriptPath)
        return result
