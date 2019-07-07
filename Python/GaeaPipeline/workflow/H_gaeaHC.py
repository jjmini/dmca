# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class gaeaHC(Workflow):
    """ gaeaHC """

    INIT = bundle(gaeaHC=bundle())
    INIT.gaeaHC.program = "GenomeAnalysisTK.jar"
    INIT.gaeaHC.parameter = ""
    INIT.gaeaHC.GenotypeGVCFs_param = ""
    INIT.gaeaHC.bed_list = ""
    INIT.gaeaHC.mapper_mem = "30720"

    def run(self, impl, dependList):
        impl.log.info("step: gaeaHC!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())
    #    bed_dir = impl.mkdir(self.option.workdir, "temp", "bed")

        if 'bed_list' in self.file:
            self.gaeaHC.bed_list = self.expath('file.bed_list')

        # extend program path
        self.gaeaHC.program = self.expath('gaeaHC.program')
        self.gaeaHC.bed_list = self.expath('gaeaHC.bed_list')

        bed_num = 0
        with open(self.gaeaHC.bed_list, 'r') as bed:
            bed_num = len(bed.readlines())
        # global param
        MapperParamDict = self.file.copy()
        hadoop_parameter = ' -D mapreduce.job.name="gaeaHC" '
        if self.hadoop.get('queue'):
            hadoop_parameter += ' -D mapreduce.job.queuename={} '.format(self.hadoop.queue)
        hadoop_parameter += ' -D mapreduce.job.maps={} '.format(bed_num)
        hadoop_parameter += ' -D mapreduce.job.reduces=0 '
        hadoop_parameter += ' -D mapreduce.map.memory.mb=%s ' % self.gaeaHC.mapper_mem
        hadoop_parameter += ' -D mapreduce.map.cpu.vcores=6 '
        hadoop_parameter += ' -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat '
        ParamDict = {
            "PROGRAM": "%s jar %s" % (self.hadoop.bin, self.hadoop.streamingJar),
            "INPUT": 'file://' + self.gaeaHC.bed_list,
            "HADOOPPARAM": hadoop_parameter
        }

        mapper = ["#!/bin/sh",
                  "export PATH=/hwfssz1/BIGDATA_COMPUTING/software/bin:$PATH",
                  "while read LINE",
                  "do",
                  "\tif [[ -n $LINE ]];then",
                  "\t\techo $LINE",
                  "\t\tbed=`echo $LINE| awk '{print $2}'`",
                  "\t\tbase=`basename $bed .bed`",
                  '\t\tjava -Xmx%sm -jar %s -T HaplotypeCaller %s -I ${BAM} -L $bed -R %s -o ${GVCF_TMP}/$base.g.vcf.gz' %
                  (self.gaeaHC.mapper_mem, self.gaeaHC.program, self.gaeaHC.parameter, self.ref.normal.ref),
                  "\t\tsleep 3",
                  '\t\tjava -Xmx%sm -jar %s -T GenotypeGVCFs --variant ${GVCF_TMP}/$base.g.vcf.gz -R %s -o ${VCF_TMP}/$base.hc.vcf.gz %s' %
                  (self.gaeaHC.mapper_mem, self.gaeaHC.program, self.ref.normal.ref, self.gaeaHC.GenotypeGVCFs_param),
                  "\telse",
                  '\t\techo "Empty LANE"',
                  "\tfi",
                  "done"
                  ]

        JobParamList = []
        for sampleName in inputInfo:
            hdfs_tmp = os.path.join(self.option.dirHDFS, sampleName, 'hc_tmp')
            vcf_tmp = impl.mkdir(self.option.workdir, "temp", sampleName, 'hc_vcf')
            gvcf_tmp = impl.mkdir(self.option.workdir, "temp", sampleName, 'hc_gvcf')
            # os.chmod(vcf_tmp, 0777)
            scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
            result.output[sampleName] = {}
            result.output[sampleName]['vcf'] = vcf_tmp
            result.output[sampleName]['gvcf'] = gvcf_tmp

            MapperParamDict.update({"BAM":inputInfo[sampleName], "VCF_TMP":vcf_tmp, "GVCF_TMP":gvcf_tmp})
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
                    "VCF_TMP": vcf_tmp,
                    "GVCF_TMP": gvcf_tmp,
                    "OUTTEMP": hdfs_tmp
                })
   
        cmd = ["source %s/bin/activate" % self.GAEA_HOME,
               '%s ${OUTTEMP}' % self.fs_cmd.delete,
               '${PROGRAM} ${HADOOPPARAM} -input ${INPUT} -output ${OUTTEMP} -mapper "sh ${MAPPER}"']
               # 'merge_vcf.py -b %s -p ${VCF_TMP} -o ${OUTPUT}' % self.gaeaHC.bed_list]

        #write script
        scriptPath = \
        impl.write_scripts(
                name = 'gaeaHC',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)

        # result
        result.script.update(scriptPath)
        return result
