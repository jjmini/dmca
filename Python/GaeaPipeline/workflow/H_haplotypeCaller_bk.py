# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class haplotypeCaller(Workflow):
    """ haplotypeCaller """

    INIT = bundle(haplotypeCaller=bundle())
    INIT.haplotypeCaller.program = "GenomeAnalysisTK.jar"
    INIT.haplotypeCaller.parameter = ""
    INIT.haplotypeCaller.bed_list = ""
    INIT.haplotypeCaller.mapper_mem = "51200"

    def run(self, impl, dependList):
        impl.log.info("step: haplotypeCaller!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())
        bed_dir = impl.mkdir(self.option.workdir, "temp", "bed")


        # extend program path
        self.haplotypeCaller.program = self.expath('haplotypeCaller.program')
        # self.haplotypeCaller.program_split_bed = self.expath('haplotypeCaller.program_split_bed')
        self.haplotypeCaller.bed_list = self.expath('haplotypeCaller.bed_list')

        bed_num = 0
        with open(self.haplotypeCaller.bed_list, 'r') as bed:
            bed_num = len(bed.readlines())
        # global param
        MapperParamDict = self.file.copy()
        hadoop_parameter = ' -D mapred.job.name="haplotypeCaller" '
        hadoop_parameter += ' -D mapred.map.tasks={} '.format(bed_num)
        hadoop_parameter += ' -D mapred.reduce.tasks=0 '
        hadoop_parameter += ' -D mapreduce.map.memory.mb=%s ' % self.haplotypeCaller.mapper_mem
        hadoop_parameter += ' -D mapreduce.map.cpu.vcores=13 '
        hadoop_parameter += ' -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat '
        ParamDict = {
            "PROGRAM": "%s jar %s" % (self.hadoop.bin, self.hadoop.streamingJar),
            "INPUT": 'file://' + self.haplotypeCaller.bed_list,
            "HADOOPPARAM": hadoop_parameter
        }

        mapper = ["#!/usr/bin/perl -w",
                  "use strict;",
                  "use File::Basename;\n",
                  "while(<STDIN>){",
                  "\tchomp;",
                  "\tmy @tmp = split(/\\t/);",
                  "\tmy $base=fileparse($_, qr\{.bed});",
                  "\tif(!-e $tmp[1])\n\t{",
                  '\t\tprint "$tmp[1] don\'t exist.\\n";',
                  '\t\texit 1;\n\t}',
                  '\tsystem("java -Xmx%s -jar %s -T HaplotypeCaller %s -I ${BAM} -L $tmp[1] -R %s -o ${VCF_TMP}.$base.g.vcf");\n}' %
                  (self.haplotypeCaller.mapper_mem, self.haplotypeCaller.program, self.haplotypeCaller.parameter, self.ref.normal.ref)
                  ]

        JobParamList = []
        for sampleName in inputInfo:
            hdfs_tmp = os.path.join(self.option.dirHDFS, sampleName, 'hc_tmp')
            tmp = impl.mkdir(self.option.workdir, "temp", sampleName, 'hc')
            os.chmod(tmp, 777)
            scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
            outputPath = impl.mkdir(self.option.workdir, "variation", 'haplotypeCaller', sampleName)
            result.output[sampleName] = os.path.join(outputPath, "{}.hc.vcf.gz".format(sampleName))

            MapperParamDict.update({"BAM":inputInfo[sampleName], "VCF_TMP":tmp})
            impl.write_file(
                fileName='hc_mapper.pl',
                scriptsdir=scriptsdir,
                commands=mapper,
                paramDict=MapperParamDict)

            #global param
            JobParamList.append({
                    "SAMPLE" : sampleName,
                    "SCRDIR" : scriptsdir,
                    "MAPPER": os.path.join(scriptsdir, "hc_mapper.pl"),
                    "OUTTEMP": hdfs_tmp
                })

        cmd = ['%s ${OUTTEMP}' % self.fs_cmd.delete,
               '${PROGRAM} ${HADOOPPARAM} -input ${INPUT} -output ${OUTTEMP} -mapper "perl ${MAPPER}"']

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
