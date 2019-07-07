# encoding: utf-8
import os
import shutil

import qualitySystem

from gaeautils import Workflow
from gaeautils import bundle


class init(Workflow):
    """ init data, init data path """

    INIT = bundle(hadoop=bundle(), init=bundle())
    INIT.init.multiUploader = 'multi_uploader.pl'
    INIT.init.gzUploader = "GzUpload.jar"
    INIT.init.gatk = "/hwfssz1/BIGDATA_COMPUTING/software/source/gatk4/gatk"
    INIT.init.bgzip = 'bgzip'
    INIT.init.perl = 'perl'
    INIT.init.samtools = 'samtools'
    INIT.init.qualitySystem = ''
    INIT.init.check_log = '%s' % os.path.join(os.environ['GAEA_HOME'], 'bin', 'check_log.pl')
    INIT.init.check_state_param = ''
    INIT.hadoop.ishadoop2 = False
    INIT.hadoop.is_at_TH = False
    INIT.hadoop.fs_mode = 'hdfs'
    INIT.hadoop.input_format = 'file'
    INIT.hadoop.mapper_num = '112'
    INIT.hadoop.reducer_num = '112'

    def check_qs(self, sampleInfo):
        for sample_name in sampleInfo:
            for dataTag in sampleInfo[sample_name]:
                fq = sampleInfo[sample_name][dataTag]['fq1']
                self.init.qualitySystem = qualitySystem.getQualitySystem(fq)
                if self.init.qualitySystem != '-1':
                    return self.init.qualitySystem

        if self.init.qualitySystem == '-1':
            raise RuntimeError('qualitySystem is wrong, the value is -1')

    def run(self, impl, sampleInfo):
        mode = self.option.mode
        result = bundle(output=bundle(), script=bundle())

        # extend program path
        self.init.multiUploader = self.expath('init.multiUploader')
        self.init.gzUploader = self.expath('init.gzUploader')
        self.init.check_log = self.expath('init.check_log')
        self.init.bgzip = self.expath('init.bgzip', False)
        self.init.samtools = self.expath('init.samtools', False)

        sampleName = self.option.multiSampleName
        scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
        self.analysisList = self.analysisList[1:]
        hdfs_gz_tmp = os.path.join(self.option.dirHDFS, sampleName, 'data', 'gz_tmp')
        tmp = impl.mkdir(self.option.workdir, "temp", sampleName, 'ubam')
        rawData = impl.mkdir(self.option.workdir, "ubam", sampleName)

        ubam = []
        DataParam = []
        output = bundle()
        cmd = []
        for sample_name in sampleInfo.keys():
            sample = sampleInfo[sample_name]
            output[sample_name] = bundle()
            for dataTag in sample.keys():
                output[sample_name][dataTag] = bundle()
                filename = '{}_{}.bam'.format(sample_name, dataTag)
                output[sample_name][dataTag]['bam'] = os.path.join(rawData, filename)
                ubam.append(output[sample_name][dataTag]['bam'])

                DataParam.append({
                    "KEY1": sample[dataTag]['fq1'],
                    "KEY2": sample[dataTag]['fq2'],
                    "KEY3": output[sample_name][dataTag]['bam'],
                    "KEY4": sample_name,
                    "KEY5": sample_name + "_" + dataTag
                })

        if DataParam:
            impl.write_file(
                fileName='data.list',
                scriptsdir=scriptsdir,
                commands=["${KEY1}\t${KEY2}\t${KEY3}\t${KEY4}\t${KEY5}"],
                JobParamList=DataParam)

            mapper = []
            mapper.append("#!/usr/bin/perl -w")
            mapper.append("use strict;\n")
            mapper.append("while(<STDIN>)\n{")
            mapper.append("\tchomp;\n\tmy @tmp = split(/\\t/);")
            mapper.append("\tif(!-e $tmp[1])\n\t{")
            mapper.append("\t\tprint \"$tmp[1] don't exist.\\n\";")
            mapper.append("\t\texit 1;\n\t}")
            mapper.append("\tsystem(\"%s FastqToSam -F1 $tmp[1] -F2 $tmp[2] -O $tmp[3] -SM $tmp[4] -RG $tmp[5] --TMP_DIR %s -PL illumina\");\n}" % (self.init.gatk, tmp ))
            impl.write_file(
                fileName='upload_mapper.pl',
                scriptsdir=scriptsdir,
                commands=mapper)

            hadoop_parameter = ' -D mapred.job.name="upload data" '
            if self.hadoop.get('queue'):
                hadoop_parameter += '-D mapreduce.job.queuename={} '.format(self.hadoop.queue)
            hadoop_parameter += ' -D mapred.map.tasks=%d ' % len(DataParam)
            hadoop_parameter += '-D mapreduce.map.memory.mb=10240 '
            hadoop_parameter += ' -D mapred.reduce.tasks=0 '
            hadoop_parameter += ' -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat '
            ParamDict = {
                "PROGRAM": "%s jar %s" % (self.hadoop.bin, self.hadoop.streamingJar),
                "MAPPER": os.path.join(scriptsdir, 'upload_mapper.pl'),
                "INPUT": 'file://' + os.path.join(scriptsdir, 'data.list'),
                "OUTPUT": hdfs_gz_tmp,
                "HADOOPPARAM": hadoop_parameter
            }

            cmd.append('%s ${OUTPUT}' % self.fs_cmd.delete)
            cmd.append('${PROGRAM} ${HADOOPPARAM} -input ${INPUT} -output ${OUTPUT} -mapper "perl ${MAPPER}"')

            # cmd.append('%s jar %s GzUploader -i %s -l' % (
            # self.hadoop.bin, self.init.gzUploader, os.path.join(scriptsdir, 'data.list')))

            # write script
            scriptPath = \
                impl.write_shell(
                    name='init',
                    scriptsdir=scriptsdir,
                    commands=cmd,
                    paramDict=ParamDict)
            result.script[sampleName] = scriptPath

        result.output = output

        if self.init.qualitySystem == '':
            self.check_qs(sampleInfo)
            print "[INFO   ]  -- qualitySystem is %s (autocheck)--" % self.init.qualitySystem
        else:
            print "[INFO   ]  -- qualitySystem is %s --" % self.init.qualitySystem

        return result
