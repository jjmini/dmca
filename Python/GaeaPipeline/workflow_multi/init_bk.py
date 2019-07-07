# encoding: utf-8
import os
import shutil

import qualitysystem

from gaeautils import Workflow
from gaeautils import bundle


class init(Workflow):
    """ init data, init data path """

    INIT = bundle(hadoop=bundle(), init=bundle())
    INIT.init.multiUploader = 'multi_uploader.pl'
    INIT.init.gzUploader = "GzUpload.jar"
    INIT.init.bgzip = 'bgzip'
    INIT.init.perl = 'perl'
    INIT.init.samtools = 'samtools'
    INIT.init.qualitysystem = ''
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
                self.init.qualitysystem = qualitysystem.getqualitysystem(fq)
                if self.init.qualitysystem != '-1':
                    return self.init.qualitysystem

        if self.init.qualitysystem == '-1':
            raise RuntimeError('qualitysystem is wrong, the value is -1')

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

        # result.output[sampleName] = output

        DataParam = []
        output = bundle()
        cmd = []
        for sample_name in sampleInfo.keys():
            sample = sampleInfo[sample_name]
            output[sample_name] = bundle()
            for dataTag in sample.keys():
                rawData = impl.mkdir(self.option.workdir, 'fq', 'raw_data', sample_name)
                laneData = os.path.join(rawData, dataTag)
                cmd.append("mkdir -p -m 777 %s" % laneData)
                output[sample_name][dataTag] = bundle()
                pathTup = impl.splitext(sample[dataTag]['fq1'])
                if pathTup and pathTup[1] == '.gz':
                    DataParam.append({
                        "KEY": sample[dataTag]['fq1'],
                        "VALUE": os.path.join(laneData, pathTup[0])
                    })
                    output[sample_name][dataTag]['fq1'] = os.path.join(laneData, pathTup[0])
                else:
                    output[sample_name][dataTag]['fq1'] = sample[dataTag]['fq1']

                if self.init.isSE == False:
                    pathTup = impl.splitext(sample[dataTag]['fq2'])
                    if pathTup and pathTup[1] == '.gz':
                        DataParam.append({
                            "KEY": sample[dataTag]['fq2'],
                            "VALUE": os.path.join(laneData, pathTup[0])
                        })
                        output[sample_name][dataTag]['fq2'] = os.path.join(laneData, pathTup[0])
                    else:
                        output[sample_name][dataTag]['fq2'] = sample[dataTag]['fq2']

                if sample[dataTag].has_key('adp1'):
                    pathTup = impl.splitext(sample[dataTag]['adp1'])
                    if pathTup and pathTup[1] == '.gz':
                        DataParam.append({
                            "KEY": sample[dataTag]['adp1'],
                            "VALUE": os.path.join(laneData, pathTup[0])
                        })
                        output[sample_name][dataTag]['adp1'] = os.path.join(laneData, pathTup[0])
                    else:
                        output[sample_name][dataTag]['adp1'] = sample[dataTag]['adp1']

                if sample[dataTag].has_key('adp2'):
                    pathTup = impl.splitext(sample[dataTag]['adp2'])
                    if pathTup and pathTup[1] == '.gz':
                        DataParam.append({
                            "KEY": sample[dataTag]['adp2'],
                            "VALUE": os.path.join(laneData, pathTup[0])
                        })
                        output[sample_name][dataTag]['adp2'] = os.path.join(laneData, pathTup[0])
                    else:
                        output[sample_name][dataTag]['adp2'] = sample[dataTag]['adp2']
                        #                 print DataParam
        if DataParam:
            impl.write_file(
                fileName='data.list',
                scriptsdir=scriptsdir,
                commands=["${KEY}\t${VALUE}"],
                JobParamList=DataParam)

            mapper = []
            mapper.append("#!/usr/bin/perl -w")
            mapper.append("use strict;\n")
            mapper.append("while(<STDIN>)\n{")
            mapper.append("\tchomp;\n\tmy @tmp = split(/\\t/);")
            mapper.append("\tif(!-e $tmp[1])\n\t{")
            mapper.append("\t\tprint \"$tmp[1] don't exist.\\n\";")
            mapper.append("\t\texit 1;\n\t}")
            mapper.append("\tsystem(\"gzip -cd $tmp[1] >$tmp[2]\");\n}")
            impl.write_file(
                fileName='upload_mapper.pl',
                scriptsdir=scriptsdir,
                commands=mapper)

            hadoop_parameter = ' -D mapred.job.name="gzip input data" '
            if self.hadoop.get('queue'):
                hadoop_parameter += '-D mapreduce.job.queuename={} '.format(self.hadoop.queue)
            hadoop_parameter += ' -D mapred.map.tasks=%d ' % len(DataParam)
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

            # write script
            scriptPath = \
                impl.write_shell(
                    name='init',
                    scriptsdir=scriptsdir,
                    commands=cmd,
                    paramDict=ParamDict)
            result.script[sampleName] = scriptPath

        result.output = output

        if self.init.qualitysystem == '':
            self.check_qs(sampleInfo)
            print "[INFO   ]  -- qualitysystem is %s (autocheck)--" % self.init.qualitysystem
        else:
            print "[INFO   ]  -- qualitysystem is %s --" % self.init.qualitysystem

        return result
