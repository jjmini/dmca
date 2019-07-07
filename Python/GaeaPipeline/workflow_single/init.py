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
    INIT.init.gzUploader = "gaeatools.jar"
    INIT.init.bgzip = 'bgzip'
    INIT.init.perl = 'perl'
    INIT.init.samtools = 'samtools'
    INIT.init.qualitysystem = ''
    INIT.init.check_log = '%s' % os.path.join(os.environ['GAEA_HOME'], 'bin', 'check_log.pl')
    INIT.init.check_state_param = ''
    INIT.hadoop.ishadoop2 = False
    INIT.hadoop.is_at_TH = False
    INIT.hadoop.fs_mode = 'hdfs'
    INIT.hadoop.input_format = 'hdfs'
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

        mapper = []
        mapper.append("#!/usr/bin/perl -w")
        mapper.append("use strict;\n")
        mapper.append("while(<STDIN>)\n{")
        mapper.append("\tchomp;\n\tmy @tmp = split(/\\t/);")
        mapper.append("\tif(!-e $tmp[1])\n\t{")
        mapper.append("\t\tprint \"$tmp[1] don't exist.\\n\";")
        mapper.append("\t\texit 1;\n\t}")
        mapper.append("\tsystem(\"%s jar %s GzUploader -i $tmp[1] -o $tmp[2] -n $tmp[3]\");\n}" % (
        self.hadoop.bin, self.init.gzUploader))

        # self.analysisList = self.analysisList[1:]

        output = bundle()
        for sample_name in sampleInfo.keys():
            raw_data = os.path.join(self.option.dirHDFS, sample_name, 'fq')
            scriptsdir = impl.mkdir(self.gaeaScriptsDir, sample_name)
            DataParam = []
            sample = sampleInfo[sample_name]
            output[sample_name] = bundle()
            # output[sample_name]['outdir'] = bundle()
            for dataTag in sample.keys():
                output[sample_name][dataTag] = bundle()
                pathTup = impl.splitext(sample[dataTag]['fq1'])
                filename = '{}_{}_{}'.format(sample_name, dataTag, pathTup[0])
                DataParam.append({
                    "KEY": sample[dataTag]['fq1'],
                    "VALUE": raw_data,
                    "VALUE2": filename
                })
                output[sample_name][dataTag]['fq1'] = os.path.join(raw_data, filename)

                if not self.init.isSE:
                    pathTup = impl.splitext(sample[dataTag]['fq2'])
                    filename = '{}_{}_{}'.format(sample_name, dataTag, pathTup[0])
                    DataParam.append({
                        "KEY": sample[dataTag]['fq2'],
                        "VALUE": raw_data,
                        "VALUE2": filename
                    })
                    output[sample_name][dataTag]['fq2'] = os.path.join(raw_data, filename)
                    # output[sample_name]['outdir'] = raw_data

            impl.write_file(
                fileName='data.list',
                scriptsdir=scriptsdir,
                commands=["${KEY}\t${VALUE}\t${VALUE2}"],
                JobParamList=DataParam)

            ParamDict = {
                "PROGRAM": "%s jar %s GzUploader" % (self.hadoop.bin, self.init.gzUploader),
                "INPUT": os.path.join(scriptsdir, 'data.list'),
            }

            # write script
            scriptPath = \
                impl.write_shell(
                    name='init',
                    scriptsdir=scriptsdir,
                    commands=['${PROGRAM} -i ${INPUT} -l'],
                    paramDict=ParamDict)

            result.script[sample_name] = scriptPath
        result.output = output

        if self.init.qualitysystem == '':
            self.check_qs(sampleInfo)
            print "[INFO   ]  -- qualitysystem is %s (autocheck)--" % self.init.qualitysystem
        else:
            print "[INFO   ]  -- qualitysystem is %s --" % self.init.qualitysystem

        return result
