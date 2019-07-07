# encoding: utf-8
import os, re
from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow
import shutil


class alignment(Workflow):
    """ alignment (bwa) """

    INIT = bundle(alignment=bundle())
    INIT.alignment.program = "bwa-streaming"
    INIT.alignment.bwaSubTool = 'aln'
    INIT.alignment.streamingJar = "Streaming_fq.jar"
    INIT.alignment.streamingJar2 = "SuffixMultipleTextOutputFormat.jar"
    INIT.alignment.parameter = ''
    INIT.alignment.defaultbwaThreadsNum = 6
    INIT.alignment.fqInputFormat = "org.bgi.flexlab.hadoop.format.fastq.FqText"
    INIT.alignment.fqInputPartitionKey = 2
    INIT.alignment.fqInputOutputKey = 3
    INIT.alignment.bwaReducerMem = ''
    INIT.alignment.bwaReducerNum = ''
    INIT.alignment.bwaReducerNumDivide = ''

    def run(self, impl, dependList):
        impl.log.info("step: alignment!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())

        # program path extend
        self.alignment.program = self.expath('alignment.program')
        self.alignment.streamingJar = self.expath('alignment.streamingJar')
        if self.alignment.parameter.rfind('--enable_filter') != -1:
            self.alignment.streamingJar += ',' + self.expath('alignment.streamingJar2')

        fs_type = 'file://'
        if self.hadoop.input_format == 'hdfs':
            fs_type = ''

        # TODO nofilter
        tmpInputInfo = bundle()
        fastqDir = impl.mkdir(self.option.workdir, 'raw_data', 'fastq')

        if self.alignment.bwaReducerMem:
            hadoop2_reducer_mem = self.alignment.bwaReducerMem
        elif self.hadoop.is_at_TH:
            hadoop2_reducer_mem = '20480'
        else:
            # hadoop2_reducer_mem = '7168'
            hadoop2_reducer_mem = '20480'

        if not self.alignment.bwaReducerNumDivide:
            self.alignment.bwaReducerNumDivide = 3
        elif self.hadoop.is_at_TH:
            self.alignment.bwaReducerNumDivide = 5

        if not self.alignment.get('bwaReducerNum'):
            self.alignment.bwaReducerNum = int(self.hadoop.reducer_num) / int(self.alignment.bwaReducerNumDivide) - 4
        if self.alignment.bwaReducerNum < 1:
            self.alignment.bwaReducerNum = 1

        hadoop_param = '-D mapred.map.tasks=%s ' % str(self.hadoop.mapper_num)
        hadoop_param += '-D mapred.reduce.tasks=%s ' % str(self.alignment.bwaReducerNum)
        hadoop_param += '-D mapreduce.reduce.cpu.vcores=5 '
        if self.hadoop.get('queue'):
            hadoop_param += '-D mapreduce.job.queuename={} '.format(self.hadoop.queue)
        if self.hadoop.get('ishadoop2'):
            hadoop_param += '-D mapreduce.reduce.memory.mb=%s ' % hadoop2_reducer_mem
        hadoop_param += "-D stream.num.map.output.key.fields=%s " % self.alignment.fqInputOutputKey
        hadoop_param += "-D num.key.fields.for.partition=%s " % self.alignment.fqInputPartitionKey
        hadoop_param += "-libjars=%s " % self.alignment.streamingJar
        hadoop_param += "-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner "
        hadoop_param += "-inputformat %s " % self.alignment.fqInputFormat
        hadoop_param += "-outputformat org.bgi.flexlab.gaea.data.mapreduce.output.text.SuffixMultipleTextOutputFormat "
        ParamDict = {
            "PROGRAM": "%s jar %s" % (self.hadoop.bin, self.hadoop.streamingJar),
            "MAPPER": '"/bin/cat"'
        }

        if int(self.init.qualitysystem) != 0:
            if self.analysisDict.has_key('filter'):
                if self.filter.parameter.rfind('-C') != -1:
                    self.init.qualitysystem = '0'
                    impl.log.info("quality system is changed to '0'.")

        rg_param_tag = '-r'
        if self.alignment.bwaSubTool.rfind('mem') != -1:
            rg_param_tag = '-R'
            samplelistparam = '-X'
            subfunc = 'mem'
            if self.init.isSE:
                subfunc += ' -p '
            if int(self.init.qualitysystem) == 1:
                self.alignment.parameter = impl.paramRectify(self.alignment.parameter, '-i', True)
            else:
                self.alignment.parameter = impl.paramRectify(self.alignment.parameter, '-i', False)
        else:
            samplelistparam = '-T'
            subfunc = 'alnpe'
            if self.init.isSE:
                subfunc = 'alnse'
            if int(self.init.qualitysystem) == 1:
                self.alignment.parameter = impl.paramRectify(self.alignment.parameter, '-I', True)
            else:
                self.alignment.parameter = impl.paramRectify(self.alignment.parameter, '-I', False)

        if not self.alignment.parameter.rfind('-t ') != -1:
            self.alignment.parameter += ' -t %s ' % str(self.alignment.defaultbwaThreadsNum)

        fs_cmd = self.fs_cmd
        # sample parameter array
        if self.option.multiSample:
            sampleName = self.option.multiSampleName
            scriptsdir = impl.mkdir(self.option.workdir, "scripts", 'gaea', sampleName)
            multi_fq_dir = os.path.join(fastqDir, sampleName)
            if dependList[0] == 'init':
                LineParam = []
                if os.path.exists(multi_fq_dir):
                    shutil.rmtree(multi_fq_dir)
                impl.mkdir(multi_fq_dir)
                sample_list = os.path.join(scriptsdir, "sampleinfo.list")
                MSL = open(sample_list, 'w')
                line = ["${ID}\t${RG}\t${FQ1}\t${FQ2}\t${ADP1}\t${ADP2}"]
                for sample_name in inputInfo:
                    sample_input = inputInfo[sample_name]
                    sample = self.sample[sample_name]['lane']
                    for dataTag in sample_input:
                        fq1 = sample_input[dataTag]['fq1']
                        fq2 = ''
                        if self.hadoop.input_format != 'hdfs':
                            fqname = os.path.basename(fq1)
                            dstFq1 = os.path.join(multi_fq_dir, '{}_{}_{}'.format(sample_name, dataTag, fqname))
                            os.symlink(fq1, dstFq1)
                            fq1 = dstFq1

                        if not self.sample[sample_name].isSE:
                            fq2 = sample_input[dataTag]['fq2']
                            if self.hadoop.input_format != 'hdfs':
                                fqname = os.path.basename(fq2)
                                dstFq2 = os.path.join(multi_fq_dir, '{}_{}_{}'.format(sample_name, dataTag, fqname))
                                os.symlink(fq2, dstFq2)
                                fq2 = dstFq2

                        LineParam.append({
                            "ID": sample[dataTag]['id'],
                            "RG": sample[dataTag]['rg'],
                            "FQ1": fs_type + fq1,
                            "FQ2": self.sample[sample_name].isSE and 'null' or fs_type + fq2,
                            "ADP1": sample[dataTag].has_key('adp1') and fs_type + sample[dataTag]['adp1'] or 'null',
                            "ADP2": sample[dataTag].has_key('adp2') and fs_type + sample[dataTag]['adp2'] or 'null'
                        })

                    impl.fileAppend(
                        fh=MSL,
                        commands=line,
                        JobParamList=LineParam)

            hdfs_outputPath = os.path.join(self.option.dirHDFS, sampleName, 'BWA_output')
            samplelist = self.results['init']['output'][sampleName]
            sampleInputInfo = inputInfo[sampleName]
            JobParam = []

            if self.ref.gender_mode == "both" and self.option.mode != 5:
                if self.info.female_counter > 0:
                    hadoopParam = ' -D mapred.job.name="bwa_%s_female" %s' % (subfunc, hadoop_param)
                    output = os.path.join(hdfs_outputPath, 'female')
                    JobParam.append({
                        "INPUT": sampleInputInfo.female,
                        "OUTPUT": output,
                        "SUBFUNC": subfunc,
                        "SAMPLELIST": "%s %s" % (samplelistparam, samplelist.female),
                        "INDEX": self.ref.female.ref,
                        "HADOOPPARAM": hadoopParam,
                        "OPTIONPARAM": self.alignment.parameter
                    })

                if self.info.male_counter > 0:
                    hadoopParam = ' -D mapred.job.name="bwa_%s_male" %s' % (subfunc, hadoop_param)
                    output = os.path.join(hdfs_outputPath, 'male')
                    JobParam.append({
                        "INPUT": sampleInputInfo.male,
                        "OUTPUT": output,
                        "SUBFUNC": subfunc,
                        "SAMPLELIST": "%s %s" % (samplelistparam, samplelist.male),
                        "INDEX": self.ref.male.ref,
                        "HADOOPPARAM": hadoopParam,
                        "OPTIONPARAM": self.alignment.parameter
                    })
            elif self.ref.gender_mode == "normal" or self.option.mode == 5:
                hadoopParam = ' -D mapred.job.name="bwa_%s" %s' % (subfunc, hadoop_param)
                output = hdfs_outputPath
                if self.option.mode == 5 or self.alignment.parameter.rfind('--enable_filter') != -1:
                    hadoopParam = ' -D multiSampleList=%s %s' % (samplelist.normal, hadoopParam)
                JobParam.append({
                    "INPUT": sampleInputInfo.normal,
                    "OUTPUT": hdfs_outputPath,
                    "SUBFUNC": subfunc,
                    "SAMPLELIST": "%s %s" % (samplelistparam, samplelist.normal),
                    "INDEX": self.ref.normal.ref,
                    "HADOOPPARAM": hadoopParam,
                    "OPTIONPARAM": self.alignment.parameter
                })

            tmp = impl.mkdir(self.option.workdir, "temp", sampleName, 'filter')
            cmd = []
            cmd.append("%s ${OUTPUT}" % fs_cmd.delete)
            cmd.append(
                '${PROGRAM} ${HADOOPPARAM} -input ${INPUT} -output ${OUTPUT} -mapper ${MAPPER} -reducer "%s ${SUBFUNC} ${SAMPLELIST} ${INDEX} ${OPTIONPARAM} -"' % self.alignment.program)
            cmd.append("%s ${OUTPUT}/_*\n" % fs_cmd.delete)
            cmd.append("%s ${OUTPUT}/*-S %s" % (fs_cmd.cp, tmp))
            cmd.append("%s ${OUTPUT}/*-S" % fs_cmd.delete)

            scriptPath = \
                impl.write_shell(
                    name='alignment',
                    scriptsdir=scriptsdir,
                    commands=cmd,
                    JobParamList=JobParam,
                    paramDict=ParamDict)

            result.output[sampleName] = hdfs_outputPath
            result.script[sampleName] = scriptPath
        else:
            if dependList[0] == 'init':
                for sampleName in inputInfo:
                    tmpInputInfo[sampleName] = bundle()
                    sample_input = inputInfo[sampleName]
                    for dataTag in sample_input:
                        laneDataDir = os.path.join(fastqDir, sampleName, dataTag)
                        if os.path.exists(laneDataDir):
                            shutil.rmtree(laneDataDir)
                        impl.mkdir(laneDataDir)
                        fq1 = sample_input[dataTag]['fq1']
                        fq2 = ''
                        if self.hadoop.input_format != 'hdfs':
                            tmpInputInfo[sampleName][dataTag] = 'file://' + laneDataDir
                            fqname = os.path.basename(fq1)
                            dstFq1 = os.path.join(laneDataDir, '{}_{}_{}'.format(sampleName, dataTag, fqname))
                            os.symlink(fq1, dstFq1)
                            fq1 = dstFq1

                            if not self.sample[sampleName].isSE:
                                fq2 = sample_input[dataTag]['fq2']
                                fqname = os.path.basename(fq2)
                                dstFq2 = os.path.join(laneDataDir, '{}_{}_{}'.format(sampleName, dataTag, fqname))
                                os.symlink(fq2, dstFq2)
                                fq2 = dstFq2
                        else:
                            tmpInputInfo[sampleName][dataTag] = os.path.dirname(sample_input[dataTag]['fq1'])

                inputInfo = tmpInputInfo

            for sampleName in inputInfo:
                scriptsdir = os.path.join(self.gaeaScriptsDir, sampleName)
                hdfs_outputPath = os.path.join(self.option.dirHDFS, sampleName, 'BWA_output')
                sampleInputInfo = inputInfo[sampleName]
                JobParam = []
                RedParam = []

                for dataTag in sampleInputInfo:
                    hdfs_outDir = os.path.join(hdfs_outputPath, sampleName + "-" + dataTag)
                    name = "%s-%s" % (sampleName, dataTag)

                    rg = self.sample[sampleName]['rg'][dataTag]
                    parameter = "%s '%s' %s " % (rg_param_tag, rg, self.alignment.parameter)
                    gender = self.sample[sampleName].gender
                    RedParam.append({
                        "DATATAG": dataTag,
                        "SUBFUNC": subfunc,
                        "INDEX": self.ref[gender].ref,
                        "OPTIONPARAM": parameter
                    })

                    JobParam.append({
                        "DATATAG": dataTag,
                        "INPUT": sampleInputInfo[dataTag],
                        "OUTDIR": hdfs_outDir,
                        "HADOOPPARAM": ' -D mapred.job.name="bwa_%s_%s" %s' % (subfunc, name, hadoop_param),
                        "REDUCER": '"sh %s"' % os.path.join(scriptsdir, 'bwa_reducer_%s.sh' % name)
                    })

                # write script (multi file)
                impl.write_file(
                    fileName='bwa_reducer_%s-${DATATAG}.sh' % sampleName,
                    scriptsdir=scriptsdir,
                    commands=["%s ${SUBFUNC} ${INDEX} ${OPTIONPARAM} -" % self.alignment.program],
                    JobParamList=RedParam)

                tmp = impl.mkdir(self.option.workdir, "temp", sampleName, 'filter')
                cmd = []
                cmd.append('echo "alignment %s-${DATATAG}"' % sampleName)
                cmd.append("%s ${OUTDIR}" % fs_cmd.delete)
                cmd.append(
                    "${PROGRAM} ${HADOOPPARAM} -input ${INPUT} -output ${OUTDIR} -mapper ${MAPPER} -reducer ${REDUCER}")
                cmd.append("%s ${OUTDIR}/*-S %s" % (fs_cmd.cp, tmp))
                cmd.append("%s ${OUTDIR}/*-S" % fs_cmd.delete)
                # write_shell name: no ext .sh (just one file)
                scriptPath = \
                    impl.write_shell(
                        name='alignment',
                        scriptsdir=scriptsdir,
                        commands=cmd,
                        JobParamList=JobParam,
                        paramDict=ParamDict)

                result.output[sampleName] = hdfs_outputPath
                result.script[sampleName] = scriptPath

        # return
        return result
