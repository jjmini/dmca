# encoding: utf-8
import os, re
from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow
import shutil
import math


class bwafilter(Workflow):
    """ bwafilter (bwa) """

    INIT = bundle(bwafilter=bundle())
    INIT.bwafilter.program = "bwa-streaming"
    INIT.bwafilter.program_report = "filter_report_merge.py"
    INIT.bwafilter.bwaSubTool = 'aln'
    INIT.bwafilter.streamingJar = "Streaming_fq.jar"
    INIT.bwafilter.streamingJar2 = "SuffixMultipleTextOutputFormat.jar"
    INIT.bwafilter.parameter = ''
    INIT.bwafilter.defaultbwaThreadsNum = 6
    INIT.bwafilter.fqInputFormat = "org.bgi.flexlab.hadoop.format.fastq.FqText"
    INIT.bwafilter.fqInputPartitionKey = 2
    INIT.bwafilter.fqInputOutputKey = 3
    INIT.bwafilter.bwaReducerMem = ''
    INIT.bwafilter.bwaReducerNum = ''
    INIT.bwafilter.bwaReducerNumDivide = ''

    def run(self, impl, dependList):
        impl.log.info("step: bwafilter!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())

        # program path extend
        self.bwafilter.program = self.expath('bwafilter.program')
        self.bwafilter.streamingJar = self.expath('bwafilter.streamingJar')
        self.bwafilter.streamingJar += ',' + self.expath('bwafilter.streamingJar2')
        self.bwafilter.program_report = self.expath('bwafilter.program_report')

        fs_type = 'file://'
        if self.hadoop.input_format == 'hdfs':
            fs_type = ''

        # TODO nofilter
        tmpInputInfo = bundle()
        fastqDir = impl.mkdir(self.option.workdir, 'raw_data', 'fastq')

        if self.bwafilter.bwaReducerMem:
            hadoop2_reducer_mem = self.bwafilter.bwaReducerMem
        elif self.hadoop.is_at_TH:
            hadoop2_reducer_mem = '20480'
        else:
            # hadoop2_reducer_mem = '7168'
            hadoop2_reducer_mem = '20480'

        if not self.bwafilter.bwaReducerNumDivide:
            self.bwafilter.bwaReducerNumDivide = 5
        elif self.hadoop.is_at_TH:
            self.bwafilter.bwaReducerNumDivide = 5

        if not self.bwafilter.get('bwaReducerNum'):
            self.bwafilter.bwaReducerNum = int(math.ceil(int(self.hadoop.reducer_num) / 42.0)) * 8 - 1
        if self.bwafilter.bwaReducerNum < 1:
            self.bwafilter.bwaReducerNum = 1

        hadoop_param = '-D mapreduce.job.maps=%s ' % str(self.hadoop.mapper_num)
        hadoop_param += '-D mapreduce.job.reduces=%s ' % str(self.bwafilter.bwaReducerNum)
        hadoop_param += '-D mapreduce.reduce.cpu.vcores=5 '
        if self.hadoop.get('queue'):
            hadoop_param += '-D mapreduce.job.queuename={} '.format(self.hadoop.queue)
        hadoop_param += '-D mapreduce.reduce.memory.mb=%s ' % hadoop2_reducer_mem
        hadoop_param += "-D stream.num.map.output.key.fields=%s " % self.bwafilter.fqInputOutputKey
        hadoop_param += "-D num.key.fields.for.partition=%s " % self.bwafilter.fqInputPartitionKey
        hadoop_param += "-libjars=%s " % self.bwafilter.streamingJar
        hadoop_param += "-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner "
        hadoop_param += "-inputformat %s " % self.bwafilter.fqInputFormat
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

        subfunc = 'filter'
        if self.init.isSE:
            subfunc += ' --is_se '
        if int(self.init.qualitysystem) == 1:
            self.bwafilter.parameter = impl.paramRectify(self.bwafilter.parameter, '-i', True)
        else:
            self.bwafilter.parameter = impl.paramRectify(self.bwafilter.parameter, '-i', False)

        if not self.bwafilter.parameter.rfind('-t ') != -1:
            self.bwafilter.parameter += ' -t %s ' % str(self.bwafilter.defaultbwaThreadsNum)

        fs_cmd = self.fs_cmd
        # sample parameter array
        if self.option.multiSample:
            print "Donn't surport multisample"
            exit(-1)

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
                RedParam.append({
                    "DATATAG": dataTag,
                    "SUBFUNC": subfunc,
                    "OPTIONPARAM": self.bwafilter.parameter
                })

                JobParam.append({
                    "DATATAG": dataTag,
                    "INPUT": sampleInputInfo[dataTag],
                    "OUTDIR": hdfs_outDir,
                    "QCDIR" : impl.mkdir(self.option.workdir, 'QC', sampleName, 'filter'+dataTag),
                    "QCTMP" : impl.mkdir(self.option.workdir, "temp", sampleName, 'filter'+dataTag),
                    "HADOOPPARAM": ' -D mapreduce.job.name="filter_%s_%s" %s' % (subfunc, name, hadoop_param),
                    "REDUCER": '"sh %s"' % os.path.join(scriptsdir, 'filter_reducer_%s.sh' % name)
                })

            # write script (multi file)
            impl.write_file(
                fileName='filter_reducer_%s-${DATATAG}.sh' % sampleName,
                scriptsdir=scriptsdir,
                commands=["%s ${SUBFUNC} ${OPTIONPARAM} -" % self.bwafilter.program],
                JobParamList=RedParam)

            cmd = []
            cmd.append('echo "bwafilter %s-${DATATAG}"' % sampleName)
            cmd.append("%s ${OUTDIR}" % fs_cmd.delete)
            cmd.append(
                "${PROGRAM} ${HADOOPPARAM} -input ${INPUT} -output ${OUTDIR} -mapper ${MAPPER} -reducer ${REDUCER}")
            cmd.append("%s ${OUTDIR}/*-S ${QCTMP}" % fs_cmd.cp)
            cmd.append("%s ${OUTDIR}/*-S" % fs_cmd.delete)
            cmd.append("%s -i ${QCTMP} -o ${QCDIR}" % self.bwafilter.program_report)
            # write_shell name: no ext .sh (just one file)
            scriptPath = \
                impl.write_shell(
                    name='bwafilter',
                    scriptsdir=scriptsdir,
                    commands=cmd,
                    JobParamList=JobParam,
                    paramDict=ParamDict)

            result.output[sampleName] = hdfs_outputPath
            result.script[sampleName] = scriptPath

        # return
        return result
