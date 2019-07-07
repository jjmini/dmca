# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class BQSRSpark(Workflow):
    """ BQSRSpark """

    INIT = bundle(BQSRSpark=bundle())
    INIT.BQSRSpark.program = "/hwfssz1/BIGDATA_COMPUTING/software/source/gatk4/gatk"
    INIT.BQSRSpark.parameter = '--bam-partition-size 10485760 --disable-sequence-dictionary-validation true --sharded-output true'
    INIT.BQSRSpark.parameter_spark = '--spark-runner SPARK --spark-master yarn --executor-memory 30g ' \
                                     '--executor-cores 4 --driver-memory 30g ' \
                                     '--conf spark.shuffle.blockTransferService=nio ' \
                                     '--conf spark.network.timeout=10000000 ' \
                                     '--conf spark.executor.heartbeatInterval=10000000'

    def run(self, impl, dependList):
        impl.log.info("step: BQSRSpark!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())

        # extend program path
        self.BQSRSpark.program = self.expath('BQSRSpark.program')
        spark_param = self.BQSRSpark.parameter_spark
        if self.hadoop.get('queue'):
            spark_param = impl.paramCheck(True, spark_param, '--queue', self.hadoop.queue)

        # script template
        fs_cmd = self.fs_cmd
        cmd = []
        cmd.append("%s ${OUTPUT}" % fs_cmd.delete)
        cmd.append("%s ${INPUT}/*/_SUCCESS ${INPUT}/*/_logs" % fs_cmd.delete)
        cmd.append("${PROGRAM} BQSRPipelineSpark -I ${INPUT} -O ${OUTPUT} -R ${REF} %s -- %s" % (
        self.BQSRSpark.parameter, spark_param))

        # global param
        ParamDict = self.file.copy()
        ParamDict.update({
            "PROGRAM": self.BQSRSpark.program,
            "REF": self.ref.normal.ref
        })

        JobParamList = []
        for sampleName in inputInfo:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
            hdfs_outputPath = os.path.join(self.option.dirHDFS, sampleName, 'BQSRSpark_output')
            result.output[sampleName] = hdfs_outputPath

            # global param
            JobParamList.append({
                "SAMPLE": sampleName,
                "SCRDIR": scriptsdir,
                "INPUT": inputInfo[sampleName],
                "OUTPUT": hdfs_outputPath,
            })

        scriptPath = \
            impl.write_scripts(
                name='BQSRSpark',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)

        result.script.update(scriptPath)
        return result
