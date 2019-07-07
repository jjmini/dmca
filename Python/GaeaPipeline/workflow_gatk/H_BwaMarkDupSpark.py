# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class BwaMarkDupSpark(Workflow):
    """ BwaMarkDupSpark """

    INIT = bundle(BwaMarkDupSpark=bundle())
    INIT.BwaMarkDupSpark.program = "/hwfssz1/BIGDATA_COMPUTING/software/source/gatk4/gatk"
    INIT.BwaMarkDupSpark.parameter = '--bam-partition-size 10485760 --disable-sequence-dictionary-validation true --sharded-output true'
    INIT.BwaMarkDupSpark.parameter_spark = '--spark-runner SPARK --spark-master yarn --executor-memory 30g ' \
                                           '--executor-cores 4 --driver-memory 30g ' \
                                           '--conf spark.shuffle.blockTransferService=nio ' \
                                           '--conf spark.network.timeout=10000000 ' \
                                           '--conf spark.executor.heartbeatInterval=10000000'

    def run(self, impl, dependList):
        impl.log.info("step: BwaMarkDupSpark!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())

        # extend program path
        self.BwaMarkDupSpark.program = self.expath('BwaMarkDupSpark.program')

        spark_param = self.BwaMarkDupSpark.parameter_spark
        if self.hadoop.get('queue'):
            spark_param = impl.paramCheck(True, spark_param, '--queue', self.hadoop.queue)

        # script template
        fs_cmd = self.fs_cmd
        cmd = []
        cmd.append("%s ${OUTPUT}" % fs_cmd.delete)
        cmd.append("%s ${INPUT}/*/_SUCCESS ${INPUT}/*/_logs" % fs_cmd.delete)
        cmd.append("${PROGRAM} BwaAndMarkDuplicatesPipelineSpark -I ${INPUT} -O ${OUTPUT} -R ${REF} ${PARAM} -- ${PARAMSPARK}")

        for sampleName in inputInfo:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
            hdfs_outputPath = os.path.join(self.option.dirHDFS, sampleName, 'BwaMarkDupSpark_output')

            # global param
            ParamDict = {
                "PROGRAM": self.BwaMarkDupSpark.program,
                "INPUT": inputInfo[sampleName],
                "OUTPUT": hdfs_outputPath,
                "REF": self.ref.normal.ref,
                "PARAM": self.BwaMarkDupSpark.parameter,
                "PARAMSPARK": spark_param
            }

            # write script
            scriptPath = \
                impl.write_shell(
                    name='BwaMarkDupSpark',
                    scriptsdir=scriptsdir,
                    commands=cmd,
                    paramDict=ParamDict)

            # result
            result.output[sampleName] = hdfs_outputPath
            result.script[sampleName] = scriptPath
        return result
