# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class jointcalling(Workflow):
    """ jointcalling """

    INIT = bundle(jointcalling=bundle())
    INIT.jointcalling.program = "gaea-1.0.0.jar"
    INIT.jointcalling.parameter = "-s 10.0 -S 10.0"

    def run(self, impl, dependList):
        impl.log.info("step: jointcalling!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())

        hadoop_parameter = ''
        if self.hadoop.get('queue'):
            hadoop_parameter += ' -D mapreduce.job.queuename={} '.format(self.hadoop.queue)

        # extend program path
        self.jointcalling.program = self.expath('jointcalling.program')

        # global param
        ParamDict = self.file.copy()
        ParamDict.update({
            "PROGRAM": "%s jar %s JointCalling %s" % (self.hadoop.bin, self.jointcalling.program, hadoop_parameter),
            "REF": "file://%s" % self.ref.normal.gaeaIndex,
            "REDUCERNUM": self.hadoop.reducer_num
        })

        # script template
        fs_cmd = self.fs_cmd
        cmd = []
        cmd.append("%s ${INPUT}/_*" % fs_cmd.delete)
        cmd.append("%s ${OUTDIR}" % fs_cmd.delete)
        cmd.append("${PROGRAM} -i ${INPUT} -o ${OUTDIR} -r ${REF} -n ${REDUCERNUM} %s" % self.jointcalling.parameter)

        JobParamList = []
        for sampleName in inputInfo:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
            hdfs_outputPath = os.path.join(self.option.dirHDFS, sampleName, 'jointcalling_output')
            result.output[sampleName] = os.path.join(hdfs_outputPath, 'vcf')

            # global param
            JobParamList.append({
                "SAMPLE": sampleName,
                "SCRDIR": scriptsdir,
                "INPUT": inputInfo[sampleName],
                "OUTDIR": hdfs_outputPath
            })

        # write script
        scriptPath = \
            impl.write_scripts(
                name='jointcalling',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)

        # result
        result.script.update(scriptPath)
        return result
