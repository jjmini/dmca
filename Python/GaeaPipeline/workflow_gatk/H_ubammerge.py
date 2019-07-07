# encoding: utf-8
import os

from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow


class ubammerge(Workflow):
    """ ubammerge """

    INIT = bundle(ubammerge=bundle())
    INIT.ubammerge.program = "/hwfssz1/BIGDATA_COMPUTING/software/source/gatk4/gatk"
    INIT.ubammerge.parameter = ''

    def run(self, impl, dependList):
        impl.log.info("step: ubammerge!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(), script=bundle())
        sampleName = self.option.multiSampleName
        merge_tmp = impl.mkdir(self.option.workdir, "temp", sampleName, 'ubammerge.bam')

        # extend program path
        self.ubammerge.program = self.expath('ubammerge.program')

        # script template
        fs_cmd = self.fs_cmd
        cmd = []
        # cmd.append("%s ${OUTDIR}/" % fs_cmd.delete)
        # cmd.append("%s ${INPUT}/*/_SUCCESS ${INPUT}/*/_logs" % fs_cmd.delete)

        scriptsdir = impl.mkdir(self.gaeaScriptsDir, sampleName)
        hdfs_outputPath = os.path.join(self.option.dirHDFS, sampleName, 'ubammerge_output')
        bams = []
        for sample_name in inputInfo:
            sample_input = inputInfo[sample_name]
            for dataTag in sample_input:
                bams.append(sample_input[dataTag]['bam'])

        if len(bams) <= 1:
            merge_tmp = bams[0]
        else:
            input_bam_command = ''
            for input_bam in bams:
                input_bam_command += "--UNMAPPED_BAM {} ".format(input_bam)
            cmd.append('%s MergeBamAlignment %s -O %s -R %s' % (self.ubammerge.program, input_bam_command, merge_tmp, self.ref.normal.ref))
        cmd.append('%s fs -mkdir -p %s' % (self.hadoop.bin, hdfs_outputPath))
        cmd.append('%s fs -put %s %s' % (self.hadoop.bin, merge_tmp, hdfs_outputPath))

        # write script
        scriptPath = \
            impl.write_shell(
                name='ubammerge',
                scriptsdir=scriptsdir,
                commands=cmd,
                paramDict=[])

        # result
        result.output[sampleName] = hdfs_outputPath
        result.script[sampleName] = scriptPath

        return result
