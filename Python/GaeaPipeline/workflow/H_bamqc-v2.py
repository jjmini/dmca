# encoding: utf-8
import os
from gaeautils.bundle import bundle
from gaeautils.workflow import Workflow

class bamqc(Workflow):
    """ bamqc v2 """

    INIT = bundle(bamqc=bundle())
    INIT.bamqc.program = "gaea-1.0.0.jar"
    INIT.bamqc.exonDepthSort = ""
    INIT.bamqc.parameter = ''

    def run(self, impl,dependList):
        impl.log.info("step: bamqc!")
        inputInfo = self.results[dependList[0]].output
        result = bundle(output=bundle(),script=bundle())
        
        #extend program path
        self.bamqc.program = self.expath('bamqc.program')
        self.bamqc.exonDepthSort = self.expath('bamqc.exonDepthSort',False)

        hadoop_parameter = ''
        if self.hadoop.get('queue'):
            hadoop_parameter += ' -D mapreduce.job.queuename={} '.format(self.hadoop.queue)
        
        #script template    
        fs_cmd = self.fs_cmd
        cmd = ["export HADOOP_HEAPSIZE=4096"]
        cmd.append("%s ${OUTDIR}" % fs_cmd.delete )
        cmd.append("${PROGRAM} %s -i ${INPUT} -d ${REF} -o ${OUTDIR} ${REGION} -r ${REDUCERNUM} ${PARAM}" % hadoop_parameter)
        cmd.append("#rm exists files:")

        if 'newCnv' in self.analysisList:
            if self.bamqc.parameter.find('-C') == -1:
                self.bamqc.parameter += ' -C '

        if self.option.multiSample:
            for sample_name in self.sample:
                cmd.append("rm ${{QCDIR}}/{0}.bam.report.txt ${{QCDIR}}/{0}.unmapped.bed".format(sample_name))
                cmd.append("rm ${{QCDIR}}/{0}.insert.xls ${{QCDIR}}/{0}.chromosome.txt".format(sample_name))
                cmd.append("rm ${{QCDIR}}/{0}.depth.txt".format(sample_name))
                if self.bamqc.parameter.find('-a') != -1:
                    cmd.append("rm -rf ${QCDIR}/%s.region.depth.tsv.gz ${QCDIR}/%s.region.cov.tsv.gz ${QCDIR}/%s.region.lowdepth.cov.tsv"% (sample_name,sample_name,sample_name))
                if self.bamqc.parameter.find('-C') != -1:
                    pool = self.sample[sample_name].get('pool')
                    cnvdir = impl.mkdir(self.option.workdir, "variation", 'cnv', pool)
                    if self.hadoop.fs_mode == 'hdfs':
                        cmd.append("\n#cp from hdfs to local:")
                        cmd.append("rm -rf %s/%s*" % (cnvdir,sample_name))
                        cmd.append("%s ${OUTDIR}/cnvDepth/%s* %s" % (fs_cmd.cp,sample_name,cnvdir))
        else:
            cmd.append("rm ${QCDIR}/*bam.report.txt ${QCDIR}/*unmapped.bed" )
            cmd.append("rm ${QCDIR}/*insert.xls ${QCDIR}/*chromosome.txt")
            cmd.append("rm ${QCDIR}/*depth.txt")
            if self.bamqc.parameter.find('-a') != -1:
                cmd.append("rm -rf ${QCDIR}/*region.depth.tsv.gz ${QCDIR}/*region.cov.tsv.gz ${QCDIR}/*region.lowdepth.cov.tsv")

        cmd.append("\n#cp from hdfs to local:")
        cmd.append("%s ${OUTDIR}/*report.txt ${QCDIR}" % fs_cmd.cp)
        cmd.append("%s ${OUTDIR}/*unmapped.bed ${QCDIR}" % fs_cmd.cp)
        cmd.append("%s ${OUTDIR}/*insert.xls ${QCDIR}" % fs_cmd.cp)
        cmd.append("%s ${OUTDIR}/*chromosome.txt ${QCDIR}" % fs_cmd.cp)
        cmd.append("%s ${OUTDIR}/*depth.txt ${QCDIR}" % fs_cmd.cp)
        if self.bamqc.parameter.find('-a') != -1:
            cmd.append("%s ${OUTDIR}/*region.depth.tsv.gz ${QCDIR}" % fs_cmd.cp)
            cmd.append("%s ${OUTDIR}/*region.cov.tsv.gz ${QCDIR}" % fs_cmd.cp)
            cmd.append("%s ${OUTDIR}/*region.lowdepth.cov.tsv ${QCDIR}" % fs_cmd.cp)
            if self.bamqc.get('exonDepthSort'):
                cmd.append("perl %s ${QCDIR} ${QCDIR}" % self.bamqc.exonDepthSort)
                
            
        #global param
        ParamDict = {
                "PROGRAM": "%s jar %s BamQualityControl" % (self.hadoop.bin, self.bamqc.program),
                "REF": "file://%s" % self.ref.normal.gaeaIndex,
                "REGION" : self.file.get('region') and " -B file://%s" % self.file.region or '',
                "REDUCERNUM":self.hadoop.reducer_num,
                "PARAM":self.bamqc.parameter
            }
        
        JobParamList = []
        for sampleName in inputInfo:
            scriptsdir = impl.mkdir(self.gaeaScriptsDir,sampleName)
            hdfs_outputPath = os.path.join(self.option.dirHDFS,sampleName,'BAMQC_output')
            QCDir = impl.mkdir(self.option.workdir, 'QC', sampleName)
            result.output[sampleName] = QCDir
            
                       
            JobParamList.append({
                    "SAMPLE": sampleName,
                    "SCRDIR": scriptsdir,
                    "INPUT": inputInfo[sampleName],
                    "QCDIR": QCDir,
                    "OUTDIR": hdfs_outputPath
                })
            
    
        #write script
        scriptPath = \
        impl.write_scripts(
                name = 'bamqc',
                commands=cmd,
                JobParamList=JobParamList,
                paramDict=ParamDict)
    
        #result
        result.script.update(scriptPath)
        return result
                                
