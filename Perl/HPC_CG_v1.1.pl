#!/usr/bin/perl -w
use strict;
use Cwd 'abs_path';
use File::Basename qw(basename dirname);
use Getopt::Long;

=head1 NAME:

	Program:	HPC_CG.pl
	Version:	1.0
	Date   :	2015-7-14
	Description:	HPC CG pipeline

	Mail bug reports to	huangzhibo@genomics.cn

=head1 USAGE:

	perl HPC_chip.pl [options]
	-o	STR	the output dir [`pwd`]
	-l	FILE	the sample list file, format example:  [required]
			    ----------------------------------------------------------------------------------------------------
			    >GS89125-FS3_barcode_36
			    rg=@RG\tID:GS89125-FS3-L01_36\tSM:GS89125-FS3_barcode_36\tPL:COMPLETE
			    fq1=/ifs5/ST_TRANS_CARDIO/PMO/CG_blackbird_test/HPC_CG_PE_test/rawdata/GS89125-FS3-L01_36.SBS.fq.gz
			    fq2=/ifs5/ST_TRANS_CARDIO/PMO/CG_blackbird_test/HPC_CG_PE_test/rawdata/GS89125-FS3-L01_36.N6.fq.gz
			    gender=male
			    >
			    ...
			    ----------------------------------------------------------------------------------------------------
	-c	FILE	the config file, contain a hash %config [required]
	-q	STR	the queue of the job [st.q]
	-p	STR	project ID	[required]
	-s	STR	the steps write to run.pl (for qsub), eg. <-s all>, <-s adv>, <-s v> [adrbv]
		a	filter and bwa: contain low quality filter, bwa, and picard SamSort
		d	duplication marking
		r	RealignerTargetCreator and IndelRealigner
		b	BaseRecalibrator and PrintReads
		v	variation calling
		f	variation filter
		n	annotation
		q	QC, depth statistics
		c	CNV

=head1 Example

	perl HPC_CG_v1.0.pl -l sample.list -c config_GAEA_clinical_gene_panel_v0.2_bgicg_single.pl -o ../ -q st.q -p F14ZQSQS1955

=cut

my $HPC_CHIP;
our $BGICGA;
BEGIN {
	die "Please specify environment variable HPC_CHIP_HOME as the home of HPC_CHIP pipeline.\n" if (!exists $ENV{"HPC_CHIP_HOME"});
	$HPC_CHIP = $ENV{"HPC_CHIP_HOME"};
	die "Please specify environment variable BGICGA_HOME as the home of bgicg annotation pipeline.\n" if (!exists $ENV{"BGICGA_HOME"});
	$BGICGA = $ENV{"BGICGA_HOME"};
}

my ($outdir,$rawdir,$list,$config,$queue,$proj_id,$interval,$step);
GetOptions(
	"o:s" => \$outdir,
	"r:s" => \$rawdir,
	"l:s" => \$list,
	"c:s" => \$config,
	"s:s" => \$step,
	"q:s" => \$queue,
	"p:s" => \$proj_id
);
die `pod2text $0` unless($list and $config and $proj_id);

$outdir||=`pwd`;
chomp($outdir);
$rawdir||="$outdir/fq";
$step||="adrbv";
$queue||="st.q";

$outdir=~s/\/$//;
$rawdir=~s/\/$//;

$outdir=abs_path($outdir);
$rawdir=abs_path($rawdir);

my $proj_para = "";
if(defined$proj_id)
{
	$proj_para = "-q $queue -P $proj_id";
}

our %config;
require $config;
# $config{"SOAPnuke"}||="-l 10 -q 0.5 -n 0.1 -G";
$config{"fastqQC"}||="";
$config{"bwa_mem"}||="-t 3 -M ";
$config{"bwa_aln"}||="-t 5 -l 25 ";
$config{"RTC"}||="-nt 8";
$config{"BR"}||="-nct 3";
$config{"PR"}||="-nct 3";
$config{"gatk_UnifiedGenotyper"}||="true";
$config{"gatk_genotyper"}||="--genotype_likelihoods_model BOTH  -stand_call_conf 30.0 -stand_emit_conf 10.0 -out_mode EMIT_ALL_SITES  --min_base_quality_score 5";
$config{"gatk_variation"}||="-nct 8 -stand_call_conf 30.0 -stand_emit_conf 10.0";
#$config{"gatk_variation"}||="-nct 8 -dcov 1000 -stand_call_conf 30.0 -stand_emit_conf 10.0";
$config{"gatk_variation_select"}||="-nt 8";
$config{"snv_filter"}||="QD<2.0 || MQ<40.0 || FS>60.0 || HaplotypeScore>13.0 || MQRankSum<-12.5 || ReadPosRankSum<-8.0";
$config{"indel_filter"}||="QD<2.0 || ReadPosRankSum<-20.0 || InbreedingCoeff<-0.8 || FS>200.0";
$config{"anno"}||="-n 5 -b 100000 -q";
$config{"filter_rules"}||=$BGICGA."/rule_spec/excel_filter_rules_example.rc";
$config{"headers"}||=$BGICGA."/rule_spec/excel_headers.online.preSet";

my ($trim1,$trim2)=(0,0);
# if($config{"SOAPnuke"}=~/-t\s(\d+),(\d+),(\d+),(\d+)/){
# 	$trim1=$1+$2;
# 	$trim2=$3+$4;
# }

my $gc_file;
if(exists $config{"run_bed"}){
	$interval=$config{"run_bed"};
	unless($interval=~/\.bed$/){
		print "USAGE:\n\tThe run_bed file must be bed file\n";
		exit;
	}
	$gc_file=$config{"run_bed"};
	$gc_file=~s/bed$/gc/;
}

unless(exists $config{"run_type"} and ($config{"run_type"} eq "single")){
	print "USAGE:\n\tPlease set the correct run_type value.\n\t\"single\"\tone sample in one vcf file\n\t\"fam\"\t\tone family in one vcf file\n";
	exit;
}

# tools
$config{"picard"}||="$HPC_CHIP/tools/picard";
my $picard=$config{"picard"};
my $gatk="$HPC_CHIP/tools/GATK3/GenomeAnalysisTK.jar";
my $fastqQC = "/home/huangzhibo/jar/FastqQC.jar";

# data
my %hg19_fa;
$hg19_fa{M}="$HPC_CHIP/db/aln_db/hg19/hg19_chM_male_mask.fa";
$hg19_fa{F}="$HPC_CHIP/db/aln_db/hg19/hg19_chM_female.fa";
my $dbsnp="$HPC_CHIP/db/aln_db/hg19/dbsnp_135.hg19.modify.vcf";
my $gold_indels = "$HPC_CHIP/db/aln_db/hg19/Mills_and_1000G_gold_standard.indels.hg19.vcf.gz";

# perl script
my $split_index="$HPC_CHIP/bin/split_index.pl";
my $indel_phasing="$HPC_CHIP/bin/indel_phasing.pl";
my $bin_gc_ref="$HPC_CHIP/bin/bin_gc_ref.pl";
my $bam_to_chip_gender="$HPC_CHIP/bin/bam_to_chip_gender.pl";
my $depthQC="$HPC_CHIP/bin/depthQC_v2.0.pl";
my $depth="$HPC_CHIP/bin/depth_v2.0.pl";
my $CNV="$HPC_CHIP/bin/CNV_v2.0.pl";
my $vcf_anno="$BGICGA/bin/bgicg_anno.pl";
my $depart_annos="$BGICGA/bin/depart_annos_v2.pl";
my $excel_report="$BGICGA/bin/excel_report_v2.pl";

# R script
my $baseR="$HPC_CHIP/bin/base.R";
my $Q20Q30R="$HPC_CHIP/bin/Q20Q30.R";
my $qualityR="$HPC_CHIP/bin/quality.R";
my $disR="$HPC_CHIP/bin/dis.R";
my $cumnR="$HPC_CHIP/bin/cumn.R";
my $insertsizeR="$HPC_CHIP/bin/insertsize.R";

# path creat
my $rawdatadir="$outdir/fq/raw_data";
my $cleandatadir="$outdir/fq/clean_data";
my $aligndir="$outdir/alignment";
my $variationdir="$outdir/variation";
my $annotationdir="$outdir/annotation/variation";
my $docdir="$outdir/doc";
my $scriptdir="$outdir/script";
`mkdir -p $outdir` unless -d $outdir;
mkdir "$outdir/fq" unless -d "$outdir/fq";
mkdir $rawdatadir unless -d $rawdatadir;
mkdir $cleandatadir unless -d $cleandatadir;
mkdir $aligndir unless -d $aligndir;
mkdir $variationdir unless -d $variationdir;
mkdir "$outdir/annotation" unless -d "$outdir/annotation";
mkdir $annotationdir unless -d $annotationdir;
mkdir "$variationdir/snv" unless -d "$variationdir/snv";
mkdir "$variationdir/short_indel" unless -d "$variationdir/short_indel";
mkdir $docdir unless -d $docdir;
mkdir "$docdir/QC" unless -d "$docdir/QC";
mkdir $scriptdir unless -d $scriptdir;
`cp $list $docdir` unless abs_path(dirname($list)) eq $docdir;
`cp $config $docdir` unless abs_path(dirname($config)) eq $docdir;

`echo "#!/bin/bash" > $docdir/HPC_chip.sh`;
my $listfile=basename($list);
my $configfile=basename($config);
unlink "$docdir/$listfile.check" if -f "$docdir/$listfile.check";
`echo perl $0 -o $outdir -l $docdir/$listfile -c $docdir/$configfile $proj_para -s $step >> $docdir/HPC_chip.sh`;

my %sample_info;
my %lane;
my %gender;

my $log="$scriptdir/log";

&pharseSampleInfo();

foreach my $sample_name (keys %sample_info)
{
		my $shelldir; my $qcdir;
		$shelldir="$scriptdir/$sample_name";
		$qcdir="$docdir/QC/$sample_name";
		mkdir "$rawdatadir/$sample_name" unless -d "$rawdatadir/$sample_name";
		`mkdir -p $shelldir` unless -d $shelldir;
		mkdir "$aligndir/$sample_name" unless -d "$aligndir/$sample_name";
		`mkdir -p $qcdir` unless -d "$qcdir";
		foreach my $laneID (keys %{$sample_info{$sample_name}})
		{
			my $lane="lane-$laneID";
			$gender{$sample_name}=	$sample_info{$sample_name}->{$laneID}->{"gender"};;
			push(@{$lane{$sample_name}},$lane);
			mkdir "$qcdir/$lane" unless -d "$qcdir/$lane";
			my $fq1 = $sample_info{$sample_name}{$laneID}{"fq1"};
			my $fq2 = $sample_info{$sample_name}{$laneID}{"fq2"};
			my $rg = $sample_info{$sample_name}{$laneID}{"rg"};
			print $rg."\n";

			my $pu=(split(/_/,$lane))[-4];
			$pu||="NA";

			#filter low quality reads
			open(FT,">$shelldir/filter.$lane.sh") || die "$!";
			print FT "#!/bin/bash\n";
			print FT "export PATH=$HPC_CHIP/tools:\$PATH\n";
			print FT "if [ ! -d $cleandatadir/$sample_name/$lane ]; then\n\tmkdir -p $cleandatadir/$sample_name/$lane\nfi\n";
			if(defined $sample_info{$sample_name}{$laneID}->{"se"}){
				print FT "java -jar $fastqQC ".$config{"fastqQC"}." -1 $fq1 -o $cleandatadir/$sample_name/$lane -C $lane\_1.fq.gz\n";
			}else{
				print FT "java -jar $fastqQC ".$config{"fastqQC"}." -1 $fq1 -2 $fq2 -o $cleandatadir/$sample_name/$lane -C $lane\_1.fq.gz -D $lane\_2.fq.gz\n";
			}
			print FT "if [ \$? -ne 0 ]; then\n\techo \"$sample_name $lane filter failed.\" >> $log\n\texit 1\nfi\n";
			#print FT "mv $cleandatadir/$sample_name/$lane/*.txt $qcdir/$lane\n";
			#print FT "Rscript $baseR $qcdir/$lane/Base_distributions_by_read_position_1.txt $qcdir/$lane/Base_distributions_by_read_position_2.txt $qcdir/$lane/$sample_name_$lane\_raw_base.png $qcdir/$lane/$sample_name_$lane\_clean_base.png\n";
			#print FT "if [ \$? -ne 0 ]; then\n\techo \"$sample_name $lane base plot failed.\" >> $log\n\texit 1\nfi\n";
			#print FT "Rscript $Q20Q30R $qcdir/$lane/Distribution_of_Q20_Q30_bases_by_read_position_1.txt $qcdir/$lane/Distribution_of_Q20_Q30_bases_by_read_position_2.txt $qcdir/$lane/$sample_name_$lane\_Q20Q30.png\n";
			#print FT "if [ \$? -ne 0 ]; then\n\techo \"$sample_name $lane Q20Q30 plot failed.\" >> $log\n\texit 1\nfi\n";
			#print FT "Rscript $qualityR $qcdir/$lane/Base_quality_value_distribution_by_read_position_1.txt $qcdir/$lane/Base_quality_value_distribution_by_read_position_2.txt $qcdir/$lane/$sample_name_$lane\_raw_base_quality.png $qcdir/$lane/$sample_name_$lane\_clean_base_quality.png $trim1 $trim2\n";
			#print FT "if [ \$? -ne 0 ]; then\n\techo \"$sample_name $lane base quality plot failed.\" >> $log\n\texit 1\nfi\n";
			print FT "echo - | awk -v S=\$SECONDS '{printf \"$sample_name $lane filter\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
			print FT "exit 0\n";
			close FT;

			#bwa aln
			open(BA,">$shelldir/bwa.$lane.sh") || die "$!";
			print BA "#!/bin/bash\n";
			print BA "export PATH=$HPC_CHIP/tools:\$PATH\n";
			print BA "bwa aln ",$config{"bwa_aln"}," $hg19_fa{M} $cleandatadir/$sample_name/$lane/$lane\_1.fq.gz > $cleandatadir/$sample_name/$lane/$lane\_1.fq.gz.fai\n";
			print BA "if [ \$? -ne 0 ]; then\n\techo \"$sample_name $lane bwa aln fq1 failed.\" >> $log\n\texit 1\nfi\n";
			if(defined $sample_info{$sample_name}{$laneID}->{"se"}){
				print BA "bwa samse -r "."\"$rg";
				print BA "\\tPU:$pu" unless $pu eq "NA";
				print BA "\" $hg19_fa{M} $cleandatadir/$sample_name/$lane/$lane\_1.fq.gz.fai $cleandatadir/$sample_name/$lane/$lane\_1.fq.gz| samtools view -S -b -o $aligndir/$sample_name/$lane.raw.bam -\n";
				print BA "if [ \$? -ne 0 ]; then\n\techo \"$sample_name $lane bwa samse failed.\" >> $log\n\texit 1\nfi\n";

			}else{
				print BA "bwa aln ",$config{"bwa_aln"}," $hg19_fa{M} $cleandatadir/$sample_name/$lane/$lane\_2.fq.gz > $cleandatadir/$sample_name/$lane/$lane\_2.fq.gz.fai\n";
				print BA "if [ \$? -ne 0 ]; then\n\techo \"$sample_name $lane bwa aln fq2 failed.\" >> $log\n\texit 1\nfi\n";
				print BA "bwa sampe -r "."\"$rg";
				print BA "\\tPU:$pu" unless $pu eq "NA";
				print BA "\" $hg19_fa{M} $cleandatadir/$sample_name/$lane/$lane\_1.fq.gz.fai $cleandatadir/$sample_name/$lane/$lane\_2.fq.gz.fai $cleandatadir/$sample_name/$lane/$lane\_1.fq.gz $cleandatadir/$sample_name/$lane/$lane\_2.fq.gz | samtools view -S -b -o $aligndir/$sample_name/$lane.raw.bam -\n";
				print BA "if [ \$? -ne 0 ]; then\n\techo \"$sample_name $lane bwa sampe failed.\" >> $log\n\texit 1\nfi\n";
			}
			print BA "java -Djava.io.tmpdir=$outdir/javatmp -jar $picard/SortSam.jar INPUT=$aligndir/$sample_name/$lane.raw.bam OUTPUT=$aligndir/$sample_name/$lane.sort.bam SORT_ORDER=coordinate VALIDATION_STRINGENCY=SILENT\n";
			print BA "if [ \$? -ne 0 ]; then\n\techo \"$sample_name $lane SortSam failed.\" >> $log\n\texit 1\nfi\n";
			print BA "rm $aligndir/$sample_name/$lane.raw.bam\n";
			print BA "rm $cleandatadir/$sample_name/$lane/$lane\_1.fq.* $cleandatadir/$sample_name/$lane/$lane\_2.fq.*\n";
			print BA "echo - | awk -v S=\$SECONDS '{printf \"$sample_name $lane bwa\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";

			print BA "exit 0\n";
			close BA;
	}
}

foreach my $key2 (keys %sample_info){
	my ($shelldir,$qcdir,$snvdir,$indeldir,$vardir);
	$shelldir="$scriptdir/$key2";
	$qcdir="$docdir/QC/$key2";
	$snvdir="$variationdir/snv/$key2";
	$indeldir="$variationdir/short_indel/$key2";
	$vardir="$variationdir/$key2";

	#merge and index
	open(MG,">$shelldir/merge.sh") || die "$!";
	print MG "#!/bin/bash\n";
	print MG "export PATH=$HPC_CHIP/tools:\$PATH\n";
	if(scalar(@{$lane{$key2}})==1){
		print MG "mv $aligndir/$key2/$lane{$key2}[0].sort.bam $aligndir/$key2/$key2.final.bam\n";
	}else{
		print MG "java -Djava.io.tmpdir=$outdir/javatmp -jar $picard/MergeSamFiles.jar ";
		foreach (@{$lane{$key2}}){
			print MG "INPUT=$aligndir/$key2/$_.sort.bam ";
		}
		print MG "OUTPUT=$aligndir/$key2/$key2.final.bam VALIDATION_STRINGENCY=SILENT\n";
		print MG "if [ \$? -ne 0 ]; then\n\techo \"$key2 MergeSamFiles failed.\" >> $log\n\texit 1\nfi\n";
		print MG "rm $aligndir/$key2/*.sort.bam\n";
	}
	print MG "samtools index $aligndir/$key2/$key2.final.bam $aligndir/$key2/$key2.final.bai\n";
	print MG "if [ \$? -ne 0 ]; then\n\techo \"$key2 MergeSamFiles index failed.\" >> $log\n\texit 1\nfi\n";
	print MG "echo - | awk -v S=\$SECONDS '{printf \"$key2 merge and index\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
	print MG "exit 0\n";
	close MG;
	#duplication marking
	open(DP,">$shelldir/dupmark.sh") || die "$!";
	print DP "#!/bin/bash\n";
	print DP "export PATH=$HPC_CHIP/tools:\$PATH\n";
	print DP "java -Djava.io.tmpdir=$outdir/javatmp -jar $picard/MarkDuplicates.jar  MAX_FILE_HANDLES_FOR_READ_ENDS_MAP=8000 INPUT=$aligndir/$key2/$key2.final.bam OUTPUT=$aligndir/$key2/$key2.sort.dup.bam METRICS_FILE=$aligndir/$key2/$key2.dup.metrics VALIDATION_STRINGENCY=SILENT\n";
	print DP "if [ \$? -ne 0 ]; then\n\techo \"$key2 MarkDuplicates failed.\" >> $log\n\texit 1\nfi\n";
#	print DP "java -Djava.io.tmpdir=$outdir/javatmp -jar $picard/FixMateInformation.jar INPUT=$aligndir/$key2/$key2.sort.dup.bam OUTPUT=$aligndir/$key2/$key2.sort.dup.fix.bam VALIDATION_STRINGENCY=SILENT\n";
#	print DP "if [ \$? -ne 0 ]; then\n\techo \"$key2 FixMateInformation failed.\" >> $log\n\texit 1\nfi\n";
#	print DP "rm $aligndir/$key2/$key2.sort.dup.bam\n";
#	print DP "mv $aligndir/$key2/$key2.sort.dup.fix.bam $aligndir/$key2/$key2.final.bam\n";
	print DP "mv $aligndir/$key2/$key2.sort.dup.bam $aligndir/$key2/$key2.final.bam\n";
	print DP "samtools index $aligndir/$key2/$key2.final.bam $aligndir/$key2/$key2.final.bai\n";
	print DP "if [ \$? -ne 0 ]; then\n\techo \"$key2 MarkDuplicates index failed.\" >> $log\n\texit 1\nfi\n";
	print DP "mv $aligndir/$key2/$key2.dup.metrics $qcdir\n";
	print DP "echo - | awk -v S=\$SECONDS '{printf \"$key2 duplicate mark\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
	print DP "exit 0\n";
	close DP;
	#RealignerTargetCreator
	open(RTC,">$shelldir/RealignerTargetCreator.sh") || die "$!";
	print RTC "#!/bin/bash\n";
	print RTC "export PATH=$HPC_CHIP/tools:\$PATH\n";
	print RTC "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -I $aligndir/$key2/$key2.final.bam -known $gold_indels -T RealignerTargetCreator ",$config{"RTC"}," ";
	print RTC "-L $interval " if $interval;
	print RTC "-o $aligndir/$key2/$key2.realn_data.intervals\n";
	print RTC "if [ \$? -ne 0 ]; then\n\techo \"$key2 RealignerTargetCreator failed.\" >> $log\n\texit 1\nfi\n";
	print RTC "echo - | awk -v S=\$SECONDS '{printf \"$key2 RealignerTargetCreator\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
	print RTC "exit 0\n";
	close RTC;
	#IndelRealigner
	open(IR,">$shelldir/IndelRealigner.sh") || die "$!";
	print IR "#!/bin/bash\n";
	print IR "export PATH=$HPC_CHIP/tools:\$PATH\n";
	print IR "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -I $aligndir/$key2/$key2.final.bam -known $gold_indels  -T IndelRealigner -filterNoBases -targetIntervals $aligndir/$key2/$key2.realn_data.intervals -o $aligndir/$key2/$key2.sort.realn.bam\n";
	print IR "if [ \$? -ne 0 ]; then\n\techo \"$key2 IndelRealigner failed.\" >> $log\n\texit 1\nfi\n";
	print IR "rm $aligndir/$key2/$key2.realn_data.intervals\n";
	print IR "mv $aligndir/$key2/$key2.sort.realn.bam $aligndir/$key2/$key2.final.bam\n";
	print IR "mv $aligndir/$key2/$key2.sort.realn.bai $aligndir/$key2/$key2.final.bai\n";
	print IR "echo - | awk -v S=\$SECONDS '{printf \"$key2 IndelRealigner\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
	print IR "exit 0\n";
	close IR;
	#recalibration
	open(BR,">$shelldir/BaseRecalibrator.sh") || die "$!";
	print BR "#!/bin/bash\n";
	print BR "export PATH=$HPC_CHIP/tools:\$PATH\n";
	print BR "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -T BaseRecalibrator ",$config{"BR"}," ";
	print BR "-L $interval " if $interval;
	print BR "-I $aligndir/$key2/$key2.final.bam -knownSites $dbsnp -knownSites $gold_indels -o $aligndir/$key2/$key2.recal_data.grp\n";
	print BR "if [ \$? -ne 0 ]; then\n\techo \"$key2 BaseRecalibrator failed.\" >> $log\n\texit 1\nfi\n";
	print BR "echo - | awk -v S=\$SECONDS '{printf \"$key2 BaseRecalibrator\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
	print BR "exit 0\n";
	close BR;
	#PrintReads
	open(PR,">$shelldir/PrintReads.sh") || die "$!";
	print PR "#!/bin/bash\n";
	print PR "export PATH=$HPC_CHIP/tools:\$PATH\n";
	print PR "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -T PrintReads ",$config{"PR"}," -filterNoBases -I $aligndir/$key2/$key2.final.bam -BQSR $aligndir/$key2/$key2.recal_data.grp -o $aligndir/$key2/$key2.sort.realn.recal.bam\n";
	print PR "if [ \$? -ne 0 ]; then\n\techo \"$key2 PrintReads failed.\" >> $log\n\texit 1\nfi\n";
	print PR "rm $aligndir/$key2/$key2.recal_data.grp\n";
	print PR "mv $aligndir/$key2/$key2.sort.realn.recal.bam $aligndir/$key2/$key2.final.bam\n";
	print PR "mv $aligndir/$key2/$key2.sort.realn.recal.bai $aligndir/$key2/$key2.final.bai\n";
	print PR "echo - | awk -v S=\$SECONDS '{printf \"$key2 PrintReads\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
	print PR "exit 0\n";
	close PR;

	#variation calling
	if($config{"gatk_UnifiedGenotyper"} =~ /true/)
	{
		`mkdir -p $snvdir` unless -d $snvdir;
		`mkdir -p $indeldir` unless -d $indeldir;
		open(VR,">$shelldir/variation.sh") || die "$!";
		print VR "#!/bin/bash\n";
		print VR "export PATH=$HPC_CHIP/tools:\$PATH\n";
		print VR "if [ ! -d $vardir ]; then\n\tmkdir -p $vardir\nfi\n";
		print VR "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -T  UnifiedGenotyper";
		print VR " --intervals $interval " if $interval;
		print VR " -I $aligndir/$key2/$key2.final.bam -o $vardir/$key2.vcf --dbsnp $dbsnp ",$config{"gatk_genotyper"},"\n";
		print VR "if [ \$? -ne 0 ]; then\n\techo \"$key2 variation calling failed.\" >> $log\n\texit 1\nfi\n";
		print VR "echo - | awk -v S=\$SECONDS '{printf \"$key2 variation calling\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
		print VR "exit 0\n";
		close VR;
	}else
	{
		`mkdir -p $snvdir` unless -d $snvdir;
		`mkdir -p $indeldir` unless -d $indeldir;
		open(VR,">$shelldir/variation.sh") || die "$!";
		print VR "#!/bin/bash\n";
		print VR "export PATH=$HPC_CHIP/tools:\$PATH\n";
		print VR "if [ ! -d $vardir ]; then\n\tmkdir -p $vardir\nfi\n";
		print VR "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -l INFO -R $hg19_fa{M} -T HaplotypeCaller ";
		print VR "-L $interval " if $interval;
		print VR "-rf BadCigar -I $aligndir/$key2/$key2.final.bam -o $vardir/$key2.vcf -A AlleleBalance -A HaplotypeScore ",$config{"gatk_variation"},"\n";
		print VR "if [ \$? -ne 0 ]; then\n\techo \"$key2 variation calling failed.\" >> $log\n\texit 1\nfi\n";
		print VR "echo - | awk -v S=\$SECONDS '{printf \"$key2 variation calling\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
		print VR "exit 0\n";
		close VR;
	}

	#snv filter and phasing
	open(SF,">$shelldir/snv_filter.sh") || die "$!";
	print SF "#!/bin/bash\n";
	print SF "export PATH=$HPC_CHIP/tools:\$PATH\n";
	print SF "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -T SelectVariants ",$config{"gatk_variation_select"}," --variant $vardir/$key2.vcf -o $snvdir/$key2.snv.raw.vcf -selectType SNP\n";
	print SF "if [ \$? -ne 0 ]; then\n\techo \"$key2 select snv failed.\" >> $log\n\texit 1\nfi\n";
	print SF "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -T VariantFiltration --variant $snvdir/$key2.snv.raw.vcf -o $snvdir/$key2.snv.filter.vcf --filterExpression \"",$config{"snv_filter"},"\" --filterName \"StandardFilter\"\n";
	print SF "if [ \$? -ne 0 ]; then\n\techo \"$key2 snv filter failed.\" >> $log\n\texit 1\nfi\n";
	print SF "rm $snvdir/$key2.snv.raw.vcf $snvdir/$key2.snv.raw.vcf.idx\n";
	print SF "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -T ReadBackedPhasing -I $aligndir/$key2/$key2.final.bam --variant $snvdir/$key2.snv.filter.vcf -o $snvdir/$key2.snv.vcf\n";
	print SF "if [ \$? -ne 0 ]; then\n\techo \"$key2 snv phasing failed.\" >> $log\n\texit 1\nfi\n";
	print SF "rm $snvdir/$key2.snv.filter.vcf $snvdir/$key2.snv.filter.vcf.idx\n";
	print SF "echo - | awk -v S=\$SECONDS '{printf \"$key2 snv filter and phasing\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
	print SF "exit 0\n";
	close SF;
	#indel filter
	open(IF,">$shelldir/indel_filter.sh") || die "$!";
	print IF "#!/bin/bash\n";
	print IF "export PATH=$HPC_CHIP/tools:\$PATH\n";
	print IF "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -T SelectVariants ",$config{"gatk_variation_select"}," --variant $vardir/$key2.vcf -o $indeldir/$key2.indel.raw.vcf -selectType INDEL\n";
	print IF "if [ \$? -ne 0 ]; then\n\techo \"$key2 select indel failed.\" >> $log\n\texit 1\nfi\n";
	print IF "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -T VariantFiltration --variant $indeldir/$key2.indel.raw.vcf -o $indeldir/$key2.indel.vcf --filterExpression \"",$config{"indel_filter"},"\" --filterName \"StandardFilter\"\n";
	print IF "if [ \$? -ne 0 ]; then\n\techo \"$key2 indel filter failed.\" >> $log\n\texit 1\nfi\n";
	print IF "rm $indeldir/$key2.indel.raw.vcf $indeldir/$key2.indel.raw.vcf.idx\n";
	print IF "echo - | awk -v S=\$SECONDS '{printf \"$key2 indel filter\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
	print IF "exit 0\n";
	close IF;
	#variation combine
	open(VC,">$shelldir/variation_combine.sh") || die "$!";
	print VC "#!/bin/bash\n";
	print VC "export PATH=$HPC_CHIP/tools:\$PATH\n";
	print VC "java -Djava.io.tmpdir=$outdir/javatmp -jar $gatk -R $hg19_fa{M} -T CombineVariants ",$config{"gatk_variation_select"}," --variant $snvdir/$key2.snv.vcf --variant $indeldir/$key2.indel.vcf -genotypeMergeOptions UNSORTED -o $vardir/$key2.filter.vcf\n";
	print VC "if [ \$? -ne 0 ]; then\n\techo \"$key2 variation combine failed.\" >> $log\n\texit 1\nfi\n";
	print VC "perl $indel_phasing -b $aligndir/$key2/$key2.final.bam -v $vardir/$key2.filter.vcf -o $vardir/$key2.final.vcf\n";
	print VC "if [ \$? -ne 0 ]; then\n\techo \"$key2 indel phasing failed.\" >> $log\n\texit 1\nfi\n";
	print VC "bgzip -f $snvdir/$key2.snv.vcf\n";
	print VC "if [ \$? -ne 0 ]; then\n\techo \"$key2 snv bgzip failed.\" >> $log\n\texit 1\nfi\n";
	print VC "rm $snvdir/$key2.snv.vcf.idx\n";
	print VC "tabix -f -p vcf $snvdir/$key2.snv.vcf.gz\n";
	print VC "if [ \$? -ne 0 ]; then\n\techo \"$key2 snv tabix failed.\" >> $log\n\texit 1\nfi\n";
	print VC "bgzip -f $indeldir/$key2.indel.vcf\n";
	print VC "if [ \$? -ne 0 ]; then\n\techo \"$key2 indel bgzip failed.\" >> $log\n\texit 1\nfi\n";
	print VC "rm $indeldir/$key2.indel.vcf.idx\n";
	print VC "tabix -f -p vcf $indeldir/$key2.indel.vcf.gz\n";
	print VC "if [ \$? -ne 0 ]; then\n\techo \"$key2 indel tabix failed.\" >> $log\n\texit 1\nfi\n";
	print VC "rm $vardir/$key2.vcf $vardir/$key2.vcf.idx\n";
	print VC "bgzip -f $vardir/$key2.final.vcf\n";
	print VC "if [ \$? -ne 0 ]; then\n\techo \"$key2 final vcf bgzip failed.\" >> $log\n\texit 1\nfi\n";
	print VC "rm $vardir/$key2.filter.vcf $vardir/$key2.filter.vcf.idx\n";
	print VC "tabix -f -p vcf $vardir/$key2.final.vcf.gz\n";
	print VC "if [ \$? -ne 0 ]; then\n\techo \"$key2 final vcf tabix failed.\" >> $log\n\texit 1\nfi\n";
	print VC "echo - | awk -v S=\$SECONDS '{printf \"$key2 variation combine\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
	print VC "exit 0\n";
	close VC;
	#vcf anno for single sample
	if($config{"run_type"} eq "single"){
		mkdir "$annotationdir/$key2" unless -d "$annotationdir/$key2";
		open(AN,">$shelldir/vcf_anno.sh") || die "$!";
		print AN "#!/bin/bash\n";
		print AN "export PATH=$HPC_CHIP/tools:\$PATH\n";
		print AN "export HPC_CHIP_HOME=$HPC_CHIP\n";
		print AN "export BGICGA_HOME=$BGICGA\n";
		#print AN "export PERL5OPT=\"-I/ifs2/S2PH/shiquan/local/ActivePerl-5.16/site/lib -I/ifs4/Gaea/data_management/PerlPackages/lib\"\n";
		#print AN "export PERL5LIB=$HPC_CHIP/lib:/ifs2/S2PH/shiquan/local/ActivePerl-5.16/site/lib\n";
		print AN "perl $vcf_anno $docdir/$configfile $vardir/$key2.final.vcf.gz -t vcf -r $annotationdir/$key2/$key2.vcf.anno.header.format.tsv ",$config{"anno"}," 2> $docdir/QC/$key2/$key2\_anno.log | perl $depart_annos -p $annotationdir/$key2/\n";
		print AN "if [ \$? -ne 0 ]; then\n\techo \"$key2 vcf anno failed.\" >> $log\n\texit 1\nfi\n";
		print AN "echo - | awk -v S=\$SECONDS '{printf \"$key2 vcf anno\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
		print AN "exit 0\n";
		close AN;
		#excel report
		open(EX,">$shelldir/excel_report.sh") || die "$!";
		print EX "#!/bin/bash\n";
		print EX "export PATH=$HPC_CHIP/tools:\$PATH\n";
		print EX "if [ -f $docdir/CNV/$key2.CNV.out ]; then\n";
		print EX "\tperl $excel_report -v $annotationdir/$key2/$key2.bed.gz -f $annotationdir/$key2/$key2.vcf.anno.header.format.tsv -r ",$config{"filter_rules"}," -u $docdir/QC/$key2/report/uncover_target.txt -c $docdir/CNV/$key2.CNV.out -o $annotationdir/$key2/$key2\_vcfanno.xlsm -b $BGICGA/bin/vbaProject.bin -s ",$config{"headers"},"\n";
		print EX "else\n\tperl $excel_report -v $annotationdir/$key2/$key2.bed.gz -f $annotationdir/$key2/$key2.vcf.anno.header.format.tsv -r ",$config{"filter_rules"}," -u $docdir/QC/$key2/report/uncover_target.txt -o $annotationdir/$key2/$key2\_vcfanno.xlsm -b $BGICGA/bin/vbaProject.bin -s ",$config{"headers"},"\nfi\n";
		print EX "if [ \$? -ne 0 ]; then\n\techo \"$key2 excel report failed.\" >> $log\n\texit 1\nfi\n";
		print EX "echo - | awk -v S=\$SECONDS '{printf \"$key2 excel report\\t%02d:%02d:%02d\\n\",S/(60*60),S%(60*60)/60,S%60}' >> $log\n";
		print EX "exit 0\n";
		close EX;
	}
}


#pharse sample list
sub pharseSampleInfo
{
    $list = abs_path($list);
	my %parameter;
	my $test=0;
	my $total_number = 0;
	my $female_total_number = 0;
	my $male_total_number = 0;
	my $sample_lane_counter = 0;
	$parameter{"male_counter"} = 0;
	$parameter{"female_counter"} = 0;
	open FQL, "$list" or die "[err]can't open $list:$!";
	while(<FQL>)
	{
		chomp;
		next if (/^\#/);
		next if (/^\s*$/);
		my @tmp = split(/\s+/);
		
		my $sample_name;
		if(/^\>(\S+)/i)
		{
			$sample_name = $1;
			if(!exists $sample_info{$sample_name})
			{
				$sample_lane_counter = 0;
			}
			else
			{
				$sample_lane_counter++;
			}
			while(<FQL>)
			{
				chomp($_);
				next if (/^\#/);
				next if (/^\s*$/);
				if(/^(\S+)\s*=\s*(\S+)/i)
				{
					$sample_info{$sample_name}->{$sample_lane_counter}->{"$1"} = $2;
				}
				if(/^>\s*$/)
				{
					last;
				}
			}
			

			my $gender = $sample_info{$sample_name}->{$sample_lane_counter}->{"gender"};
			if($gender)
			{
				if($gender eq "female" || $gender eq "F")
				{
					$female_total_number++;
					$sample_info{$sample_name}->{$sample_lane_counter}->{"id"} = $female_total_number - 1;
					if($sample_lane_counter == 0)
					{
						$parameter{"female_counter"}++;
					}
					$sample_info{$sample_name}->{$sample_lane_counter}->{"gender"} = "female";
				}
				if($gender eq "male" || $gender eq "M")
				{
					$male_total_number++;
					$sample_info{$sample_name}->{$sample_lane_counter}->{"id"} = $male_total_number - 1;
					if($sample_lane_counter == 0)
					{
						$parameter{"male_counter"}++;
					}
					$sample_info{$sample_name}->{$sample_lane_counter}->{"gender"} = "male";
				}
			}
			else
			{
				$sample_info{$sample_name}->{$sample_lane_counter}->{"id"} = $total_number - 1;
			}
		}

	}
	if(!%sample_info) 
	{
		errLog("sample info do not exists!");
	}
	my %fastq_property = ("id"=>0, "fq1"=>0, "fq2"=>0, "adp1"=>0, "adp2"=>0, "gender"=>0, "family"=>0, "type"=>0, "pool"=>0, "rg"=>0, "libname"=>0, "insert_size"=>0);
	my @fastq_necessary_property = ("id", "fq1", "rg", "gender");
	my @fastq_file_property = ("fq1", "fq2");

	foreach my $sample_name (keys %sample_info)
	{
		logs("sample: $sample_name");
		my $sample_gender;
		foreach my $lane_num (keys %{$sample_info{$sample_name}})
		{	
			if(!$sample_gender)
			{
				$sample_gender = $sample_info{$sample_name}{$lane_num}->{"gender"};
			}
			else
			{
				if($sample_gender ne $sample_info{$sample_name}{$lane_num}->{"gender"})
				{
					errLog("gender in $sample_name is different in each lane!");
				}
			}
			foreach my $property (@fastq_necessary_property)
			{
				if(!exists $sample_info{$sample_name}{$lane_num}->{$property})
				{
					errLog("fastq prperty: $property is not exists in the lane $lane_num of $sample_name. You must set $property in your sample file.");
				}
			}
			
			foreach my $property(keys %{$sample_info{$sample_name}{$lane_num}})
			{
				if(!exists $fastq_property{$property})
				{
					errLog("$property in lane $lane_num of $sample_name is not a standard property of fastq. Please check of your sample file.");
				}
			}
			
			foreach my $property (@fastq_file_property)
			{
				if(exists $sample_info{$sample_name}{$lane_num}->{$property} && !-e $sample_info{$sample_name}{$lane_num}->{$property})
				{
					errLog("$property file in the lane $lane_num of $sample_name: $sample_info{$sample_name}{$lane_num}->{$property} is not exists. Please check of your sample file.");
				}
			}
			
			#check fq2 or is se
			if(!defined $sample_info{$sample_name}{$lane_num}->{"fq2"})
			{
				logs("lane: $sample_name\_$lane_num SEdata");
				$sample_info{$sample_name}{$lane_num}->{"se"} = 1;
				$sample_info{$sample_name}{$lane_num}->{"fq2"} = "null";
			}
		}
	}
}

sub errLog
{
	my $message = shift;
	print STDERR "[ERROR] $message\n";
	exit;
}

sub warnLog
{
	my $message = shift;
	print STDERR "[WARN] $message\n";
}

sub logs
{
	my $message = shift;
	print STDOUT "[INFO] $message\n";
}

sub mkdirs
{
	my $dir = shift;
	system("mkdir -p $dir") if(!-d $dir);
}

#qsub: write run.pl
open RUN,">$scriptdir/run.pl";
foreach my $sample_name (keys %sample_info)
{
	my $file;
	my $merge_job = "";
	if($step=~/a/)
	{
		print RUN "foreach my \$file (glob(\"$scriptdir/$sample_name/filter.*.sh\")){\n";
		print RUN "\tmy\$lane = `basename \$file`;\n";
		print RUN "\tmy \$jobinfo=`qsub -l vf=2G  $proj_para -e \$file.err -o \$file.out \$file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{\$lane.\"$sample_name\_filter\"}.=\",\$1\";\n\t}\n}\n"; 
		my $filter_job = "-hold_jid \$job{\$lane.\"$sample_name\_filter\"}";      

		print RUN "\nforeach my \$file (glob(\"$scriptdir/$sample_name/bwa.*.sh\")){\n";
		print RUN "\tmy\$lane = `basename \$file`;\n";
		print RUN "\t\$lane =~ s/bwa/filter/;\n";
		print RUN "\tmy \$jobinfo=`qsub -l vf=5G  $proj_para $filter_job -e \$file.err -o \$file.out \$file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{\"$sample_name\_bwa\"}.=\",\$1\";\n\t}\n}\n"; 
		my $bwa_job = "-hold_jid \$job{\"$sample_name\_bwa\"}";

		$file = "$scriptdir/$sample_name/merge.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=5G  $proj_para $bwa_job -e $file.err -o $file.out $file`;\n";
		print RUN "if(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\$job{\"$sample_name\_merge\"}=\$1;\n}\n"; 
		$merge_job = "-hold_jid \$job{\"$sample_name\_merge\"}";
	}

	my $dupmark_job = $merge_job;
	if($step=~/d/)
	{
		$file = "$scriptdir/$sample_name/dupmark.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=3G  $proj_para $merge_job -e $file.err -o $file.out $file`;\n";
		print RUN "if(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\$job{\"$sample_name\_dupmark\"}=\$1;\n}\n"; 
		$dupmark_job = "-hold_jid \$job{\"$sample_name\_dupmark\"}";
	}

	my $realignIndel_job = $dupmark_job;
	if($step=~/r/)
	{
		$file = "$scriptdir/$sample_name/RealignerTargetCreator.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=5G  $proj_para $dupmark_job -e $file.err -o $file.out $file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{\"$sample_name\_realignTarget\"}=\$1;\n\t}\n"; 
		my $realignTarget_job = "-hold_jid \$job{\"$sample_name\_realignTarget\"}";

		$file = "$scriptdir/$sample_name/IndelRealigner.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=5G  $proj_para $realignTarget_job -e $file.err -o $file.out $file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{$sample_name.\"_realignIndel\"}=\$1;\n\t}\n"; 
		$realignIndel_job = "-hold_jid \$job{$sample_name.\"_realignIndel\"}";
	}

	my $PrintReads_job = $realignIndel_job;
	if($step=~/b/)
	{
		$file = "$scriptdir/$sample_name/BaseRecalibrator.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=5G  $proj_para $realignIndel_job -e $file.err -o $file.out $file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{$sample_name.\"_BaseRecal\"}=\$1;\n\t}\n"; 
		my $BaseRecal_job = "-hold_jid \$job{$sample_name.\"_BaseRecal\"}";

		$file = "$scriptdir/$sample_name/PrintReads.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=5G  $proj_para $BaseRecal_job -e $file.err -o $file.out $file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{$sample_name.\"_PrintReads\"}=\$1;\n\t}\n"; 
		$PrintReads_job = "-hold_jid \$job{$sample_name.\"_PrintReads\"}";
	}
    my $variation_job = "";
	if($step=~/v/)
	{
		$file = "$scriptdir/$sample_name/variation.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=5G  $proj_para $PrintReads_job -e $file.err -o $file.out $file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{$sample_name.\"_variation\"}=\$1;\n\t}\n"; 
		$variation_job = "-hold_jid \$job{$sample_name.\"_variation\"}";
	}

	my $Variation_combine_job = "";
	if($step=~/f/){

		$file = "$scriptdir/$sample_name/indel_filter.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=2G  $proj_para $variation_job -e $file.err -o $file.out $file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{$sample_name.\"_indel_filter\"}=\$1;\n\t}\n"; 
		my $indel_filter_job = "-hold_jid \$job{$sample_name.\"_indel_filter\"}";

		$file = "$scriptdir/$sample_name/snv_filter.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=2G  $proj_para $indel_filter_job -e $file.err -o $file.out $file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{$sample_name.\"_snv_filter\"}=\$1;\n\t}\n"; 
		my $snv_filter_job = "-hold_jid \$job{$sample_name.\"_snv_filter\"}";

		$file = "$scriptdir/$sample_name/variation_combine.sh";
		print RUN "\n\$jobinfo=`qsub -l vf=3G  $proj_para $snv_filter_job -e $file.err -o $file.out $file`;\n";
		print RUN "\tif(\$jobinfo=~/^Your job (\\d+) \\(\\\"(.*?)\\\"\\) has been submitted\$/){\n";
		print RUN "\t\t\$job{$sample_name.\"_variation_combine\"}=\$1;\n\t}\n"; 
		$Variation_combine_job = "-hold_jid \$job{$sample_name.\"_variation_combine\"}";
	}
}
