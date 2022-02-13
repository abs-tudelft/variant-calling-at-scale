git clone https://github.com/samtools/htslib.git
cd htslib/
git submodule update --init --recursive
autoreconf -i
./configure
make

wget https://github.com/samtools/samtools/releases/download/1.12/samtools-1.12.tar.bz2
tar -xvjf samtools-1.12.tar.bz2 
cd samtools-1.12/
./configure #--prefix=/usr/local/bin/
make

cd ..
git clone https://github.com/lh3/bwa.git
cd bwa; make

cd /scratch-shared/tahmad/bio_data/short/HG002

wget https://cgl.gi.ucsc.edu/data/aws_staging/HG002.novaseq.pcr-free.35x.R1.fastq.gz
wget https://cgl.gi.ucsc.edu/data/aws_staging/HG002.novaseq.pcr-free.35x.R2.fastq.gz

gunzip HG002.novaseq.pcr-free.35x.R1.fastq.gz
gunzip HG002.novaseq.pcr-free.35x.R2.fastq.gz

time ${PATH}/bwa index /scratch-shared/tahmad/bio_data/reference/GRCh38_no_alt_analysis_set.fasta

time ${PATH}/bwa mem -t $(nproc) -R '@RG\tID:sample_lane\tSM:sample\tPL:illumina\tLB:sample\tPU:lane' /scratch-shared/tahmad/bio_data/reference/GRCh38_no_alt_analysis_set.fasta /scratch-shared/tahmad/bio_data/short/HG002/HG002.novaseq.pcr-free.35x.R1.fastq /scratch-shared/tahmad/bio_data/short/HG002/HG002.novaseq.pcr-free.35x.R2.fastq > /scratch-shared/tahmad/bio_data/short/HG002/out.sam

time ${PATH}/samtools sort -@ $(nproc) -o /scratch-shared/tahmad/bio_data/short/HG002/out.bam /scratch-shared/tahmad/bio_data/short/HG002/out.sam

time ${PATH}/samtools index -@ $(nproc) /scratch-shared/tahmad/bio_data/short/HG002/out.bam

time ${PATH}/sambamba markdup -t $(nproc) -r /scratch-shared/tahmad/bio_data/short/HG002/out.bam /scratch-shared/tahmad/bio_data/short/HG002/out_md.bam

singularity exec ${PATH}/octopus_latest.sif octopus --threads  $(nproc)  --reference /scratch-shared/tahmad/bio_data/reference/GRCh38_no_alt_analysis_set.fasta  --reads /scratch-shared/tahmad/bio_data/short/HG002/out.bam -T chr1 to chrM --sequence-error-model PCRF.NovaSeq --forest /opt/octopus/resources/forests/germline.v0.7.4.forest -o /scratch-shared/tahmad/bio_data/short/HG002/octopus_without_md.vcf.gz
