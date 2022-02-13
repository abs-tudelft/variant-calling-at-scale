
mkdir -p singularity
cd singularity

singularity pull docker://jmcdani20/hap.py:v0.3.12
singularity pull docker://dancooke/octopus
cd ..

mkdir -p tools
cd tools

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
cd ..

git clone --recursive https://github.com/biod/sambamba.git
cd sambamba; make
cd ..

mkdir -p reference
cd reference

ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz
gunzip GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.gz > GRCh38_no_alt_analysis_set.fasta
ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/405/GCA_000001405.15_GRCh38/seqs_for_alignment_pipelines.ucsc_ids/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.fai
mv GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.fai GRCh38_no_alt_analysis_set.fasta.fai
cd ..

mkdir -p HG002
cd HG002

wget https://cgl.gi.ucsc.edu/data/aws_staging/HG002.novaseq.pcr-free.35x.R1.fastq.gz
wget https://cgl.gi.ucsc.edu/data/aws_staging/HG002.novaseq.pcr-free.35x.R2.fastq.gz

gunzip HG002.novaseq.pcr-free.35x.R1.fastq.gz
gunzip HG002.novaseq.pcr-free.35x.R2.fastq.gz
cd ..

time ${PATH}/tools/bwa index reference/GRCh38_no_alt_analysis_set.fasta

time ${PATH}/tools/bwa mem -t $(nproc) -R '@RG\tID:sample_lane\tSM:sample\tPL:illumina\tLB:sample\tPU:lane' reference/GRCh38_no_alt_analysis_set.fasta HG002/HG002.novaseq.pcr-free.35x.R1.fastq HG002/HG002.novaseq.pcr-free.35x.R2.fastq > HG002/out.sam

time ${PATH}/tools/samtools sort -@ $(nproc) -o HG002/out.bam HG002/out.sam

time ${PATH}/tools/samtools index -@ $(nproc) HG002/out.bam

time ${PATH}/tools/sambamba markdup -t $(nproc) -r HG002/out.bam HG002/out_md.bam

singularity exec ${PATH}/octopus_latest.sif octopus --threads  $(nproc)  --reference reference/GRCh38_no_alt_analysis_set.fasta  --reads HG002/out_md.bam -T chr1 to chrM --sequence-error-model PCRF.NovaSeq --forest /opt/octopus/resources/forests/germline.v0.7.4.forest -o HG002/octopus_md.vcf.gz

mkdir -p benchmark
cd benchmark

# Download truth VCFs
wget ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/release/AshkenazimTrio/HG003_NA24149_father/NISTv4.2.1/GRCh38/HG003_GRCh38_1_22_v4.2.1_benchmark_noinconsistent.bed
wget ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/release/AshkenazimTrio/HG003_NA24149_father/NISTv4.2.1/GRCh38/HG003_GRCh38_1_22_v4.2.1_benchmark.vcf.gz
wget ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/release/AshkenazimTrio/HG003_NA24149_father/NISTv4.2.1/GRCh38/HG003_GRCh38_1_22_v4.2.1_benchmark.vcf.gz.tbi

mkdir -p happy
time singularity exec ${PATH}/hap.py_v0.3.12.sif /opt/hap.py/bin/hap.py         --threads 8         -r reference/GRCh38_no_alt_analysis_set.fasta         -f benchmark/HG002_GRCh38_1_22_v4.2.1_benchmark_noinconsistent.bed         -o giab-comparison.v4.2.merge_pass         --engine=vcfeval        --pass-only -l chr20       benchmark/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz         HG002/octopus_without_md.vcf.gz
