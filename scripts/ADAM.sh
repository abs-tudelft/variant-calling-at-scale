#!/bin/bash 

#SBATCH -p gpu
#SBATCH --job-name=spark-multi-node

#Number of seperate nodes reserved for Spark cluster
#SBATCH --nodes=3

#Number of excecution slots

 
#SBATCH -t 1:10:00                  #job to run for 10 minutes
#SBATCH --output=bd.out
#SBATCH --mem=8G  

############################
#module load pre2019
#module load python                 # load python,spark, and java modules
#module load java/oracle/8u73
############################
#conda activate pyarrow-dev
#source activate pyarrow-dev

#SPARK_HOME=~/tahmad/spark
#export PATH=$SPARK_HOME/bin:$PATH

#export ARROW_HOME=~/tools
#export LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH

#M2_HOME=~/tahmad/tools/apache-maven-3.6.3
#export PATH=$M2_HOME/bin:$PATH

#JAVA_HOME=~/tahmad/tools/jdk-14.0.1
#export PATH=$JAVA_HOME/bin:$PATH

export SPARK_HOME=/home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7
#chmod -R a+rwx $SPARK_HOME

'''
#SBATCH --ntasks-per-node 1   #4 tasks per node
#SBATCH --cpus-per-task 16       #4 cpus per task
'''

source /home/tahmad/tahmad/del.txt
#source run.txt
#rm -r /home/tahmad/tahmad/outputs
#####################################################################################
nodes=($(scontrol show hostname $SLURM_NODELIST))
nnodes=${#nodes[@]}
last=$(( $nnodes - 1 ))

#sed -i 's/tcn768/'"${nodes[0]}"'/g' ~/tahmad/SparkGA2/config/config.xml

ssh ${nodes[0]} hostname
echo -n "starting spark master on $MASTER... ";
#singularity exec ~/tahmad/spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/sbin/start-master.sh 
#singularity exec ~/tahmad/spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/sbin/start-master.sh
/home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/sbin/start-master.sh

PORT=7077
export MASTER="spark://${nodes[0]}:$PORT"
echo -n "starting spark master on $MASTER... ";
echo "done";
sleep 2;
echo "spark cluster web interface: http://tcn753:8080"  >$HOME/spark-info
echo "           spark master URL: spark://tcn753:7077" >>$HOME/spark-info
#singularity exec ~/tahmad/spark.simg plasma_store_server -m 3000000000 -s /tmp/store0 &
export MASTER=$MASTER

echo starting on-node worker
#singularity exec ~/tahmad/spark.simg nohup /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/sbin/start-slave.sh -c 16 -m 64G ${MASTER}

echo opening ssh connections to start the other nodes worker processeses
i=0
for i in $( seq 1 $last )
do
    ssh ${nodes[i]} hostname
done

echo starting remote workers
'''
for i in $( seq 1 $last )
do
   /usr/bin/ssh ${nodes[$i]} "singularity exec ~/tahmad/spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/sbin/start-slave.sh -c 24 -m 60G ${MASTER}; echo ${nodes[$i]} " &
done
'''

for i in $( seq 1 $last )
do
   /usr/bin/ssh ${nodes[$i]} "/home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/sbin/start-slave.sh -c 16 -m 8G ${MASTER}; echo ${nodes[$i]} " &
done

'''
for i in $( seq 1 $last )
do
   /usr/bin/ssh ${nodes[$i]} "singularity exec ~/tahmad/spark.simg plasma_store_server -m 3000000000 -s /tmp/store0 & " &
done
'''
sleep 25;
#######################################################################################
#singularity exec ~/tahmad/spark.simg plasma_store_server -m 30000 -s /tmp/store0 &
#singularity exec spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/bin/spark-submit --version
#singularity exec spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/bin/spark-submit /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/examples/src/main/python/pi.py 50
#singularity exec spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client  --driver-memory 12g --executor-memory 16g --num-executors 4 --executor-cores 16 /home/tahmad/tahmad/script.py

#singularity exec spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=1800000  --conf spark.executor.heartbeatInterval=18000  --driver-memory 50g --executor-memory 30g --num-executors 32 --executor-cores 24 /home/tahmad/tahmad/script.py 
#source del.txt

#singularity exec spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=1800000  --conf spark.executor.heartbeatInterval=18000  --driver-memory 50g --executor-memory 30g --num-executors 32 --executor-cores 24 /home/tahmad/tahmad/script.py
#source del.txt

#singularity exec spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=1800000  --conf spark.executor.heartbeatInterval=18000  --driver-memory 50g --executor-memory 30g --num-executors 32 --executor-cores 24 /home/tahmad/tahmad/script.py
#source del.txt

#singularity exec spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=1800000  --conf spark.executor.heartbeatInterval=18000  --driver-memory 50g --executor-memory 30g --num-executors 32 --executor-cores 24 /home/tahmad/tahmad/script.py
#source del.txt

#singularity exec spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=1800000  --conf spark.executor.heartbeatInterval=18000  --driver-memory 50g --executor-memory 30g --num-executors 32 --executor-cores 24 /home/tahmad/tahmad/script.py
#source del.txt

#singularity exec spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=1800000  --conf spark.executor.heartbeatInterval=18000  --driver-memory 50g --executor-memory 30g --num-executors 32 --executor-cores 24 /home/tahmad/tahmad/script.py
#source del.txt

#/home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=1800000  --conf spark.executor.heartbeatInterval=18000  --driver-memory 5g --executor-memory 10g --num-executors 2 --executor-cores 24 /home/tahmad/tahmad/script.py


rm -r /home/tahmad/tahmad/bd/sample.alignments.adam
rm -r /home/tahmad/mcx/bio_data/gcat_025.adam
#rm /home/tahmad/mcx/bio_data/gcat_025.fastq
rm -r /home/tahmad/mcx/bio_data/ERR001268_400.adam

#/home/tahmad/tahmad/bd/cannoli/bin/cannoli-submit interleaveFastq /scratch-shared/tahmad/bio_data/ERR001268/ERR001268_1.filt.fastq.gz /scratch-shared/tahmad/bio_data/ERR001268/ERR001268_2.filt.fastq.gz /scratch-shared/tahmad/bio_data/ERR001268/ERR001268.fastq


#/home/tahmad/tahmad/bd/cannoli/bin/cannoli-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=18000000  --conf spark.storage.blockManagerSlaveTimeoutMs=18000000 --conf spark.executor.heartbeatInterval=18000000 --driver-memory 20g --executor-memory 50g --num-executors 2 --executor-cores 24 -- bwaMem -executable "/home/tahmad/tahmad/BWA/new/bwa/bwa" /home/tahmad/mcx/bio_data/gcat_025.fastq /home/tahmad/mcx/bio_data/gcat_025.adam -index /home/tahmad/mcx/bio_data/ucsc.hg19.fasta -sample_id sample -sequence_dictionary /home/tahmad/mcx/bio_data/ucsc.hg19.dict -force_load_ifastq -fragments -add_files

#rm -r /home/tahmad/mcx/bio_data/gcat_025.adam

#/home/tahmad/tahmad/bd/cannoli/bin/cannoli-submit --master spark://${nodes[0]}:7077 --deploy-mode client -- bwaMem -executable "/home/tahmad/tahmad/BWA/new/bwa/bwa" /home/tahmad/mcx/bio_data/gcat_025.fastq /home/tahmad/mcx/bio_data/gcat_025.adam -index /home/tahmad/mcx/bio_data/ucsc.hg19.fasta -sample_id sample -sequence_dictionary /home/tahmad/mcx/bio_data/ucsc.hg19.dict -force_load_ifastq -fragments -add_files

#rm -r /home/tahmad/mcx/bio_data/gcat_025.adam

#/home/tahmad/tahmad/bd/cannoli/bin/cannoli-submit bwaMem -executable "/home/tahmad/tahmad/BWA/new/bwa/bwa" /home/tahmad/mcx/bio_data/gcat_025.fastq /home/tahmad/mcx/bio_data/gcat_025.adam -index /home/tahmad/mcx/bio_data/ucsc.hg19.fasta -sample_id sample -sequence_dictionary /home/tahmad/mcx/bio_data/ucsc.hg19.dict -force_load_ifastq -fragments -add_files

#rm -r /home/tahmad/mcx/bio_data/gcat_025_sb.adam

#/home/tahmad/tahmad/bd/cannoli/bin/cannoli-submit -- bwaMem -executable "/home/tahmad/tahmad/BWA/new/bwa/bwa" -index /home/tahmad/mcx/bio_data/ucsc.hg19.fasta /home/tahmad/mcx/bio_data/gcat_025.fastq /home/tahmad/mcx/bio_data/gcat_025_sb.adam -sample_id sample  

rm -r /scratch-shared/tahmad/bio_data/ERR001268/ERR001268.adam
##/home/tahmad/tahmad/bd/cannoli/bin/cannoli-submit --master spark://${nodes[0]}:7077 --deploy-mode client -- bwaMem -executable "/home/tahmad/tahmad/BWA/new/bwa/bwa" -index /home/tahmad/mcx/bio_data/ucsc.hg19.fasta /scratch-shared/tahmad/bio_data/ERR001268/ERR001268.fastq /scratch-shared/tahmad/bio_data/ERR001268/ERR001268.adam -sample_id sample -force_load_ifastq

#/home/tahmad/tahmad/bd/cannoli/bin/cannoli-submit --master spark://${nodes[0]}:7077 --deploy-mode client --num-executors 2 --executor-cores 24 --driver-memory 10g --executor-memory 10g --  bwaMem -executable "/home/tahmad/tahmad/BWA/new/bwa/bwa" /home/tahmad/mcx/bio_data/ERR001268_400.fastq /home/tahmad/mcx/bio_data/ERR001268_400.adam -index /home/tahmad/mcx/bio_data/ucsc.hg19.fasta -sample_id sample -sequence_dictionary /home/tahmad/mcx/bio_data/ucsc.hg19.dict -force_load_ifastq -fragments -add_files

#/home/tahmad/tahmad/bd/cannoli/bin/cannoli-submit --driver-memory 10g --executor-memory 10g --num-executors 4 --executor-cores 24 interleaveFastq  /home/tahmad/mcx/bio_data/gcat_025_1a.fastq /home/tahmad/mcx/bio_data/gcat_025_2a.fastq  /home/tahmad/mcx/bio_data/gcat_025_a.fastq

#/home/tahmad/tahmad/bd/cannoli/bin/cannoli-submit --driver-memory 10g --executor-memory 10g --num-executors 4 --executor-cores 24 bwaMem -executable "/home/tahmad/tahmad/BWA/new/bwa/bwa" -index /home/tahmad/mcx/bio_data/ucsc.hg19.fasta  /home/tahmad/mcx/bio_data/gcat_025_a.fastq /home/tahmad/tahmad/bd/sample.alignments.adam -sample_id sample -sequence_dictionary /home/tahmad/mcx/bio_data/ucsc.hg19.dict -force_load_ifastq -fragments -add_files


SECONDS=0
#/home/tahmad/tahmad/bd/cannoli/bin/cannoli-shell --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=18000000  --conf spark.storage.blockManagerSlaveTimeoutMs=18000000 --conf spark.executor.heartbeatInterval=18000000  --driver-memory 10g --executor-memory 10g --num-executors 4 --executor-cores 24 -i /home/tahmad/tahmad/bd/cannoli/run.scala

duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."

SECONDS=0
rm -r /scratch-shared/tahmad/bio_data/ERR001268/ERR001268.adam
##/home/tahmad/tahmad/bd/adam/bin/adam-shell --master spark://${nodes[0]}:7077 --deploy-mode client -i /home/tahmad/tahmad/bd/adam/run.scala

#singularity exec spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/bin/spark-submit /home/tahmad/tahmad/script.py
#######################################################################################

duration=$SECONDS
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."

echo $MASTER
sleep infinity

#singularity exec spark.simg kill $(jobs -p)
#singularity exec spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/sbin/stop-slave.sh #kill $(pidof plasma_store_server)
#singularity exec spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/sbin/stop-master.sh
'''
for i in $( seq 1 $last )
do
   /usr/bin/ssh ${nodes[$i]} " singularity exec ~/tahmad/spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/sbin/stop-slave.sh ; echo ${nodes[$i]}; kill $(pidof plasma_store_server) " &
done
'''
#######################################################################################
