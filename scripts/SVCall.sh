#!/bin/bash 
#SBATCH --output=sparksingu.out
#SBATCH -t 02:10:00 


#SBATCH -p fat
#SBATCH -N 1                          
#SBATCH --mem=240G 

#SBATCH packjob

#SBATCH -p gpu 
#SBATCH -N 16  
#SBATCH --mem=16G 

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

echo "DEBUG: SLURM_NODELIST=$SLURM_JOB_NODELIST_PACK_GROUP_0"
echo "DEBUG: SLURM_NODELIST=$SLURM_JOB_NODELIST_PACK_GROUP_1"

export SPARK_HOME=/home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7
chmod -R a+rwx $SPARK_HOME

source del.txt
#source run.txt
#rm -r /home/tahmad/tahmad/outputs
#####################################################################################
nodes=($(scontrol show hostname $SLURM_NODELIST))
nnodes=${#nodes[@]}
last=$(( $nnodes - 1 ))

#sed -i 's/tcn768/'"${nodes[0]}"'/g' ~/tahmad/SparkGA2/config/config.xml
echo ${nodes}

ssh ${nodes[0]} hostname
echo -n "starting spark master on $MASTER... ";
#singularity exec ~/tahmad/spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/sbin/start-master.sh 
singularity exec ~/tahmad/spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/sbin/start-master.sh
#/home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/sbin/start-master.sh

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

for i in $( seq 1 $last )
do
   /usr/bin/ssh ${nodes[$i]} "singularity exec ~/tahmad/spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/sbin/start-slave.sh -c 16 -m 16G ${MASTER}; echo ${nodes[$i]} " &
done

'''
for i in $( seq 1 $last )
do
   /usr/bin/ssh ${nodes[$i]} "/home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/sbin/start-slave.sh -c 24 -m 25G ${MASTER}; echo ${nodes[$i]} " &
done
'''
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

python run.py ${nodes[0]}

#singularity exec ~/tahmad/spark.simg /home/tahmad/tahmad/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=1800000  --conf spark.executor.heartbeatInterval=18000  --driver-memory 16g --executor-memory 12g --num-executors 4 --executor-cores 20 /home/tahmad/tahmad/script.py
#source del.txt

#/home/tahmad/tahmad/testing/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master spark://${nodes[0]}:7077 --deploy-mode client --conf spark.network.timeout=1800000  --conf spark.executor.heartbeatInterval=18000  --driver-memory 5g --executor-memory 10g --num-executors 4 --executor-cores 20 /home/tahmad/tahmad/testing/script.py

#singularity exec spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/bin/spark-submit /home/tahmad/tahmad/script.py
#######################################################################################


#singularity exec spark.simg kill $(jobs -p)
#singularity exec spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/sbin/stop-slave.sh #kill $(pidof plasma_store_server)
#singularity exec spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/sbin/stop-master.sh
'''
for i in $( seq 1 $last )
do
   /usr/bin/ssh ${nodes[$i]} " singularity exec ~/tahmad/spark.simg /home/tahmad/tahmad/spark-2.3.4-bin-hadoop2.7/sbin/stop-slave.sh ; echo ${nodes[$i]}; kill $(pidof plasma_store_server) " &
done
'''

#echo $MASTER
#sleep infinity

#######################################################################################
