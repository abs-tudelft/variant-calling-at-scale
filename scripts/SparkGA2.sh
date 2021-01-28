#!/bin/bash 
#SBATCH -N 3                           #request 3 nodes
#SBATCH -t 2:10:00                  #job to run for 10 minutes
#SBATCH --ntasks-per-node 4   #4 tasks per node
#SBATCH --cpus-per-task 4       #4 cpus per task
#SBATCH --output=master6.out
#SBATCH --mem=50G  

############################
module load pre2019
module load python                 # load python,spark, and java modules
module load java/oracle/8u73
############################
#conda activate pyarrow-dev
source activate pyarrow-dev

SPARK_HOME=~/tahmad/spark-2.3.4-bin-hadoop2.7
export PATH=$SPARK_HOME/bin:$PATH

export ARROW_HOME=~/tools
export LD_LIBRARY_PATH=$ARROW_HOME/lib:$LD_LIBRARY_PATH

rm -r  /scratch-shared/tahmad/bio_data/ERR001268-n/output

#M2_HOME=~/tahmad/tools/apache-maven-3.6.3
#export PATH=$M2_HOME/bin:$PATH

#JAVA_HOME=~/tahmad/tools/jdk-14.0.1
#export PATH=$JAVA_HOME/bin:$PATH

#####################################################################################
nodes=($(scontrol show hostname $SLURM_NODELIST))
nnodes=${#nodes[@]}
last=$(( $nnodes - 1 ))

sed -i 's/tcn611/'"${nodes[0]}"'/g' ~/tahmad/SparkGA2/config/config.xml

ssh ${nodes[0]} hostname
echo -n "starting spark master on $MASTER... ";
$SPARK_HOME/sbin/start-master.sh
PORT=7077
export MASTER="spark://${nodes[0]}:$PORT"
echo -n "starting spark master on $MASTER... ";
echo "done";
sleep 2;
echo "spark cluster web interface: http://tcn753:8080"  >$HOME/spark-info
echo "           spark master URL: spark://tcn753:7077" >>$HOME/spark-info

export MASTER=$MASTER

echo starting on-node worker
nohup ${SPARK_HOME}/sbin/start-slave.sh -c 24 -m 64G ${MASTER}

echo opening ssh connections to start the other nodes worker processeses
i=0
for i in $( seq 1 $last )
do
    ssh ${nodes[i]} hostname
done

echo starting remote workers

for i in $( seq 1 $last )
do
   /usr/bin/ssh ${nodes[$i]} "module load pre2019 spark java/oracle/8u73 python ; nohup $SPARK_HOME/sbin/start-slave.sh -c 16 -m 64G ${MASTER}; echo ${nodes[$i]} " &
   #/usr/bin/ssh ${nodes[$i]} "module load pre2019 java/oracle/8u73 python ; SPARK_HOME=~/tahmad/spark; export PATH=$SPARK_HOME/bin:$PATH; nohup $SPARK_HOME/sbin/start-slave.sh -c 24 -m 64G ${MASTER}; echo ${nodes[$i]}; /home/tahmad/tahmad/scripts/iostat.sh > /home/tahmad/tahmad/io_${nodes[$i]}.csv " &

done
#######################################################################################

cd ~/tahmad/SparkGA2
#python runChunker.py chunker_2.11-1.0.jar config.xml
python runPart.py config/config.xml 1
python runPart.py config/config.xml 2
python runPart.py config/config.xml 3
'''
python runPart.py config/config.xml 1
python runPart.py config/config.xml 2
python runPart.py config/config.xml 3


python runPart.py config/config.xml 1
python runPart.py config/config.xml 2
python runPart.py config/config.xml 3


python runPart.py config/config.xml 1
python runPart.py config/config.xml 2
python runPart.py config/config.xml 3


python runPart.py config/config.xml 1
python runPart.py config/config.xml 2
python runPart.py config/config.xml 3
'''
#######################################################################################
$SPARK_HOME/sbin/stop-slave.sh
$SPARK_HOME/sbin/stop-master.sh

for i in $( seq 1 $last )
do
   /usr/bin/ssh ${nodes[$i]} " $SPARK_HOME/sbin/stop-slave.sh ; echo ${nodes[$i]} " &
done
#######################################################################################
