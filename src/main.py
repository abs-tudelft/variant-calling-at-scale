
#!/usr/bin/python

#export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
#export PATH=$JAVA_HOME/bin:$PATH

#export PYSPARK_PYTHON=/usr/bin/python3.6
#export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6

#alias python='/usr/bin/python3.6'
import multiprocessing
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
import threading

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma

import subprocess
import time
import random
import string
import sys
import os
import glob

import re
import hashlib
#import pysam

from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.sql.functions import pandas_udf,PandasUDFType

from pyspark.sql.functions import lit, count, col, when, UserDefinedFunction
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.types import *

from pyspark.rdd import RDD

from pyspark.sql.pandas.types import from_arrow_schema, to_arrow_schema

from pyspark.sql.functions import split
from pyspark.sql import functions as F

from pyspark.sql.dataframe import DataFrame

#from pyspark.sql.types import from_arrow_schema
#from pyspark.sql.dataframe import DataFrame
#from pyspark.serializers import ArrowSerializer, PickleSerializer, AutoBatchedSerializer


#################################################
BAM_CMATCH      ='M'
BAM_CINS        ='I'
BAM_CDEL        ='D'
BAM_CREF_SKIP   ='N'
BAM_CSOFT_CLIP  ='S'
BAM_CHARD_CLIP  ='H'
BAM_CPAD        ='P'
BAM_CEQUAL      ='='
BAM_CDIFF       ='X'
BAM_CBACK       ='B'

BAM_CIGAR_STR   ="MIDNSHP=XB"
BAM_CIGAR_SHIFT =4
BAM_CIGAR_MASK  =0xf
BAM_CIGAR_TYPE  =0x3C1A7

CIGAR_REGEX = re.compile("(\d+)([MIDNSHP=XB])")
#################################################


#################################################
def _arrow_record_batch_dumps(rb):
    
    #from pyspark.serializers import ArrowSerializer

    #import os
    #os.environ['ARROW_PRE_0_15_IPC_FORMAT'] = '1'
    
    return bytearray(rb.serialize())

    #return map(bytearray, map(ArrowSerializer().dumps, rb))


def createFromArrowRecordBatchesRDD(self, ardd, schema=None, timezone=None):
    #from pyspark.sql.types import from_arrow_schema
    #from pyspark.sql.dataframe import DataFrame
    #from pyspark.serializers import ArrowSerializer, PickleSerializer, AutoBatchedSerializer

    from pyspark.sql.pandas.types import from_arrow_schema
    from pyspark.sql.dataframe import DataFrame

    # Filter out and cache arrow record batches 
    ardd = ardd.filter(lambda x: isinstance(x, pa.RecordBatch)).cache()
    
    ardd = ardd.map(_arrow_record_batch_dumps)
    
    #schema = pa.schema([pa.field('c0', pa.int16()),
    #                    pa.field('c1', pa.int32())],
    #                   metadata={b'foo': b'bar'})
    schema = from_arrow_schema(_schema())

    # Create the Spark DataFrame directly from the Arrow data and schema
    jrdd = ardd._to_java_object_rdd()
    jdf = self._jvm.PythonSQLUtils.toDataFrame(jrdd, schema.json(), self._wrapped._jsqlContext)
    df = DataFrame(jdf, self._wrapped)
    df._schema = schema

    return df

def createFromArrowRecordBatchesRDD1(self, prdd, batches, schema=None, timezone=None):
    print("From inside createFromArrowRecordBatchesRDD..")

    prdd = prdd.flatMap(lambda x: map(bytearray, map(ArrowSerializer().dumps, batches)))

    # Create the Spark DataFrame directly from the Arrow data and schema
    jrdd = prdd._to_java_object_rdd()
    jdf = self._jvm.PythonSQLUtils.arrowPayloadToDataFrame(
        jrdd, from_arrow_schema(arrow_schema()).json(), self._wrapped._jsqlContext)
    df = DataFrame(jdf, self._wrapped)
    df._schema = schema
    return df

#################################################
# Connect to clients
def connect():
    global client
    client = plasma.connect('/tmp/store0', 0)
    #np.random.seed(int(time.time() * 10e7) % 10000000)

def arrow_schema():
    fields = [
        pa.field('qNames', pa.string()),
        pa.field('flags', pa.int32()),
        pa.field('rIDs', pa.int32()),
        pa.field('beginPoss', pa.int32()),
        pa.field('mapQs', pa.int32()),

        pa.field('cigars', pa.string()),

        pa.field('rNextIds', pa.int32()),
        pa.field('pNexts', pa.int32()),
        pa.field('tLens', pa.int32()),

        pa.field('seqs', pa.string()),
        pa.field('quals', pa.string()), #issue TODO
        pa.field('tagss', pa.string())

    ]
    return pa.schema(fields)

def _schema():
    fields = [
        pa.field('beginPoss', pa.int32()),
        pa.field('sam', pa.string())
    ]
    return pa.schema(fields)

def runDeepVariant(inBAM):
    
    #/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-2x-seqk/ERR194147-chr1-0.bam
    #/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-2x-seqk/ERR194147-chr1.bam
    
    
    items = inBAM.split('-')
    matching = [s for s in items if "chr" in s]
    print(matching)
    if ".bam" in matching[0]:
        CHR = matching[0].replace(".bam", "")
        CHRPART = ""
    else:
        last2 = items[-2:]
        last = last2[-1:]
        last= last[0]
        CHR = matching[0]
        CHRPART = last.replace(".bam", "")
 
    BIN_VERSION="1.0.0"
    OUTPUT_DIR='/scratch-shared/tahmad/bio_data/HG001_NA12878/quickstart-output-'+CHR+'-'+CHRPART
    print(OUTPUT_DIR)
    os.system("mkdir -p OUTPUT_DIR")
    
    docker='docker://google/deepvariant:'+BIN_VERSION 
    interdir='--intermediate_results_dir='+OUTPUT_DIR+'/intermediate_results_dir'
    #ref='--ref=/home/tahmad/mcx/bio_data/fasta/'+CHR+'/'+CHR+'.fa'
    ref='--ref=/scratch-shared/tahmad/bio_data/hg38/fasta/'+CHR+'.fa'
    read='--reads='+inBAM
    out='--output_vcf='+OUTPUT_DIR+'/output.vcf.gz' 
    outg='--output_gvcf='+OUTPUT_DIR+'/output.g.vcf.gz'

    #os.system("singularity run -B /usr/lib/locale/:/usr/lib/locale/  docker://google/deepvariant:"+BIN_VERSION+  " /opt/deepvariant/bin/run_deepvariant  --model_type=WGS   --ref=/home/tahmad/mcx/bio_data/fasta/"+CHR+"/"+CHR+".fa  --reads="+inBAM+  " --output_vcf="+OUTPUT_DIR+"/ERR194147-output.vcf.gz  --output_gvcf="+OUTPUT_DIR+"/ERR194147-output.g.vcf.gz  --intermediate_results_dir "+OUTPUT_DIR+"/intermediate_results_dir --num_shards=1")
    cmd= ['/usr/bin/singularity', 'run', '-B /usr/lib/locale/:/usr/lib/locale/',  docker, '/opt/deepvariant/bin/run_deepvariant',  '--model_type=WGS',   ref, read, out, outg,  interdir, '--num_shards=6']
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()
    #os.system("which singularity")
    #prog = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #out, err = prog.communicate()
    return 1

def runBWA(fqfile):
    print(fqfile)

    ## importing socket module
    #import socket
    ## getting the hostname by socket.gethostname() method
    #hostname = socket.gethostname()
    ## getting the IP address using socket.gethostbyname() method
    #ip_address = socket.gethostbyname(hostname)
    os.system("plasma_store_server -m 8000000000 -s /tmp/store0 &")
    #cmd='./bwa mem -t 2 /home/tahmad/GenData/hg19.fasta' + ' ' + files[0] +' ' + files[1]
    #os.system("/home/tahmad/tahmad/apps/bwa/bwa mem -t 16 /home/tahmad/mcx/bio_data/ucsc.hg19.fasta" + " " + files + " " + ">" +" " + files+".sam")
    out = fqfile[0]+'.sam'
    #cmd = ['/home/tahmad/tahmad/apps/bwa/bwa', 'mem', '-t 16', '/home/tahmad/mcx/bio_data/ucsc.hg19.fasta', files, '>', out]
    #cmd = ['/home/tahmad/tahmad/bwa', 'mem', '-t 16', '/home/tahmad/mcx/bio_data/ucsc.hg19.fasta', files[0], files[1]]
    #cmd = ['/home/tahmad/tahmad/bwa', 'mem', '-t 16', '/home/tahmad/mcx/bio_data/ucsc.hg19.fasta', '/home/tahmad/mcx/bio_data/ERR001268_1_400_lines.fastq', '/home/tahmad/mcx/bio_data/ERR001268_2_400_lines.fastq']
    #cmd = ['/home/tahmad/tahmad/bwa', 'mem', '-t 16', '/home/tahmad/mcx/bio_data/ucsc.hg19.fasta', files]
    #print(cmd)
    #cmd = ['/home/tahmad/tahmad/bwa', 'mem', '-t 16', '/home/tahmad/mcx/bio_data/ucsc.hg19.fasta', '/home/tahmad/mcx/bio_data/ERR001268/ERR001268_1.fastq.gz', '/home/tahmad/mcx/bio_data/ERR001268/ERR001268_2.fastq.gz']
    #cmd = ['/home/tahmad/tahmad/bwa', 'mem', '-t 24', '/home/tahmad/mcx/bio_data/ucsc.hg19.fasta', fqfile]
    cmd = ['/home/tahmad/tahmad/bwa', 'mem', '-t 12', '/home/tahmad/mcx/bio_data/ucsc.hg19.fasta', fqfile[0], fqfile[1]]
    #cmd = ['/home/tahmad/tahmad/BWA/new/bwa/bwa', 'mem', '-t 20', '/home/tahmad/mcx/bio_data/ucsc.hg19.fasta', fqfile[0], fqfile[1], '-o', out]
    #cmd = ['/home/tahmad/tahmad/bwa', 'mem', '-t 2', '/home/tahmad/mcx/bio_data/ucsc.hg19.fasta', '/scratch-shared/tahmad/bio_data/ERR001268/ERR001268-4-seqk/ERR001268_1.filt.part_001.fastq.gz', '/scratch-shared/tahmad/bio_data/ERR001268/ERR001268-4-seqk/ERR001268_2.filt.part_001.fastq.gz']
    #os.system(cmd)
    #print(execute(
    #    cmd,
    #    lambda x: print("STDOUT: %s" % x),
    #    lambda x: print("STDERR: %s" % x),
    #))
    #with open("/home/tahmad/tahmad/log.txt", "w") as log:
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()
    '''
    # Connect to the plasma store.
    prdd=[]
    connect()
    lines = [line[:20] for line in open('/home/tahmad/tahmad/objID.txt')]
    lines = lines[:2]
    object_ids = [plasma.ObjectID(byte) for byte in lines]
    buffers = client.get_buffers(object_ids)
    #return [pa.read_record_batch(pa.BufferReader(buf), arrow_schema()) for buf in buffers]

    for k in range(len(object_ids)):
        batch = pa.read_record_batch(buffers[k], arrow_schema())#.to_pandas()
        prdd.append(batch)
    os.system("kill $(pidof plasma_store_server)")
    '''
    return 1

def ArrowDataCollection(chr_id):
    #Connect to the plasma store.
    #connect()
    lines = [line[:20] for line in open('/dev/shm/objID.txt')]
    lines = lines[:65]
    print(lines)
    object_ids = [plasma.ObjectID(bytes(byte.encode())) for byte in lines]
    #Connect to the plasma store.
    connect()
    [buffers] = client.get_buffers([object_ids[chr_id]])#(b'u$a71i0Rkk*1LkQ46d2D')])
    batch = pa.read_record_batch(buffers, _schema())#.to_pandas()
    
    #if(chr_id==64):
    #    os.system("kill $(pidof plasma_store_server)")
    return batch

def hostname(x):
    ## importing socket module
    import socket
    ## getting the hostname by socket.gethostname() method
    hostname = socket.gethostname()
    ## getting the IP address using socket.gethostbyname() method
    ip_address = socket.gethostbyname(hostname)
    return [hostname,ip_address]

def rb_return(ardd):
    data = [
        pa.array(range(5), type='int16'),
        pa.array([-10, -5, 0, None, 10], type='int32')
    ]
    schema = pa.schema([pa.field('c0', pa.int16()),
                        pa.field('c1', pa.int32())],
                       metadata={b'foo': b'bar'})
    return pa.RecordBatch.from_arrays(data, schema)


#########################################################
def qual_score_by_cigar(cigars):
    qpos = 0
    parts = CIGAR_REGEX.findall(cigars)
    for y in range(0, len(parts)):
        op = parts[y][1] 
        if op == BAM_CMATCH or \
            op == BAM_CINS or \
            op == BAM_CSOFT_CLIP or \
            op == BAM_CEQUAL or \
            op == BAM_CDIFF:
                   qpos += int(parts[y][0])    
    return qpos
#########################################################
def mean_qual(quals):
    qual = [ord(c) for c in quals]
    mqual = (1 - 10 ** (np.mean(qual)/-10))
    return mqual
#########################################################
    
def sam_operations_pairs(pdf):
 
    res_d=[]
    process_next = False
    process_start = False

    index_d=[]
    read_qual_d=[]
    j=0
    # fields to store for each alignment
    #fields = ['index','read_qual']
    #data = {f: list() for f in fields}
    
    len_pdf=len(pdf.index)
    for i in range(0, len_pdf-1):
        if int(pdf.loc[i+1, 'beginPoss']) == int(pdf.loc[i, 'beginPoss']) and int(pdf.loc[i+1, 'pNexts']) == int(pdf.loc[i, 'pNexts']): 
            #if process_next == False and process_start == False:
                # fields to store for each alignment
                #fields = ['index','read_qual']
                #data = {f: list() for f in fields}
             
            index_d.insert(j, i)
            read_qual_d.insert(j, mean_qual(pdf.loc[i, 'quals']))
            j=j+1
            #data['index'].append(i)
            #data['read_qual'].append(mean_qual(pdf.loc[i, 'quals']))
            process_next = True
        else: 
            process_start = True
            res_d.insert(i, int(pdf.loc[i, 'flags']))

        if process_next == True and process_start == True:
            #sdf = pd.DataFrame(data)
            #rq = sdf["read_qual"]
            read_qual_index = read_qual_d.index( max(read_qual_d) ) 

            for i in range(0, len(read_qual_d)):
                if i == read_qual_index:
                    #res_d.insert(sdf.loc[i, 'index'], int(pdf.loc[i, 'flags']))
                    res_d.insert(index_d[i], int(pdf.loc[i, 'flags'])) 
                else: 
                    #res_d.insert(sdf.loc[i, 'index'], int(pdf.loc[i, 'flags']) | 1024)  
                    res_d.insert(index_d[i], int(pdf.loc[i, 'flags']) | 1024)
            process_next = False 
            process_start = False
 
            index_d.clear()
            read_qual_d.clear()
            j=0

             
    res_d.insert(len_pdf, int(pdf.loc[len_pdf-1, 'flags']))
    
    return  pd.DataFrame({'flags': res_d})

#######################################################################################################

def sam_operations(pdf):
    # fields to store for each alignment
    fields = ['query_md5', 'flags','ref_start','ref_end','query_length','read_qual','num_passes']
    data = {f: list() for f in fields}

    for i in range(1, len(pdf)):
        data['query_md5'].append(hashlib.md5(pdf.loc[i, 'qNames'].encode('utf-8')).hexdigest())
        data['flags'].append(int(pdf.loc[i, 'flags']))
        data['ref_start'].append(int(pdf.loc[i, 'beginPoss']))
        data['ref_end'].append(int(pdf.loc[i, 'pNexts']))
        data['query_length'].append(int(qual_score_by_cigar(pdf.loc[i, 'cigars'])))
        #if read.has_tag('rq'):
            #data['read_qual'].append(None)#float(read.get_tag('rq')))
        #elif read.query_qualities:
        data['read_qual'].append(mean_qual(pdf.loc[i, 'quals']))
        #if read.has_tag('np'):
        data['num_passes'].append(None)#int(read.get_tag('np')))
        #data['dup'].append(False)
        #data['dup_index'].append(None)
        
    #if not data['read_qual']:
    #    data.pop('read_qual', None)
    #    fields.remove('read_qual')
    #if not data['num_passes']:
    #    data.pop('num_passes', None)
    #    fields.remove('num_passes')
        
    sdf = pd.DataFrame(data)
        
    res_d=[]
    by = ['read_qual', 'num_passes', 'query_md5']
    ascending = [False, False, True]
    columns = ['dup','dup_index']
    df = pd.DataFrame(([False, None] for i in range(len(sdf))), columns=columns)
    dup_index = 0
    dup_state = False
    for i in range(1, len(sdf)):
        if abs(sdf.loc[i-1, 'ref_start'] - sdf.loc[i, 'ref_start']) <= 2 and abs(sdf.loc[i, 'ref_end'] - sdf.loc[i-1, 'ref_end']) <= 2 and abs(sdf.loc[i-1, 'query_length'] - sdf.loc[i, 'query_length']) <= 10/100.0 * sdf.loc[i, 'query_length']: 
            df.loc[[i, i-1], 'dup'] = True  # mark this read and previous read
            df.loc[[i, i-1], 'dup_index'] = dup_index  # a block of duplicates
            dup_state = True  # in the middle of a block of duplicates
        elif dup_state:
            dup_state = False
            dup_index += 1
            
    for i in range(0, dup_index):
        df.loc[sdf[df['dup_index'] == i].sort_values(by=by, ascending=ascending).index[0], 'dup'] = False

    duplicates = sdf[df['dup']].index.values
    for i in range(0, len(sdf)):
        if i in duplicates:
            res_d.insert(i, sdf.loc[i, 'flags'] | 1024)
        else: 
            res_d.insert(i, sdf.loc[i, 'flags'])
            
    return  pd.DataFrame({'flags': res_d})
        
    #return pd.DataFrame(data)
       
#########################################################



def arrow_data_process(i):
    print(i)
    ardd = spark.sparkContext.parallelize([i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i], 16).map(ArrowDataCollection)
    df = spark.createFromArrowRecordBatchesRDD(ardd)
    sorted_sdf = df.orderBy('beginPoss', ascending=True)
    #sorted_sdf.show()
    if(not sorted_sdf.rdd.isEmpty()):
        sam_df = sorted_sdf.select(split(sorted_sdf.sam,"\t")).rdd.flatMap(lambda x: x).toDF(schema=["qNames","flags","rIDs","beginPoss", "mapQs", "cigars", "rNextIds", "pNexts", "tLens", "seqs", "quals", "tagss"])
        md_df = sam_df.groupby("rIDs").applyInPandas(sam_operations_pairs, schema="flags integer")
        #md_df.show()
        #md_df.write.option("sep", "\t").option("encoding", "UTF-8").csv('/home/tahmad/tahmad/outputs/output'+str(i)+'.csv')

    return 1

def bwa_process(path):
    
    file_names1 = sorted(glob.glob(path+"-4-seqk/ERR001268_1"+ "*.fastq.gz"))
    file_names2 = sorted(glob.glob(path+"-4-seqk/ERR001268_2"+ "*.fastq.gz"))
    #file_names1 = sorted(glob.glob(path+"-4-seqk/ERR001268_1.filt.part_001"+ "*.fastq.gz"))
    #file_names2 = sorted(glob.glob(path+"-4-seqk/ERR001268_2.filt.part_001"+ "*.fastq.gz"))
    print(file_names1)
    print(file_names2)
    fqfile = list(zip(file_names1, file_names2))
    
    #runBWA(fqfile)
    frdd = spark.sparkContext.parallelize(fqfile, len(fqfile))
    frdd = frdd.map(runBWA).collect() 
    return 1

def streaming_process(path):
    cmd = '/home/tahmad/tahmad/seqkit split2 --threads=18 -1 '+ path +'_1.filt.fastq.gz -2 '+ path +'_2.filt.fastq.gz -p 4 -O '+ path +'-4-seqk -f'
    os.system(cmd)
    return 1

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark example") \
        .getOrCreate()

    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    #os.system("plasma_store_server -m 3000000000 -s /tmp/store0 & ./bwa mem -t 24 /home/tahmad/mcx/bio_data/ucsc.hg19.fasta /home/tahmad/mcx/bio_data/gcat_025_1a.fastq /home/tahmad/mcx/bio_data/gcat_025_2a.fastq")
#    os.system("plasma_store_server -m 3000000000 -s /tmp/store0 &")
#    cmdStr = ['./bwa', 'mem', '-t 4', '/home/tahmad/GenData/hg19.fasta',
#            '/home/tahmad/GenData/gcat_025_1a.fastq',
#            '/home/tahmad/GenData/gcat_025_2a.fastq']
    cmdStr = ['./bwa', 'mem', '-t 2', '/home/tahmad/GenData/hg19.fasta',
            '/home/tahmad/GenData/ERR001268_1_400_lines.fastq',
            '/home/tahmad/GenData/ERR001268_2_400_lines.fastq']
    #print(execute(
    #    cmdStr,
    #    lambda x: print("STDOUT: %s" % x),
    #    lambda x: print("STDERR: %s" % x),
    #))
    file_names1 = sorted(glob.glob("/home/tahmad/mcx/bio_data/input/ERR001268_1" + "*.fastq"))
    file_names2 = sorted(glob.glob("/home/tahmad/mcx/bio_data/input/ERR001268_2" + "*.fastq"))
    #file_names1 = sorted(glob.glob("/home/tahmad/GenData/gcat_025_1a" + ".fastq"))
    #file_names2 = sorted(glob.glob("/home/tahmad/GenData/gcat_025_2a" + ".fastq"))
    
    file_names = list(zip(file_names1, file_names2))
    #file_names = sorted(glob.glob("/home/tahmad/tahmad/SparkGA2/chunks/" + "*.fq.gz"))
    print(file_names)
    #inData=spark.sparkContext.parallelize(file_names, 3).coalesce(3)
    #print(inData.collect())
    #print(file_names)
    start = time.time()
    outIDs = spark.sparkContext.parallelize([0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7],32).map(lambda x: hostname(x)).collect()
    outIDs = spark.sparkContext.parallelize([0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7],32).map(lambda x: hostname(x)).collect()
    #outIDs = spark.sparkContext.parallelize([0,1,2,3,4],5).map(lambda x: hostname(x)).collect()
    #outIDs = spark.sparkContext.parallelize([0,1,2,3,4],5).map(lambda x: hostname(x)).collect()
    print(outIDs)
    #prdd = spark.sparkContext.parallelize([0],1).map(runBWA)
    stop = time.time()
    print('BWA flatMap took {} seconds.'
          .format(stop - start))
     
    #print(outIDs)
    os.system("which singularity")
    #print(spark.sparkContext.parallelize(list(range(1000))).flatMap(lambda x: hostname(x)).collect())
    #print("**********************************************************************")
    #print(spark.sparkContext.parallelize(list(range(100))).map(lambda x: hostname(x)).collect()) 
    start = time.time() 
    #prdd = spark.sparkContext.parallelize([0],1).map(ArrowDataCollection).cache()#.collect()
    stop = time.time()
    print('BWA flatMap ArrowDataCollection took {} seconds.'
          .format(stop - start))
    #SparkSession.createFromArrowRecordBatchesRDD = createFromArrowRecordBatchesRDD
    #batches.append(pdf1[0])
    #batches.append(pdf1[1])

    #start = time.time()
    #rdd_batch = []
    #rdd_batch.append(batch[i] for batch in batches[i][0])
    #rdd_batch.append(batches[1][0])
    #rdd_batch.append(batches[2][0])

    #sdf1 = spark.sparkContext.parallelize([0])
    #sdf = spark.createFromArrowRecordBatchesRDD(sdf1, rdd_batch)
    #stop = time.time()
    #print('BWA flatMap SDF Creation [chr-1] took {} seconds.'
    #      .format(stop - start))

    #start = time.time()
    #result_sdf = sdf.sortWithinPartitions('beginPoss', ascending=True).orderBy('beginPoss', ascending=True) 
    #result_sdf = sdf.orderBy('beginPoss', ascending=True)
    #stop = time.time()
    #print('BWA flatMap sorting took {} seconds.'
    #      .format(stop - start))
    #result_sdf.show()
    
    #rdd_batch1 = []
    #rdd_batch1.append(batches[0][1])
    #rdd_batch1.append(batches[1][1])
    #rdd_batch1.append(batches[2][1])

    #sdf = spark.createFromArrowRecordBatchesRDD(sdf1, rdd_batch1)
    #start = time.time()
    #result_sdf = sdf.sortWithinPartitions('beginPoss', ascending=True).orderBy('beginPoss', ascending=True)
    #result_sdf = sdf.orderBy('beginPoss', ascending=True)
    #stop = time.time()
    #print('BWA flatMap sorting took {} seconds.'
    #      .format(stop - start))

    #result_sdf.show()

    #file_names1 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/NA12878/SRR029655-8-seqk/SRR029655_1.filt.part_" + "*.fastq.gz"))
    #file_names2 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/NA12878/SRR029655-8-seqk/SRR029655_2.filt.part_" + "*.fastq.gz"))
    #file_names1 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-8-seqk/ERR194147_1.part_" + "*.fastq"))
    #file_names2 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-8-seqk/ERR194147_2.part_" + "*.fastq"))
    '''
    file_names1 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-32-seqk/ERR194147_1.part_" + "*.fastq"))
    file_names2 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-32-seqk/ERR194147_2.part_" + "*.fastq"))
    
    
    fqfile = list(zip(file_names1, file_names2))
    '''
    #print(fqfile)
    #fqfile = sorted(glob.glob("/home/tahmad/tahmad/SparkGA2/chunks-ERR001268/" + "*.fq.gz"))
    
    #file_names1 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-2x-seqk/ERR194147-2x-32-seqk/ERR194147_1.part_" + "*.fastq"))
    #file_names2 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-2x-seqk/ERR194147-2x-32-seqk/ERR194147_2.part_" + "*.fastq"))
    ####################################
    '''
     path="/scratch-shared/tahmad/bio_data/ERR001268/ERR001268"
   
    job1 = multiprocessing.Process(target=streaming_process, args=(path,))
    job1.start()
    job2 = multiprocessing.Process(target=bwa_process, args=(path,))
    job2.start()
    # Wait for both jobs to finish
    job1.join()
    job2.join()
    '''
    ####################################
    
    file_names1 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR001268/ERR001268-16-seqk/ERR001268_1.filt.part_" + "*.fastq.gz"))
    file_names2 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR001268/ERR001268-16-seqk/ERR001268_2.filt.part_" + "*.fastq.gz"))
    
    #file_names1 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-4-seqk/ERR194147_1.part"+"*.fastq.gz"))
    #file_names2 = sorted(glob.glob("/scratch-shared/tahmad/bio_data/ERR194147/ERR194147-4-seqk/ERR194147_2.part"+"*.fastq.gz"))

    fqfile = list(zip(file_names1, file_names2))

    frdd = spark.sparkContext.parallelize(fqfile, len(fqfile))
    frdd = frdd.map(runBWA).collect()
     
    SparkSession.createFromArrowRecordBatchesRDD = createFromArrowRecordBatchesRDD
    
    ardd = spark.sparkContext.parallelize([0],1).map(ArrowDataCollection)
    df = spark.createFromArrowRecordBatchesRDD(ardd)
    sorted_sdf = df.orderBy('beginPoss', ascending=True) 
    sorted_sdf.show()
    
    '''    
    file_bam = sorted(glob.glob("/scratch-shared/tahmad/bio_data/HG001_NA12878/HG001_NA12878-chr22" + ".bam"))
    print(file_bam)
    brdd = spark.sparkContext.parallelize(file_bam, len(file_bam))
    brdd = brdd.map(runDeepVariant).collect()
    '''
    #ardd = spark.sparkContext.parallelize([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],16).map(ArrowDataCollection)
    #df = spark.createFromArrowRecordBatchesRDD(ardd)
    #sorted_sdf = df.orderBy('beginPoss', ascending=True) 
    #sorted_sdf.show()

    #Connect the processes in the pool.
    #pool = Pool(initargs=(), processes=2)
    #pool.map(arrow_data_process, range(2))
 
    p = ThreadPool(65)
    p.map(arrow_data_process, range(65))
    
    '''
    # Create the Spark schema from the first Arrow batch (always at least 1 batch after slicing)
    if isinstance(schema, (list, tuple)):
        struct = from_arrow_schema(batch.schema)
        for i, name in enumerate(schema):
            struct.fields[i].name = name
            struct.names[i] = name
        schema = struct

    # Create the Spark DataFrame directly from the Arrow data and schema
    jrdd = spark._sc._serialize_to_jvm(batch, len(batch), ArrowSerializer())
    jdf = spark._jvm.PythonSQLUtils.arrowPayloadToDataFrame(
        jrdd, schema.json(), spark._wrapped._jsqlContext)
    df = DataFrame(jdf, self._wrapped)
    df._schema = schema
    df.show()
    #ardd = spark.sparkContext.parallelize([0,1,2],3).map(lambda x: rb_return(x)) 
    #SparkSession.createFromPandasDataframesRDD = createFromPandasDataframesRDD
    #sdf = spark.createFromPandasDataframesRDD(prdd)
    #sdf.show()
    '''
    '''
    sdf1 = spark.sparkContext.parallelize([0])
    sdf = spark.createFromArrowRecordBatchesRDD(sdf1, batches[1])
    result_sdf = sdf.sortWithinPartitions('beginPoss', ascending=True).orderBy('beginPoss', ascending=True)
    result_sdf.show()

    sdf1 = spark.sparkContext.parallelize([0])
    sdf = spark.createFromArrowRecordBatchesRDD(sdf1, batches[1])
    result_sdf = sdf.sortWithinPartitions('beginPoss', ascending=True).orderBy('beginPoss', ascending=True)
    result_sdf.show()
    '''
    '''
    sdf = spark.createFromArrowRecordBatchesRDD(sdf1, batches[1])
    result_sdf = sdf.sortWithinPartitions('beginPoss', ascending=True).orderBy('beginPoss', ascending=True)
    result_sdf.show()
    '''
    #result_pdf = sorted_sdf.select("*").toPandas().to_csv('csv.csv')
    #print(result_pdf)
    #chr1=[]
    #chr1.append(outData[0][0])
    #chr1.append(outData[2][0])
    '''
    Chr1= pa.Table.from_batches([outData[0][0], outData[2][0]])
    resultChr1 = Chr1.to_pandas()
    sdfChr1 = spark.createDataFrame(resultChr1)
    sorted_sdfChr1 = sdfChr1.orderBy('beginPoss', ascending=True)
    #print(result)
    #df = batches_to_df(outData[0][0])
    '''
    '''
    process = subprocess.Popen(['stdbuf', '-o0'] + cmdStr, stdout=subprocess.PIPE, universal_newlines=False)
    #process.wait()
    start = time.time()
    #out = process.communicate() #(out, err) = process.communicate()
    stop = time.time()
    print('Pipe Buffers took {} seconds.'
          .format(stop - start))
    
    #for line in iter(process.stdout.readline, ''):
    #    print("<***> " + line.rstrip('\r'))
    # Start the plasma store.
    #p = subprocess.Popen(['plasma_store_server',
    #                      '-s', '/tmp/store0'])
    #print(out)
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print ("<***>" + output.strip())
    '''
    #process.wait()
    #view = memoryview(out[0])
    #limited = view[0:2959]
    #bytes(limited)
    #print(limited.tobytes())
    #dfs = get_dfs_arrow(out[0])
    '''
    buffer1 = pa.BufferReader(out[0][2960:2960+4304])#[0:2960])
    batch1 = pa.read_record_batch(buffer1, schema())
    df1 = batch1.to_pandas()
    print(df1)
    '''
    '''
    lines = [line[:20] for line in open('objID.txt')]
    #print(lines)
    object_ids = [plasma.ObjectID(byte) for byte in lines]
    object_ids = object_ids[:len(lines)]
    print(object_ids)

    # Connect to the plasma store.
    connect()

    # Connect the processes in the pool.
    pool = Pool(initializer=connect, initargs=(), processes=len(lines))

    dfs = get_dfs_arrow(object_ids)
    #print(dfs)
    '''
    '''
    start = time.time()
    sorted_df = dfs[0].sort_values(by='beginPoss')
    stop = time.time()
    print('Sort took {} seconds.'
          .format(stop - start))


    spark = SparkSession \
        .builder \
        .appName("Python Arrow-in-Spark example") \
        .getOrCreate()

    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    start = time.time()
    sdf = spark.createDataFrame(dfs[0])
    sorted_sdf = sdf.orderBy('beginPoss', ascending=True)
    stop = time.time()
    print('Sort took {} seconds.'
          .format(stop - start))

    spark.stop()
    '''
 #   os.system("kill $(pidof plasma_store_server)")

    spark.stop()
