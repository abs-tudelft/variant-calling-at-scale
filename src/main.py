#!/usr/bin/python

import multiprocessing
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
import threading

import numpy as np
import pyarrow as pa
import pysam
import pandas as pd
import re
import hashlib

'''
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.plasma as plasma
'''
import subprocess
import time
import random
import string
import sys
import os
import glob
import argparse
import shutil
'''
import re
import hashlib
import pysam
'''
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

#################################################
CHRMS=['1', '110', '120', '130', '140', '150','160', '170', '180', '190', '2', '210', '220', '230', '240', '250', '260', '270', '280', '290', '3', '31', '32', '33', '34','35','36','37', '4', '41', '42', '43', '44','45','46','47', '5', '51', '52', '53', '54','55','56','57', '6', '61', '62', '63', '64','65','66', '7', '71', '72', '73','74','75','76', '8', '81', '82', '83','84','85', '9', '91', '92', '93','94','95', '10', '101', '102', '103','104','105', '11', '111', '112', '113','114','115', '12', '121', '122', '123','124','125', '13', '131', '132','133','134', '14', '141', '142','143', '15', '151', '152','153', '16', '161', '162','163', '17', '171', '172', '18', '181', '182', '19', '191', '20', '201','202', '21', '211', '22', '221', '23', '231', '232', '233','234','235', '24', '241', '25']

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
    if(args.aligner=="BWA"):
        schema = from_arrow_schema(sam_schema())
    else:
        schema = from_arrow_schema(_schema())

    # Create the Spark DataFrame directly from the Arrow data and schema
    jrdd = ardd._to_java_object_rdd()
    jdf = self._jvm.PythonSQLUtils.toDataFrame(jrdd, schema.json(), self._wrapped._jsqlContext)
    df = DataFrame(jdf, self._wrapped)
    df._schema = schema

    return df

def _schema():
    fields = [
        pa.field('beginPoss', pa.int32()),
        pa.field('sam', pa.string())
    ]
    return pa.schema(fields)

def sam_schema():
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

#########################################################
#########################################################
def runDeepVariant(n_index):
    for index, inBAM in enumerate(BAM_FILES, start=0):
        if index % int(NODES) == n_index:
            #/scratch-shared/tahmad/bio_data/HG002/HG002_chr15:76493391-86493391.bam
            print(inBAM)

            index_path='samtools index -@' + CORES +' '+ inBAM
            os.system(index_path)

            REGION = inBAM.split('_')[-1].replace(".bam", "")
            CHR= inBAM.split('_')[-1].replace(".bam", "").split(':')[0]
            BIN_VERSION="1.1.0"

            OUT=os.path.dirname(inBAM)
            print(REGION)

            DVOUTPUT=OUT+'/outputs/dv_output-'+REGION
            WHATSHAPOUTPUT=OUT+'/outputs/whatshap_output-'+REGION

            if os.path.exists(DVOUTPUT) and os.path.exists(WHATSHAPOUTPUT):
                shutil.rmtree(DVOUTPUT)
                shutil.rmtree(WHATSHAPOUTPUT)

            os.mkdir(DVOUTPUT)
            os.mkdir(WHATSHAPOUTPUT)

            OUTPUT_DIR=DVOUTPUT+'/quickstart-output-'+REGION

            docker='docker://google/deepvariant:'+BIN_VERSION
            interdir='--intermediate_results_dir='+DVOUTPUT+'/intermediate_results_dir'
            ref_dv='--ref='+REF #/scratch-shared/tahmad/bio_data/GRCh38/GRCh38.fa'
            read='--reads='+inBAM
            out='--output_vcf='+OUT+'/'+ REGION +'.dv.vcf.gz'
            outg='--output_gvcf='+DVOUTPUT+'/'+ REGION +'.dv.g.vcf.gz'
            region='--regions='+REGION
            #region='--regions=chr20'
            threads='--num_shards='+CORES

            #cmd='singularity run -B /mnt/fs_shared/:/mnt/fs_shared/ /mnt/fs_shared/deepvariant_1.1.0.sif ' + ' /opt/deepvariant/bin/run_deepvariant --model_type=PACBIO ' + ref_dv +' '+ read +' '+ out +' '+ outg +' '+ interdir +' '+ region +' '+ threads
            #print(cmd)
            #os.system(cmd)
            #DeepVariant 1-pass
            cmd= ['/usr/local/bin/singularity', 'run', '-B /mnt/fs_shared/:/mnt/fs_shared/', '/mnt/fs_shared/deepvariant_1.1.0.sif', '/opt/deepvariant/bin/run_deepvariant',  '--model_type=PACBIO', ref_dv, read, out, outg, interdir, region, threads]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            process.wait()

            ref=REF #'/scratch-shared/tahmad/bio_data/GRCh38/GRCh38.fa'
            dv_out= OUT+'/'+ REGION +'.dv.vcf.gz'
            outphased=WHATSHAPOUTPUT+'/'+REGION+'.phased.vcf.gz'
            chr_phasing='--chromosome='+CHR

            #Whathap phase
            cmd= ['/usr/local/bin/whatshap', 'phase', '--ignore-read-groups', '-o', outphased,  '--reference', ref, chr_phasing, dv_out, inBAM]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            process.wait()

            #Tabix VCF
            tabix='tabix -p vcf '+ outphased
            os.system(tabix)

            outhaplotagged=WHATSHAPOUTPUT+'/'+REGION+'.haplotagged.bam'

            #Whathap haplotag
            cmd= ['/usr/local/bin/whatshap', 'haplotag',  '--ignore-read-groups', '-o', outhaplotagged,  '--reference', ref, outphased,  inBAM]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            process.wait()

            #Index haplotagged
            indexhaplotagged='samtools index ' + outhaplotagged
            os.system(indexhaplotagged)

            out1='--output_vcf='+OUT+'/'+ REGION +'.haplotagged.vcf.gz'
            outg1='--output_gvcf='+WHATSHAPOUTPUT+'/'+ REGION +'.haplotagged.g.vcf.gz'
            readwh='--reads='+outhaplotagged

            #DeepVariant 2-pass
            cmd= ['/usr/local/bin/singularity', 'run', '-B /mnt/fs_shared/:/mnt/fs_shared/', '/mnt/fs_shared/deepvariant_1.1.0.sif', '/opt/deepvariant/bin/run_deepvariant',  '--model_type=PACBIO', ref_dv, readwh, '--use_hp_information', out1, outg1, interdir, region, threads]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            process.wait()

    return 1
#######################################################################################
def runDeepVariantBWA(n_index):
    for index, inBAM in enumerate(BAM_FILES, start=0):
        if index % int(NODES) == n_index:
            #/scratch-shared/tahmad/bio_data/HG002/HG002_chr15:76493391-86493391.bam
            print(inBAM)

            index_path='samtools index -@' + CORES +' '+ inBAM
            os.system(index_path)

            REGION = inBAM.split('_')[-1].replace(".bam", "")
            CHR= inBAM.split('_')[-1].replace(".bam", "").split(':')[0]
            BIN_VERSION="1.1.0"

            OUT=os.path.dirname(inBAM)
            print(REGION)

            DVOUTPUT=OUT+'/outputs/dv_output-'+REGION
            WHATSHAPOUTPUT=OUT+'/outputs/whatshap_output-'+REGION

            if os.path.exists(DVOUTPUT) and os.path.exists(WHATSHAPOUTPUT):
                shutil.rmtree(DVOUTPUT)
                shutil.rmtree(WHATSHAPOUTPUT)

            os.mkdir(DVOUTPUT)
            os.mkdir(WHATSHAPOUTPUT)

            OUTPUT_DIR=DVOUTPUT+'/quickstart-output-'+REGION

            docker='docker://google/deepvariant:'+BIN_VERSION
            interdir='--intermediate_results_dir='+DVOUTPUT+'/intermediate_results_dir'
            ref_dv='--ref='+REF #/scratch-shared/tahmad/bio_data/GRCh38/GRCh38.fa'
            read='--reads='+inBAM
            out='--output_vcf='+OUT+'/'+ REGION +'.dv.vcf.gz'
            outg='--output_gvcf='+DVOUTPUT+'/'+ REGION +'.dv.g.vcf.gz'
            region='--regions='+REGION
            #region='--regions=chr20'
            threads='--num_shards='+CORES

            #cmd='singularity run -B /mnt/fs_shared/:/mnt/fs_shared/ /mnt/fs_shared/deepvariant_1.1.0.sif ' + ' /opt/deepvariant/bin/run_deepvariant --model_type=PACBIO ' + ref_dv +' '+ read +' '+ out +' '+ outg +' '+ interdir +' '+ region +' '+ threads
            #print(cmd)
            #os.system(cmd)
            #DeepVariant 1-pass
            cmd= ['/usr/local/bin/singularity', 'run', '-B /mnt/fs_shared/:/mnt/fs_shared/', '/mnt/fs_shared/deepvariant_1.1.0.sif', '/opt/deepvariant/bin/run_deepvariant',  '--model_type=WGS', ref_dv, read, out, outg, interdir, region, threads]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            process.wait()

    return 1
#########################################################
def runOctopusBWA(n_index):
    for index, inBAM in enumerate(BAM_FILES, start=0):
        if index % int(NODES) == n_index:
            #/scratch-shared/tahmad/bio_data/HG002/HG002_chr15:76493391-86493391.bam
            print(inBAM)

            index_path='samtools index -@' + CORES +' '+ inBAM
            os.system(index_path)

            REGION = inBAM.split('_')[-1].replace(".bam", "")

            OUT=os.path.dirname(inBAM)
            print(REGION)

            ref='--reference='+REF 
            reads='--reads='+inBAM
            out='--output='+OUT+'/'+ REGION +'_octopus.vcf.gz'
            region='--regions='+REGION
            #region='--regions=chr20'
            threads='--threads='+CORES
            err_model='--sequence-error-model=PCRF.NovaSeq'
            forest='--forest=/opt/octopus/resources/forests/germline.v0.7.4.forest'

            #singularity exec octopus_latest.sif octopus --threads 24  --reference /scratch-shared/tahmad/bio_data/GRCh38/GRCh38.fa  --reads /scratch-shared/tahmad/bio_data/NA24385/HG003/HG003_chr20.bam -T chr20 --sequence-error-model PCRF.NovaSeq --forest /opt/octopus/resources/forests/germline.v0.7.4.forest -o /scratch-shared/tahmad/bio_data/NA24385/HG003/octopus.vcf.gz

            cmd= ['/usr/local/bin/singularity', 'exec', '-B /mnt/fs_shared/:/mnt/fs_shared/', '/mnt/fs_shared/octopus_latest.sif', 'octopus',  threads, ref, reads, region, err_model, forest, out]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
            process.wait()

    return 1  
    
#########################################################
def long_reads_write_bam(df, by, part):

    def sam_operations(pdf):

        # importing socket module
        #import socket
        # getting the hostname by socket.gethostname() method
        #hostname = socket.gethostname()

        name = args.path.split('/')[-2] 
        #headerpath=sorted(glob.glob(args.path+'bams/'+name+'_header_'+'*.sam'))
        #headerpath=sorted(glob.glob(args.path+'bams/header.sam'))
        samfile = pysam.AlignmentFile(args.path+'bams/header.sam')
        header = samfile.header
        #print(header)
        samfile.close()
        
        ##/scratch-shared/tahmad/bio_data/FDA/Illumnia/HG003/8/bams/
        sam_name=args.path+'bams/'+name+'_'+part+'.bam'
        print(sam_name)
        len_pdf=len(pdf.index)
        with pysam.AlignmentFile(sam_name, "wb", header=header) as outf:

            for i in range(0, len_pdf-1):
                #if i in duplicates:
                #    res_d.insert(i, sdf.loc[i, 'flags'] | 1024)
                #else:
                #    res_d.insert(i, sdf.loc[i, 'flags'])
                #header = pysam.AlignmentHeader.from_dict(header)
                a = pysam.AlignedSegment()#(header)
                a.query_name = pdf.loc[i, 'qNames'] 

                seq=pdf.loc[i, 'seqs']
                if(seq=='*'):
                    a.query_sequence= ""
                else:
                    a.query_sequence= seq

                a.flag = int(pdf.loc[i, 'flags'])
                #a.reference_name=pdf.loc[i, 'rIDs']
                
                a.reference_start = int(pdf.loc[i, 'beginPoss'])-1
                #print(a.reference_start)
                a.mapping_quality = int(pdf.loc[i, 'mapQs'])
                a.cigarstring = pdf.loc[i, 'cigars']
                #a.next_reference_name=pdf.loc[i, 'rNextIds']

                pNextsvalue=int(pdf.loc[i, 'pNexts'])
                if(pNextsvalue ==0):
                    a.next_reference_start= -1 #int(pdf.loc[i, 'pNexts'])
                else:
                    a.next_reference_start=pNextsvalue

                a.template_length= int(pdf.loc[i, 'tLens'])
                if(seq=='*'):
                    a.query_qualities= ""
                else:
                    a.query_qualities = pysam.qualitystring_to_array(pdf.loc[i, 'quals'])
                #a.tags = (("NM", 1), ("RG", "L1"))
                items = pdf.loc[i, 'tagss'].split('\'')
                #print(items)
                for item in items[1:]:
                    sub = item.split(':')
                    a.set_tag(sub[0], sub[-1])

                #a.reference_name=pdf.loc[i, 'rIDs']
                #try:
                #    a.next_reference_name=pdf.loc[i, 'rNextIds']
                #except:
                #    a.next_reference_name="*"
                rid=pdf.loc[i, 'rIDs']
                if(rid=="chrM"):
                    a.reference_id=-1
                elif(rid=="chrX"):
                    a.reference_id=22
                elif(rid=="chrY"):
                    a.reference_id=23
                else:
                    chrno=rid.replace("chr", "")
                    a.reference_id = int(chrno)-1
                '''
                rnid=pdf.loc[i, 'rNextIds']
                if(rnid=="chrM"):
                    a.next_reference_id=-1
                elif(rnid=="chrX"):
                    a.next_reference_id=22
                elif(rnid=="chrY"):
                    a.next_reference_id=23
                else:
                    chrno=rnid.replace("chr", "")
                    a.next_reference_id = int(chrno)-1
                '''
                a.next_reference_id = -1

                outf.write(a)

        index_path='samtools index ' + sam_name
        os.system(index_path)

        return  pdf[['flags']] #pdf.loc[:, 'flags'] #pd.DataFrame({'flags': res_d})

    return df.groupby(by).applyInPandas(sam_operations, schema="flags string")

#########################################################
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
def short_reads_write_bam(df, by, part):

    def sam_operations(pdf):

        # importing socket module
        #import socket
        # getting the hostname by socket.gethostname() method
        #hostname = socket.gethostname()

        #********************************************************#

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
            if pdf.loc[i+1, 'beginPoss'] == pdf.loc[i, 'beginPoss'] and pdf.loc[i+1, 'pNexts'] == pdf.loc[i, 'pNexts']:
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
                res_d.insert(i, pdf.loc[i, 'flags'])

            if process_next == True and process_start == True:
                #sdf = pd.DataFrame(data)
                #rq = sdf["read_qual"]
                read_qual_index = read_qual_d.index( max(read_qual_d) )

                for i in range(0, len(read_qual_d)):
                    if not i == read_qual_index:
                        pdf.loc[i, 'flags']= pdf.loc[i, 'flags'] | 1024

                process_next = False
                process_start = False

                index_d.clear()
                read_qual_d.clear()
                j=0

        #********************************************************#

        name = args.path.split('/')[-2] 
        headerpath=sorted(glob.glob(args.path+'bams/header_'+'*.sam'))
        #headerpath=sorted(glob.glob(args.path+'bams/header.sam'))
        samfile = pysam.AlignmentFile(headerpath[1])
        header = samfile.header
        #print(header)
        samfile.close()
        
        ##/scratch-shared/tahmad/bio_data/FDA/Illumnia/HG003/8/bams/
        sam_name=args.path+'bams/'+name+'_'+part+'.bam'
        print(sam_name)
        #len_pdf=len(pdf.index)
        with pysam.AlignmentFile(sam_name, "wb", header=header) as outf:

            for i in range(0, len_pdf-1):
                #if i in duplicates:
                #    res_d.insert(i, sdf.loc[i, 'flags'] | 1024)
                #else:
                #    res_d.insert(i, sdf.loc[i, 'flags'])
                #header = pysam.AlignmentHeader.from_dict(header)
                a = pysam.AlignedSegment()#(header)
                a.query_name = pdf.loc[i, 'qNames'] 

                seq=pdf.loc[i, 'seqs']
                if(seq=='*'):
                    a.query_sequence= ""
                else:
                    a.query_sequence= seq

                a.flag = pdf.loc[i, 'flags']
                #a.reference_name=pdf.loc[i, 'rIDs']
                
                a.reference_start = pdf.loc[i, 'beginPoss']-1
                #print(a.reference_start)
                a.mapping_quality = pdf.loc[i, 'mapQs']
                a.cigarstring = pdf.loc[i, 'cigars']
                #a.next_reference_name=pdf.loc[i, 'rNextIds']

                pNextsvalue=pdf.loc[i, 'pNexts']
                if(pNextsvalue ==0):
                    a.next_reference_start= -1 #int(pdf.loc[i, 'pNexts'])
                else:
                    a.next_reference_start=pNextsvalue

                a.template_length= pdf.loc[i, 'tLens']
                if(seq=='*'):
                    a.query_qualities= ""
                else:
                    a.query_qualities = pysam.qualitystring_to_array(pdf.loc[i, 'quals'])
                #a.tags = (("NM", 1), ("RG", "L1"))
                items = pdf.loc[i, 'tagss'].split('\'')
                #print(items)
                for item in items[1:]:
                    sub = item.split(':')
                    a.set_tag(sub[0], sub[-1])

                #a.reference_name=pdf.loc[i, 'rIDs']
                #try:
                #    a.next_reference_name=pdf.loc[i, 'rNextIds']
                #except:
                #    a.next_reference_name="*"
                '''
                rid=pdf.loc[i, 'rIDs']
                if(rid=="chrM"):
                    a.reference_id=-1
                elif(rid=="chrX"):
                    a.reference_id=22
                elif(rid=="chrY"):
                    a.reference_id=23
                else:
                    chrno=rid.replace("chr", "")
                    a.reference_id = chrno-1
                '''
                a.reference_id=pdf.loc[i, 'rIDs']-1
                '''
                rnid=pdf.loc[i, 'rNextIds']
                if(rnid=="chrM"):
                    a.next_reference_id=-1
                elif(rnid=="chrX"):
                    a.next_reference_id=22
                elif(rnid=="chrY"):
                    a.next_reference_id=23
                else:
                    chrno=rnid.replace("chr", "")
                    a.next_reference_id = int(chrno)-1
                '''
                a.next_reference_id = -1

                outf.write(a)

        index_path='samtools index ' + sam_name
        os.system(index_path)

        return  pdf[['flags']] #pdf.loc[:, 'flags'] #pd.DataFrame({'flags': res_d})

    return df.groupby(by).applyInPandas(sam_operations, schema="flags integer")

#########################################################
def short_reads_write_bam_alt(df, by, part):

    def sam_operations(pdf):

        # importing socket module
        #import socket
        # getting the hostname by socket.gethostname() method
        #hostname = socket.gethostname()

        #********************************************************#
                
        flag_reg=[]
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
            if pdf.loc[i+1, 'beginPoss'] == pdf.loc[i, 'beginPoss'] and pdf.loc[i+1, 'pNexts'] == pdf.loc[i, 'pNexts']:
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
                res_d.insert(i, pdf.loc[i, 'flags'])

            if process_next == True and process_start == True:
                #sdf = pd.DataFrame(data)
                #rq = sdf["read_qual"]
                read_qual_index = read_qual_d.index( max(read_qual_d) )

                for i in range(0, len(read_qual_d)):
                    if not i == read_qual_index:
                        pdf.loc[i, 'flags']= pdf.loc[i, 'flags'] | 1024
                    #else:
                    #    flag_reg.insert(i, pdf.loc[i, 'flags'])
                process_next = False
                process_start = False

                index_d.clear()
                read_qual_d.clear()
                j=0
        
        #********************************************************#

        name = args.path.split('/')[-2] 
        headerpath=sorted(glob.glob(args.path+'bams/header_'+'*.sam'))
        #headerpath=sorted(glob.glob(args.path+'bams/header.sam'))
        samfile = pysam.AlignmentFile(headerpath[1])
        header = samfile.header
        #print(header)
        samfile.close()
 

        ##/scratch-shared/tahmad/bio_data/FDA/Illumnia/HG003/8/bams/
        sam_name=args.path+'bams/'+name+'_'+part+'.bam'
        print(sam_name)
        #len_pdf=len(pdf.index)

        table=pa.Table.from_pandas(pdf)
        d = table.to_pydict()
        with pysam.AlignmentFile(sam_name, "wb", header=header) as outf:
            for qNames,flags,rIDs,beginPoss, mapQs,cigars,rNextIds,pNexts,tLens,seqs,quals,tagss in zip(d['qNames'], d['flags'], d['rIDs'], d['beginPoss'], d['mapQs'], d['cigars'], d['rNextIds'], d['pNexts'], d['tLens'], d['seqs'], d['quals'], d['tagss']):
        

        

            #for i in range(0, len_pdf-1):
                #if i in duplicates:
                #    res_d.insert(i, sdf.loc[i, 'flags'] | 1024)
                #else:
                #    res_d.insert(i, sdf.loc[i, 'flags'])
                #header = pysam.AlignmentHeader.from_dict(header)
                a = pysam.AlignedSegment()#(header)
                a.query_name = qNames #pdf.loc[i, 'qNames'] 

                seq=seqs #pdf.loc[i, 'seqs']
                if(seq=='*'):
                    a.query_sequence= ""
                else:
                    a.query_sequence= seq

                a.flag = flags #pdf.loc[i, 'flags']
                #a.reference_name=pdf.loc[i, 'rIDs']
                
                a.reference_start = beginPoss-1 #pdf.loc[i, 'beginPoss']-1
                #print(a.reference_start)
                a.mapping_quality = mapQs #pdf.loc[i, 'mapQs']
                a.cigarstring = cigars #pdf.loc[i, 'cigars']
                #a.next_reference_name=pdf.loc[i, 'rNextIds']

                pNextsvalue=pNexts #pdf.loc[i, 'pNexts']
                if(pNextsvalue ==0):
                    a.next_reference_start= -1 #int(pdf.loc[i, 'pNexts'])
                else:
                    a.next_reference_start=pNextsvalue

                a.template_length= tLens #pdf.loc[i, 'tLens']
                if(seq=='*'):
                    a.query_qualities= ""
                else:
                    a.query_qualities = pysam.qualitystring_to_array(quals) #pdf.loc[i, 'quals'])
                #a.tags = (("NM", 1), ("RG", "L1"))
                items = tagss.split('\'') #pdf.loc[i, 'tagss'].split('\'')
                #print(items)
                for item in items[1:]:
                    sub = item.split(':')
                    a.set_tag(sub[0], sub[-1])

                #a.reference_name=pdf.loc[i, 'rIDs']
                #try:
                #    a.next_reference_name=pdf.loc[i, 'rNextIds']
                #except:
                #    a.next_reference_name="*"
                '''
                rid=pdf.loc[i, 'rIDs']
                if(rid=="chrM"):
                    a.reference_id=-1
                elif(rid=="chrX"):
                    a.reference_id=22
                elif(rid=="chrY"):
                    a.reference_id=23
                else:
                    chrno=rid.replace("chr", "")
                    a.reference_id = chrno-1
                '''
                a.reference_id=rIDs-1 #pdf.loc[i, 'rIDs']-1
                '''
                rnid=pdf.loc[i, 'rNextIds']
                if(rnid=="chrM"):
                    a.next_reference_id=-1
                elif(rnid=="chrX"):
                    a.next_reference_id=22
                elif(rnid=="chrY"):
                    a.next_reference_id=23
                else:
                    chrno=rnid.replace("chr", "")
                    a.next_reference_id = int(chrno)-1
                '''
                a.next_reference_id = -1

                outf.write(a)

        index_path='samtools index ' + sam_name
        os.system(index_path)

        return  pdf[['flags']] #pdf.loc[:, 'flags'] #pd.DataFrame({'flags': res_d})

    return df.groupby(by).applyInPandas(sam_operations, schema="flags integer")

#########################################################
def arrow_data_collection(path):
    with pa.RecordBatchFileReader(path) as reader:
        batch = reader.get_batch(0)
    #print(batch.to_pandas())
    return batch

def process_output(itern):
    arrow_file_list = sorted(glob.glob(args.path+'arrow/'+CHRMS[itern]+"_" + "*.arrow"))
    #print(arrow_file_list)
    ardd = spark.sparkContext.parallelize(arrow_file_list, len(arrow_file_list)).map(arrow_data_collection)
    df = spark.createFromArrowRecordBatchesRDD(ardd).orderBy('beginPoss', ascending=True).coalesce(1)
    if(args.aligner=="Minimap2"):
        df = df.select(split(df.sam,"\t")).rdd.flatMap(lambda x: x).toDF(schema=["qNames","flags","rIDs","beginPoss", "mapQs", "cigars", "rNextIds", "pNexts", "tLens", "seqs", "quals", "tagss"])
    #print(df)
    if(args.aligner=="Minimap2"):
        long_reads_write_bam(df, by="rIDs", part=arrow_file_list[0].split('_')[-2]).show() #
    else:
        short_reads_write_bam_alt(df, by="rIDs", part=arrow_file_list[0].split('_')[-2]).show() #
    df.unpersist(True)
    
    return 1
#######################################################

def run_Minimap2(fqfile):
    print(fqfile)

    ## importing socket module
    import socket
    ## getting the hostname by socket.gethostname() method
    hostname = socket.gethostname()

    name=args.path.split('/')[-2]
    plfm=args.path.split('/')[-3]
    rg='@RG\\tID:'+name+'\\tSM:sample\\tPL:'+plfm+'\\tLB:sample\\tPU:lane' 
    print(rg)
    os.system('whoami')
    #access='sudo -S chmod -R 777 '+args.path+'bams/' 
    #os.system(access)
    
    out = name+'_header_'+hostname+'.sam' 
    out = args.path+'parts/'+name+'_header_'+hostname+'.sam' 

    rg='@RG\tID:'+name+'\tSM:sample\tPL:'+plfm+'\tLB:sample\tPU:lane'
    #cmd='minimap2 -a -k 19 -O 5,56 -E 4,1 -B 5 -z 400,50 -r 2k --eqx --secondary=no '+ ' -t '+ CORES + ' '+ REF + ' ' + fqfile + ' -o ' + out
    cmd = ['/usr/local/bin/minimap2', '-a',  '-k 19',  '-O 5,56',  '-E 4,1',  '-B 5',  '-z 400,50',  '-r 2k',  '--eqx',  '--secondary=no', '-R', rg, '-t', CORES, REF, fqfile, '-o', out]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()

    return hostname

def streaming_Minimap2(path):
    os.system('whoami')
    #/scratch-shared/tahmad/bio_data/FDA/HG002/
    file_name = path.split('/')[-2]
    #parts=path.split('/')[-1]
    cmd = 'seqkit split2 --threads='+CORES+ ' ' +args.path + file_name +'.fastq -p '+NODES+' -O '+ args.path + 'parts' +' -f'
    os.system(cmd)
    return 1

def process_Minimap2(path):
    time.sleep(15)
    #/scratch-shared/tahmad/bio_data/FDA/HG002/
    file_name = path.split('/')[-2]
    file_names = sorted(glob.glob(args.path+'parts/'+file_name+".part_"+ "*.fastq"))
    frdd = spark.sparkContext.parallelize(file_names, len(file_names))
    frdd = frdd.map(run_Minimap2).collect()
    return 1

#########################################################

#########################################################

def run_BWA(fqfile):
    print(fqfile)
    ## importing socket module
    import socket
    ## getting the hostname by socket.gethostname() method
    hostname = socket.gethostname()
   
    name=args.path.split('/')[-2]
    plfm='Illumnina'#args.path.split('/')[-3]
 
    out = args.path+'bams/header'+'_'+hostname+'.sam'
    rg='@RG\\tID:'+name+'\\tSM:sample\\tPL:'+plfm+'\\tLB:sample\\tPU:lane'

    cmd = ['/usr/local/bin/bwa', 'mem', '-R', rg, '-t', CORES, REF, fqfile[0], fqfile[1], '-o', out]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    process.wait()

    return hostname

def streaming_BWA(path):
    #/scratch-shared/tahmad/bio_data/FDA/HG002/
    file_name = path.split('/')[-2]
    #sudo seqkit split2 --threads=8 -1 /mnt/fs_shared/query/ERR001268/ERR001268_1.fastq -2 /mnt/fs_shared/query/ERR001268/ERR001268_2.fastq -p 2 -O /mnt/fs_shared/query/ERR001268/parts -f
    cmd = 'seqkit split2 --threads='+ CORES +' -1 '+ path + file_name +'_1.fastq -2 '+ path + file_name  +'_2.fastq -p '+NODES+' -O '+ args.path + 'parts' +' -f'
    #os.system(cmd)
    return 1

def process_BWA(path):
    time.sleep(5)
    #/scratch-shared/tahmad/bio_data/FDA/HG002/
    name = path.split('/')[-2]
    file_names_r1 = sorted(glob.glob(args.path+'parts/'+name+"_1.part_"+ "*.fastq"))
    file_names_r2 = sorted(glob.glob(args.path+'parts/'+name+"_2.part_"+ "*.fastq"))
    fqfile = list(zip(file_names_r1, file_names_r2))
    frdd = spark.sparkContext.parallelize(fqfile, len(fqfile))
    frdd = frdd.map(run_BWA).collect()
    return 1

#########################################################
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
requiredNamed = parser.add_argument_group('Required arguments')

parser.add_argument("-standalone",  "--standalone",  help="Standalone mode.", type=str, default='no')
parser.add_argument("-utilspath",  "--utilspath",  help="Utils path.", type=str, default='no')

requiredNamed.add_argument("-part",  "--part",  help="Part number of pipeline", required=True)
requiredNamed.add_argument("-ref",  "--ref",  help="Reference genome file with .fai index", required=True)
requiredNamed.add_argument("-nodes",  "--nodes",  help="Number of nodes", required=True)
requiredNamed.add_argument("-path",  "--path",  help="Input FASTQ path", required=True)
requiredNamed.add_argument("-cores",  "--cores",  help="Number od cores on each node", required=True)
requiredNamed.add_argument("-aligner",  "--aligner",  help="Aligner (BWA or Minimap2)", required=True)
requiredNamed.add_argument("-vcaller",  "--vcaller",  help="Variant Caller (DeepVariant or Octopus)", required=True)

args = parser.parse_args()

CORES=args.cores
NODES=args.nodes
REF=args.ref
BAM_FILES=[]
#########################################################
def hostname(x):
    ## importing socket module
    import socket
    ## getting the hostname by socket.gethostname() method
    hostname = socket.gethostname()
    ## getting the IP address using socket.gethostbyname() method
    ip_address = socket.gethostbyname(hostname)
    return [hostname,ip_address]
#########################################################

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Variant calling workflow") \
        .getOrCreate()

    # Enable Arrow-based columnar data transfers
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    print("BWA started...")
    outIDs = spark.sparkContext.parallelize([0,1],2).map(lambda x: hostname(x)).collect() 
    print(outIDs)
    
    #path="/scratch-shared/tahmad/bio_data/FDA/Illumina/HG003/"
    OUTDIR=args.path
    ARROWOUT=OUTDIR+'arrow'
    BAMOUT=OUTDIR+'bams'
    PARTSOUT=OUTDIR+'parts'

    #frdd = spark.sparkContext.parallelize(["a","b"], 2)
    #frdd = frdd.map(run_Minimap2).collect()

    if(args.part=="1"):    
        ####################################
        start = time.time()
        #################################### 
        print("Part-1")
           
        #if os.path.exists(ARROWOUT):
        #    shutil.rmtree(ARROWOUT)
        #if os.path.exists(BAMOUT):
        #    shutil.rmtree(BAMOUT)
        #if os.path.exists(PARTSOUT):
        #    shutil.rmtree(PARTSOUT)
        #try:
        #    os.mkdir(PARTSOUT)
        #except OSError:
        #    print ("Creation of the directory %s failed" % PARTSOUT)
        #else:
        #    print ("Successfully created the directory %s " % PARTSOUT)
        #try:
        #    os.mkdir(ARROWOUT)
        #except OSError:
        #    print ("Creation of the directory %s failed" % ARROWOUT)
        #else:
        #    print ("Successfully created the directory %s " % ARROWOUT)

        #try:
        #    os.mkdir(BAMOUT)
        #except OSError:
        #    print ("Creation of the directory %s failed" % BAMOUT)
        #else:
        #    print ("Successfully created the directory %s " % BAMOUT)
        
        if(args.aligner=="Minimap2"):
        
            job1 = multiprocessing.Process(target=streaming_Minimap2, args=(args.path,))
            job1.start()
            job2 = multiprocessing.Process(target=process_Minimap2, args=(args.path,))
            job2.start()
            # Wait for both jobs to finish
            job1.join()
            job2.join()
        
            #process_Minimap2(args.path)

        elif(args.aligner=="BWA"):
            job1 = multiprocessing.Process(target=streaming_BWA, args=(args.path,))
            job1.start()
            job2 = multiprocessing.Process(target=process_BWA, args=(args.path,))
            job2.start()
            # Wait for both jobs to finish
            job1.join()
            job2.join()
        
        ####################################
        stop = time.time()
        print('minimap2 took {} seconds.'
              .format(stop - start))
        ####################################
    elif(args.part=="2"):
        print("Part-2")
        
        SparkSession.createFromArrowRecordBatchesRDD = createFromArrowRecordBatchesRDD

        ####################################
        start = time.time()
        ####################################

        PARTS=128

        #for x in range(0, PARTS, int(NODES)+8):
        #    p = ThreadPool(int(NODES)+8)
        #    p.map(process_output, range(x, int(NODES)+8+x, 1))

        p = ThreadPool(10)
        p.map(process_output, range(10))

        #process_output(0)
        ####################################
        stop = time.time()
        print('BAM output took {} seconds.'
              .format(stop - start))
        ####################################
        
    elif(args.part=="3"):
        print("Part-3")
        '''
        if(args.standalone=="yes"):
            name = args.path.split('/')[-2]
            chunking_cmd=args.utils+'chunking.sh '+ CORES +' '+ args.path+name
            process = subprocess.Popen(chunking_cmd, stdout=subprocess.PIPE)
            process.wait()
        
        files_bam = sorted(glob.glob(BAMOUT+'*_chr'+'*.bam'), key=os.path.getsize)

        #for x in range(0, PARTS, int(NODES)):

        #    files_bam_part=files_bam[x:x+int(NODES)]
        #    brdd = spark.sparkContext.parallelize(files_bam_part, len(files_bam_part))
        #    brdd = brdd.map(runDeepVariant).collect()
        '''
        BAM_FILES=sorted(glob.glob(args.path+'bams/'+'*.bam'), key=os.path.getsize)
        print(BAM_FILES)
        brdd = spark.sparkContext.parallelize(list(range(int(NODES))), int(NODES))

        if(args.vcaller=='DeepVariant'):
            brdd = brdd.map(runDeepVariantBWA).collect()
        elif(args.vcaller=='Octopus'):
            brdd = brdd.map(runOctopusBWA).collect()

        #runDeepVariant(sorted(glob.glob(args.path+'bams/*.bam'))[0])
    
    elif(args.part=="4"):
        print("Part-4")
        mergevcf=args.path+'vcfmerge.sh '+args.path+'bams/'
        os.system(mergevcf)

        cmd=['/usr/local/bin/singularity', 'exec', '-B /mnt/fs_shared/:/mnt/fs_shared/', '/mnt/fs_shared/hap.py_v0.3.12.sif', '/opt/hap.py/bin/hap.py', '/mnt/fs_shared/benchmarks/HG003/HG003_GRCh38_1_22_v4.2_benchmark.vcf.gz', '/mnt/fs_shared/query/HG003/bams/query.dv_merged.vcf', '-f /mnt/fs_shared/benchmarks/HG003/HG003_GRCh38_1_22_v4.2_benchmark.bed', '-r /mnt/fs_shared/reference/GRCh38.fa', '-o /mnt/fs_shared/benchmarks/HG003/happy.output', '--pass-only', '-l chr20', '--engine=vcfeval', '--threads=2']
        #process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        #process.wait()

        t_vcf='/mnt/fs_shared/benchmarks/HG003/HG003_GRCh38_1_22_v4.2_benchmark.vcf.gz'
        o_vcf='/mnt/fs_shared/query/HG003/bams/query.dv_merged.vcf.gz'
        t_bed='/mnt/fs_shared/benchmarks/HG003/HG003_GRCh38_1_22_v4.2_benchmark.bed'
        ref='/mnt/fs_shared/reference/GRCh38.fa'
        
        
        cmd='/usr/local/bin/singularity exec -B /mnt/fs_shared/:/mnt/fs_shared/ /mnt/fs_shared/hap.py_latest.sif /opt/hap.py/bin/hap.py '+ t_vcf +' '+ o_vcf + ' -f '+ t_bed+ ' -r '+ ref+ ' -o ' + args.path+'happy.output --pass-only -l chr20 --engine=vcfeval --threads='+CORES
        os.system(cmd)

    spark.stop()
