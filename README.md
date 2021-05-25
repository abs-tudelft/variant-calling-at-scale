# SVCall

SVCall is a scalable, parallel and efficient implementation of next generation sequencing data pre-processing and variant calling workflows. Our design tightly integrates most pre-processing workflow stages, using Spark built-in functions to sort reads by coordinates, and mark duplicates efficiently. A cluster scaled DeepVariant for both CPU-only and CPU+GPU clusters is also integrated in this workflow. 

## Apache Arrow Dependencies
 - [C++ libraries](https://github.com/apache/arrow/tree/master/cpp)
 - [C bindings using GLib](https://github.com/apache/arrow/tree/master/c_glib)
 - [Plasma Object Store](https://github.com/apache/arrow/tree/master/cpp/src/plasma): a
   shared-memory blob store, part of the C++ codebase
 - [Python libraries](https://github.com/apache/arrow/tree/master/python)

## Installation Requirements
 - Ubuntu 16.04/18.04
 - Apache Spark 3.0.1
 - [Singularity](https://sylabs.io/docs/) container
 
## Configuration & Build

1. Install [Singularity](https://sylabs.io/docs/) container
2. Download our Singularity [script](https://github.com/abs-tudelft/arrow-gen/tree/master/Singularity) and generate singularity image (this image contains all Arrow related packges necessary for building/compiling BWA-MEM)
3. Now enter into generated image using command:
         
        sudo singularity shell <image_name>.simg

## Standalone pre-processing on clusters:
FASTQ data is streamed to BWA on every cluster node, BWA output is piped into Sambamba to perform sorting, duplicates removal option is also available, if enabled sorted data is piped to this stage as well. For final output, Samtools (merge) is used to produces a single BAM output, ready for further down stream analysis.
- BWA (alignment) `->` Sambamba (Sorting) `->` Duplicates removal (optional) `->` Samtools (merge BAMs)
