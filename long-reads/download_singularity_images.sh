
cd /scratch-shared/tahmad/images

#Long reads aligners
singularity pull docker://quay.io/biocontainers/lra:1.3.2--h00214ad_1

#Long reads variant callers
singularity pull docker://kishwars/pepper_deepvariant:r0.7
singularity pull docker://google/deepvariant:1.3.0-gpu
singularity pull docker://google/deepvariant:1.3.0
singularity pull docker://ontresearch/medaka:latest

singularity pull docker://dancooke/octopus
