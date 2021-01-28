# SVCall

## How to run 
1. Install [Singularity](https://sylabs.io/docs/) container
2. Download our Singularity [script](https://github.com/abs-tudelft/arrow-gen/tree/master/Singularity) and generate singularity image (this image contains all Arrow related packges necessary for building/compiling BWA-MEM)
3. Now enter into generated image using command:
         
        sudo singularity shell <image_name>.simg
