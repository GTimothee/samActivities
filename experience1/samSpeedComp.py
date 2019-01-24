import nibabel as nib
import numpy as np
import imageutils as iu
import os, sys
import math
from time import time
from enum import Enum
from shutil import copyfile
import csv

#Program to test the speed of splitting and merging on both HDD and tmpfs

class Strategy(Enum):
    NAIVE = 1
    MULTIPLE = 2
    CLUSTERED = 3

traductor = {
        'NAIVE':'clustered',
        'CLUSTERED':'clustered',
        'MULTIPLE':'multiple'
        }

def extractBigBrainSamples(bigBrainPath, outputDir, nbSamples, sampleMaxSize):
    """ Extract file samples from the big brain nii.gz file using nibabel.
    Several samples will allow to do several times the same tests but using different data.

    Args:
        bigBrainPath: Path to the big brain file (nii.gz extension).
        outputDir: Path to output directory in which to store the samples.
        nbSamples: Number of samples to extract from the image.
        sampleMaxSize: Maximum size of a slab in terms of memory (in Gigabytes)
    """

    if not os.path.isfile(bigBrainPath):
        print(bigBrainPath + " not found.")
        return

    if not os.path.isdir(outputDir):
        print(outputDir + " not found.")
        return

    #get a proxy to the big brain file
    proxy = nib.load(bigBrainPath) #proxy image from nibabel

    #get info about the image
    sliceWidth = proxy.shape[0]
    sliceDepth = proxy.shape[1]
    sliceByteSize = proxy.dataobj[...,0].nbytes /1000000000.0  #size of a slice in GB

    print("One slice shape:" + str(proxy.shape))
    print("Slice width : " + str(sliceWidth))
    print("Slice depth : " + str(sliceDepth))
    print("Size of a slice in Bytes :" + str(sliceByteSize))

    nbSlicePerSample = int(math.floor(float(sampleMaxSize)/sliceByteSize))

    startIndex = 0
    endIndex = nbSlicePerSample
    sampleIndex =0
    while endIndex <= nbSlicePerSample * nbSamples:

        #extract a slab, create new img and save
        dataSlabSample = proxy.dataobj[...,startIndex:endIndex]
        array_img = nib.Nifti1Image(dataSlabSample, proxy.affine) #create new nifti img
        nib.save(array_img, os.path.join(outputDir,'bigBrainSample' + str(sampleIndex) + '.nii'))
        print("Processed slab number " + str(sampleIndex))

        #print info to the user
        print(startIndex)
        print(endIndex)
        print(nbSlicePerSample*nbSamples)

        #continue
        startIndex=endIndex
        endIndex+=nbSlicePerSample
        sampleIndex+=1

    print("Number slices per sample :" + str(nbSlicePerSample))
    return nbSlicePerSample

def applySplit(filePath, outputDir, outputFileName, slabWidth, strategy):
    """ Split a nii input file using sam/imageutils.

    Args:
        filePath: Path to the nii input file that we want to split.
        outputDir: Path to the output directory in which to store the created splits.
        outputFileName: Output file name of the created splits.
        strategy: Algorithm to use to split.
    """

    if not os.path.isfile(filePath):
        print(filePath + " not found.")
        return

    if not os.path.isdir(outputDir):
        print(outputDir + " not found.")
        return

    img = iu.ImageUtils(filepath=filePath)

    #algorithm selector
    applySplit={
            Strategy.NAIVE: lambda img, slabWidth, outputDir, outputFileName : img.split(
                first_dim=770,
                second_dim=605,
                third_dim=slabWidth,
                local_dir=outputDir,
                filename_prefix=outputFileName),

            Strategy.MULTIPLE: lambda img, slabWidth, outputDir, outputFileName:img.split_multiple_writes(
                Y_splits=10, Z_splits=5, X_splits=2,
                out_dir=outputDir,
                mem=12*1024**3,
                filename_prefix=outputFileName,
                extension="nii"),

            Strategy.CLUSTERED:lambda img, slabWidth, outputDir, outputFileName:img.split_clustered_writes(
                Y_splits=10, Z_splits=5, X_splits=2,
                out_dir=outputDir,
                mem=12*1024**3,
                filename_prefix=outputFileName,
                extension="nii")
    }

    t=time()
    applySplit[Strategy[strategy]](img, slabWidth, outputDir, outputFileName) #run the appropriate split algorithm
    t=time()-t
    print("Processing time to split " + filePath + " using " + str(strategy) + ": " + str(t) + " seconds.")

def buildDataSamples():
    """ Split the big brain into slabs
    """
    print("Splitting Big Brain...")

    bigBrainPath = os.path.join("/data", "bigbrain_40microns.nii.gz")
    outputDir = "bigBrainGenSamples" #generated samples

    slabWidth = extractBigBrainSamples(bigBrainPath=bigBrainPath,
                                        outputDir=outputDir,
                                        nbSamples=5,
                                        sampleMaxSize=3.0)
    print("Done. Slab width : " + str(slabWidth))

def argsManager():
    """ Parser to manage command line arguments.

    Return:
        args: List of parsed arguments and associated values.
    """
    parser = argparse.ArgumentParse(description="Benchmarking program to evaluate split/merge efficiency.")

    parser.add_argument("hddWorkdirPath",
                            help="",
                            type="str")
    parser.add_argument("tmpfsWorkdirPath",
                            help="",
                            type="str")
    parser.add_argument("outputCsvFilePath"
                            help="",
                            type="str")
    parser.add_argument("configFilePath",
                            help="",
                            type="str")

    return parser.parse_args()

def evaluate(hddWorkdirPath, tmpfsWorkdirPath):
    """Main program which aimed, for each sample of big brain, at splitting it and then re-merging it.
    We split and merge each sample of big brain on both EXT and TPMFS file systems.

    Args:
        hddWorkdirPath:Work directory for the EXT file system.
        tmpfsWorkdirPath: Work directory for the TMPFS file system.
    """

    samplesDir = "bigBrainGenSamples"

    csvFile = open(csvFilePath, "w")

    #for each input file (sample of big brain)
    for fileName in os.listdir(hddWorkdirPath + "/" + samplesDir):
        if not fileName.endswith("nii"):# or fileName.endswith("bigBrainSample0.nii"):
            continue

        filePathTmpfs = os.path.join(tmpfsWorkdirPath, samplesDir, fileName)
        filePathHdd = os.path.join(hddWorkdirPath, samplesDir, fileName)

        outputDirPath = "splittedSamples"
        mergeFileName = fileName + "Merged.nii"
        legendFileName = "legend.txt"
        splitName =  os.path.splitext(fileName)[0] + "Split"
        slabWidth=128

        #temporary copy the file to split and merge on tmpfs device
        copyfile(filePathHdd, filePathTmpfs)

        #for each algorithm
        for strategy in list(Strategy):

            #split on TMPFS
            applySplit(filePath=filePathTmpfs,
                    outputDir=os.path.join(tmpfsWorkdirPath, outputDirPath),
                    outputFileName=traductor[strategy.name], #splitName,
                    slabWidth=slabWidth,
                    strategy=strategy.name)

            #merge on TMPFS
            applyMerge(outputFilePath=os.path.join(tmpfsWorkdirPath, outputDirPath, mergeFileName),
                                legendFilePath=os.path.join(tmpfsWorkdirPath, outputDirPath, legendFileName),
                                strategy= strategy.name,
                                nbSlices=slabWidth)

            for filename in os.listdir(os.path.join(tmpfsWorkdirPath, outputDirPath)): #empty output dir on tmpfs
                os.remove(os.path.join(tmpfsWorkdirPath, outputDirPath, filename))

            #split on EXT
            applySplit(filePath=filePathHdd,
                    outputDir = os.path.join(hddWorkdirPath, outputDirPath),
                    outputFileName=traductor[strategy.name], #splitName,
                    slabWidth=slabWidth,
                    strategy=strategy.name)

            #merge on EXT
            applyMerge(outputFilePath=os.path.join(hddWorkdirPath, outputDirPath, mergeFileName),
                                legendFilePath=os.path.join(hddWorkdirPath, outputDirPath, legendFileName),
                                strategy=strategy.name,
                                nbSlices=slabWidth)

            for filename in os.listdir(os.path.join(hddWorkdirPath, outputDirPath)):
                os.remove(os.path.join(hddWorkdirPath, outputDirPath, filename))

            print("\n")

        os.remove(filePathTmpfs) #delete temporary input file to split and merge from tpmfs device


def applyMerge(outputFilePath, legendFilePath, strategy, nbSlices):
    """ Merge splits that have previously been created from a nii image in order to rebuild the input nii image.
    The splits have been saved into an output folder with a 'legend' text file containing the indexes of the splits to be merged.

    Args:
        outputFilePath: Path and name of the output file to be created by merging the splits of the sample being merged.
        legendFilePath: Path and name of the legend file that contains thenames of the splits of the sample being merged.
        strategy: Algorithm to use for the merging (see Strategy enum at the top of the file).
        nbSlices: Number of slices in the sample being merged. It depends on the extractBigBrainSamples function (in this file).
    """

    #open a new image object
    img=iu.ImageUtils(outputFilePath,
                        first_dim=3850,
                        second_dim=3025,
                        third_dim=nbSlices,
                        dtype=np.uint16)

    mem=12*1024**3
    applyMerge = {
            Strategy.NAIVE: lambda legendFilePath, mem : img.reconstruct_img(legendFilePath, "clustered", 0),
            Strategy.CLUSTERED: lambda legendFilePath, mem : img.reconstruct_img(legendFilePath, "clustered", mem),
            Strategy.MULTIPLE: lambda legendFilePath, mem : img.reconstruct_img(legendFilePath, "multiple", mem)
    }

    #apply the merge
    t=time()
    applyMerge[Strategy[strategy]](legendFilePath, mem)
    t=time()-t
    print("Processing time to merge " + outputFilePath + " using " + str(strategy) +  ": " + str(t) + " seconds." )

if __name__ == "__main__":
    hddWorkdirPath="/data/tguedon/samActivities"
    tmpfsWorkdirPath="/dev/shm/tguedon"

    #buildDataSamples()
    evaluate(hddWorkdirPath, tmpfsWorkdirPath)

