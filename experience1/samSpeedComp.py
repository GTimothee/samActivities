import nibabel as nib
import numpy as np
import imageutils as iu
import os, sys
import math
from time import time
from enum import Enum
from shutil import copyfile
import csv
import json
import argparse

#Program to test the speed of splitting and merging on both HDD and tmpfs

class Strategy(Enum):
    NAIVE = 1
    MULTIPLE = 2
    CLUSTERED = 3

translator = {
        'NAIVE':'clustered',
        'CLUSTERED':'clustered',
        'MULTIPLE':'multiple'}

def argsManager():
    """ Parser to manage command line arguments.

    Return:
        args: List of parsed arguments and associated values.
    """
    parser = argparse.ArgumentParser(description="Benchmarking program to evaluate split/merge efficiency.")
    parser.add_argument("hddWorkDirPath", help="", type=str)
    parser.add_argument("tmpfsWorkDirPath", help="", type=str)
    parser.add_argument("BigBrainSamplDirPath", help="", type=str)
    parser.add_argument("outputCsvFilePath", help="", type=str)
    parser.add_argument("configFilePath", help="", type=str)
    parser.add_argument("splitsDirPathTmpfs", help="", type=str)
    parser.add_argument("splitsDirPathHdd", help="", type=str)
    return parser.parse_args()

def evaluate(args):
    """Main program which aimed, for each sample of big brain, at splitting it and then re-merging it.
    We split and merge each sample of big brain on both EXT and TPMFS file systems.

    """

    print(args.configFilePath)
    with open(args.configFilePath) as jsonFile:
        config = json.load(jsonFile)

    with open(args.outputCsvFilePath, "w+") as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(["sample", "hardware_type", "file_system", "strategy", "split_time", "merge_time", "config_file"])

        #for each input file (sample of big brain)
        for fileToSplitName in os.listdir(args.BigBrainSamplDirPath):
            if not fileToSplitName.endswith("nii"):
                continue

            #we will split and merge a given input file
            filePathTmpfs = os.path.join(args.splitsDirPathTmpfs, fileToSplitName)
            filePathHdd = os.path.join(args.splitsDirPathHdd, fileToSplitName)
            splitName =  os.path.splitext(fileToSplitName)[0] + "Split"
            mergeFileName = fileToSplitName + "MergedBack.nii"
            copyfile(filePathHdd, filePathTmpfs) #temporary copy the file to split and merge on tmpfs device

            #for each algorithm, do split and merge (all on the same input file)
            for strategy in list(Strategy):

                times = applySplitAndMerge(splitDir=args.splitsDirPathTmpfs,
                                    fileToSplitPath=filePathTmpfs,
                                    strategy=strategy,
                                    config=config,
                                    mergeFileName=mergeFileName)
                writer.writerow([fileToSplitName, "", "tmpfs", strategy, times[0], times[1], args.configFilePath])

                times = applySplitAndMerge(splitDir=args.splitsDirPathHdd,
                                    fileToSplitPath=filePathHdd,
                                    strategy=strategy,
                                    config=config,
                                    mergeFileName=mergeFileName)

                writer.writerow([fileToSplitName, "hdd", "ext4", strategy, times[0], times[1], args.configFilePath])

                #Empty the directories
                for filename in os.listdir(args.splitsDirPathTmpfs): #empty output dir on tmpfs
                    os.remove(os.path.join(args.splitsDirPathTmpfs, filename))
                for filename in os.listdir(args.splitsDirPathHdd)):
                    os.remove(os.path.join(args.splitsDirPathHdd, filename))

                print("\n")

            os.remove(filePathTmpfs) #delete temporary input file to split and merge from tpmfs device

        csvFile.close()

def applySplitAndMerge(splitDir, fileToSplitPath, strategy, config, mergeFileName):
    """ Split an input file and then merge it back.

    Args:
        splitDir: Path to the directory in which to store the splits.
        fileToSplitPath: Path to the file to be splitted.
        strategy: Strategy to use for the split process.
        slabWidth: To be replaced by config data.
        mergeFileName: File name of the output merged file.
    """

    splitTime = applySplit(config,
                        filePath=fileToSplitPath,
                        outputDir=splitDir,
                        outputFileName=translator[strategy.name], #splitName
                        strategy=strategy.name)

    mergeTime = applyMerge(config,
                        outputFilePath=os.path.join(splitDir, mergeFileName),
                        legendFilePath=os.path.join(splitDir, "legend.txt"),
                        strategy= strategy.name)

    return [splitTime, mergeTime]

def applySplit(config, filePath, outputDir, outputFileName, strategy):
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
            Strategy.NAIVE: lambda img, config, outputDir, outputFileName, strategy : img.split(
                first_dim=int(config['split'][strategy.lower()]["first_dim"]),
                second_dim=int(config['split'][strategy.lower()]["second_dim"]),
                third_dim=int(config['split'][strategy.lower()]["third_dim"]),
                local_dir=outputDir,
                filename_prefix=outputFileName),

            Strategy.MULTIPLE: lambda img, config, outputDir, outputFileName, strategy:img.split_multiple_writes(
                Y_splits=int(config['split'][strategy.name.lower()]["Y_splits"]),
                Z_splits=int(config['split'][strategy.name.lower()]["Z_splits"]),
                X_splits=int(config['split'][strategy.name.lower()]["X_splits"]),
                out_dir=outputDir,
                mem=config['split']['mem'],
                filename_prefix=outputFileName,
                extension="nii"),

            Strategy.CLUSTERED:lambda img, config, outputDir, outputFileName, strategy:img.split_clustered_writes(
                Y_splits=int(config['split'][strategy.name.lower()]["Y_splits"]),
                Z_splits=int(config['split'][strategy.name.lower()]["Z_splits"]),
                X_splits=int(config['split'][strategy.name.lower()]["X_splits"]),
                out_dir=outputDir,
                mem=config['split']['mem'],
                filename_prefix=outputFileName,
                extension="nii")
    }

    t=time()
    applySplit[Strategy[strategy]](img, config, outputDir, outputFileName, strategy) #run the appropriate split algorithm
    t=time()-t
    print("Processing time to split " + filePath + " using " + str(strategy) + ": " + str(t) + " seconds.")
    return t

def applyMerge(config, outputFilePath, legendFilePath, strategy):
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
                        first_dim = config['merge']["first_dim"],
                        second_dim = config['merge']["second_dim"],
                        third_dim = config['merge']["third_dim"],
                        dtype = np.uint16)

    applyMerge = {
            Strategy.NAIVE: lambda legendFilePath, config, strategy : img.reconstruct_img(legendFilePath, "clustered", config['merge']["mem"][strategy.lower()]),
            Strategy.CLUSTERED: lambda legendFilePath, mem, config, strategy : img.reconstruct_img(legendFilePath, "clustered", config['merge']["mem"][strategy.lower()]),
            Strategy.MULTIPLE: lambda legendFilePath, mem, config, strategy : img.reconstruct_img(legendFilePath, "multiple", config['merge']["mem"][strategy.lower()])
    }

    #apply the merge
    t=time()
    applyMerge[Strategy[strategy]](legendFilePath, config, strategy)
    t=time()-t
    print("Processing time to merge " + outputFilePath + " using " + str(strategy) +  ": " + str(t) + " seconds." )
    return t

if __name__ == "__main__":
    args=argsManager()
    evaluate(args)
