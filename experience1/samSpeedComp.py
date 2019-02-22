import nibabel as nib
import numpy as np
#import imageutils as iu
import os, sys
import math
from time import time
from enum import Enum
from shutil import copyfile
import csv
import json
import argparse
import random

import imp
iu = imp.load_source('module.name', '/home/tim/projects/sam/sam/imageutils.py')

#Program to test the speed of splitting and merging on both HDD and tmpfs

class Strategy(Enum):
    MULTIPLE=2
    CLUSTERED=1
    NAIVE = 3

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
    parser.add_argument("bigBrainSamplDirPathTmpfs", help="", type=str)
    parser.add_argument("bigBrainSamplDirPathSsd", help="", type=str)
    parser.add_argument("bigBrainSamplDirPathHdd", help="", type=str)
    parser.add_argument("outputCsvFilePath", help="", type=str)
    parser.add_argument("configFilePath", help="", type=str)
    parser.add_argument("splitsDirPathTmpfs", help="", type=str)
    parser.add_argument("splitsDirPathSsd", help="", type=str)
    parser.add_argument("splitsDirPathHdd", help="", type=str)
    parser.add_argument("nbSamplesToTreat", help="", type=int)
    parser.add_argument("nbRuns", help="", type=int)
    return parser.parse_args()

def evaluate(args):
    """Main program which aimed, for each sample of big brain, at splitting it and then re-merging it.
    We split and merge each sample of big brain on both EXT and TPMFS file systems.

    """

    with open(args.configFilePath) as jsonFile:
        config = json.load(jsonFile)

    with open(args.outputCsvFilePath, "w+") as csvFile:
        writer = csv.writer(csvFile)

        #output csv file's header
        writer.writerow(["sample", "hardware_type", "file_system", "strategy",
                            "split_time", "merge_time"])
        """writer.writerow(["sample", "hardware_type", "file_system", "strategy",
                            "split_time", "merge_time",
                            "split_nb_seeks", "merge_nb_seeks",
                            "split_read_time", "merge_read_time",
                            "split_write_time", "merge_write_time",
                            "split_seek_time", "merge_seek_time"])"""

        #create the runs to execute
        runs = list()
        for fileName in os.listdir(args.bigBrainSamplDirPathHdd)[:args.nbSamplesToTreat]: #testing on 5 samples should be enough, at least for the moment
            if not fileName.endswith("nii"):
                continue

            for strategy in list(Strategy):
                for i in range(args.nbRuns):
                    if not args.bigBrainSamplDirPathTmpfs == "none":
                        runs.append({'fileName':fileName,
                                        'strategy':strategy,
                                        'hardware':'ram',
                                        'filesystem':'tmpfs',
                                        'bigBrainSamplDir':args.bigBrainSamplDirPathTmpfs,
                                        'splitDir':args.splitsDirPathTmpfs})
                    if not args.bigBrainSamplDirPathHdd == "none":
                        runs.append({'fileName':fileName,
                                        'strategy':strategy,
                                        'hardware':'hdd',
                                        'filesystem':'ext4',
                                        'bigBrainSamplDir':args.bigBrainSamplDirPathHdd,
                                        'splitDir':args.splitsDirPathHdd})

                    if not args.bigBrainSamplDirPathSsd == "none":
                        runs.append({'fileName':fileName,
                                        'strategy':strategy,
                                        'hardware':'ssd',
                                        'filesystem':'ext4',
                                        'bigBrainSamplDir':args.bigBrainSamplDirPathSsd,
                                        'splitDir':args.splitsDirPathSsd})

        random.shuffle(runs)

        #for each input file (sample of big brain)
        for run in runs:

            importFile = False
            if run['filesystem'] == 'tmpfs':
                importFile = True

            #do split and merge
            splitStatsDict, mergeStatsDict = applySplitAndMerge(splitDir=run['splitDir'],
                                    filePathHdd = os.path.join(args.bigBrainSamplDirPathHdd, run['fileName']),
                                    fileToSplitPath=os.path.join(run['bigBrainSamplDir'], run['fileName']),
                                    strategy=run['strategy'],
                                    config=config,
                                    mergeFileName=run['fileName'] + "MergedBack.nii",
                                    importFile=importFile)

            #write stats
            writer.writerow([run['fileName'], run['hardware'], run['filesystem'], run['strategy'],
                                    splitStatsDict['split_time'], mergeStatsDict['merge_time']])

            """writer.writerow([run['fileName'], run['hardware'], run['filesystem'], run['strategy'],
                                    splitStatsDict['split_time'], mergeStatsDict['merge_time'],
                                    splitStatsDict['nb_seeks'], mergeStatsDict['nb_seeks'],
                                    splitStatsDict['read_time'], mergeStatsDict['read_time'],
                                    splitStatsDict['write_time'], mergeStatsDict['write_time'],
                                    splitStatsDict['seek_time'], mergeStatsDict['seek_time']])
            """
            #Empty the output split directories
            for f in os.listdir(run['splitDir']):
                os.remove(os.path.join(run['splitDir'], f))

        csvFile.close()

def applySplitAndMerge(splitDir, filePathHdd, fileToSplitPath, strategy, config, mergeFileName, importFile):
    """ Split an input file and then merge it back.

    Args:
        splitDir: Path to the directory in which to store the splits.
        fileToSplitPath: Path to the file to be splitted.
        strategy: Strategy to use for the split process.
        slabWidth: To be replaced by config data.
        mergeFileName: File name of the output merged file.
    """

    if importFile == True:
        copyfile(filePathHdd, fileToSplitPath) #temporary retrieve the file to split

    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    print(strategy.name)
    splitStatsDict = applySplit(config,
                        filePath=fileToSplitPath,
                        outputDir=splitDir,
                        outputFileName=translator[strategy.name], #splitName
                        strategy=strategy.name)

    if importFile == True:
        os.remove(fileToSplitPath) #delete input file to save space
        importFile = False

    os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    mergeStatsDict = applyMerge(config,
                        outputFilePath=os.path.join(splitDir, mergeFileName),
                        legendFilePath=os.path.join(splitDir, "legend.txt"),
                        strategy= strategy.name)

    return splitStatsDict, mergeStatsDict

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
    naiveFunc = lambda img, config, outputDir, outputFileName, strategy : img.split(
                first_dim=int(config['split'][strategy.lower()]["first_dim"]),
                second_dim=int(config['split'][strategy.lower()]["second_dim"]),
                third_dim=int(config['split'][strategy.lower()]["third_dim"]),
                local_dir=outputDir,
                filename_prefix=outputFileName)

    multipleFunc = lambda img, config, outputDir, outputFileName, strategy: img.split_multiple_writes(
                Y_splits=int(config['split'][strategy.lower()]["Y_splits"]),
                Z_splits=int(config['split'][strategy.lower()]["Z_splits"]),
                X_splits=int(config['split'][strategy.lower()]["X_splits"]),
                out_dir=outputDir,
                mem=int(config['split']['mem']),
                filename_prefix=outputFileName,
                extension="nii",
                benchmark=True)

    clusteredFunc = lambda img, config, outputDir, outputFileName, strategy:img.split_clustered_writes(
                Y_splits=int(config['split'][strategy.lower()]["Y_splits"]),
                Z_splits=int(config['split'][strategy.lower()]["Z_splits"]),
                X_splits=int(config['split'][strategy.lower()]["X_splits"]),
                out_dir=outputDir,
                mem=int(config['split']['mem']),
                filename_prefix=outputFileName,
                extension="nii")

    applySplit={
                Strategy.NAIVE:naiveFunc,
                Strategy.CLUSTERED:clusteredFunc,
                Strategy.MULTIPLE:multipleFunc
    }

    t=time()
    stats_dict = applySplit[Strategy[strategy]](img, config, outputDir, outputFileName, strategy) #run the appropriate split algorithm
    t=time()-t
    stats_dict = dict()
    stats_dict['split_time']=t
    print("Processing time to split " + filePath + " using " + str(strategy) + ": " + str(t) + " seconds.")
    return stats_dict

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
                        first_dim = int(config['merge']["first_dim"]),
                        second_dim = int(config['merge']["second_dim"]),
                        third_dim = int(config['merge']["third_dim"]),
                        dtype = np.uint16)

    #apply the merge
    mergeStrategy=strategy.lower()
    if mergeStrategy=="naive":
        mergeStrategy="clustered"
    t=time()
    stats_dict = img.merge(legendFilePath, mergeStrategy, int(config['merge']["mem"][strategy.lower()]))
    t=time()-t
    stats_dict = dict()
    stats_dict['merge_time']=t
    print("Processing time to merge " + outputFilePath + " using " + str(strategy) +  ": " + str(t) + " seconds." )
    return stats_dict

if __name__ == "__main__":
    args=argsManager()
    evaluate(args)
