import nibabel as nib
import numpy as np
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
from sam import imageutils as iu
import subprocess

#Program to test the speed of splitting and merging on SSD ext4, HDD ext4 and/or shared memory tmpfs

class Strategy(Enum):
    MULTIPLE = 2
    CLUSTERED = 1
    NAIVE = 3

translator = {'NAIVE':'clustered',
              'CLUSTERED':'clustered',
              'MULTIPLE':'multiple',
              'naive':list(Strategy)[2],
              'clustered':list(Strategy)[1],
              'multiple':list(Strategy)[0]}

def extract_args(args):
    """ From a file path and the id of a run we extract the arguments of the program.
    """
    with open(args.runs_file_path) as json_file:
	       args = json.load(json_file)[args.run_id]['config']
    return dict(args)

def args_manager():
    """ Parser to manage command line arguments.
    Return:
        args: List of parsed arguments and associated values.
    """
    parser = argparse.ArgumentParser(description="Benchmarking program to evaluate split/merge efficiency.")
    parser.add_argument("runs_file_path", help="File path to the json file containing information about all runs.", type=str)
    parser.add_argument("run_id", help="Name of the run you want to run.", type=str)
    return extract_args(parser.parse_args())

def benchmarking(args):
    """Main program which aimed, for each sample of big brain, at splitting it and then re-merging it.
    We split and merge each sample of big brain on both EXT and TPMFS file systems.

    """

    with open(args['configFilePath']) as jsonFile:
        config = json.load(jsonFile)

    with open(args['outputCsvFilePath'], "w+") as csvFile:
        writer = csv.writer(csvFile)

        #output csv file's header
        writer.writerow(["sample", "hardware_type", "file_system", "strategy",
                            "split_time", "merge_time",
                            "split_nb_seeks", "merge_nb_seeks",
                            "split_read_time", "merge_read_time",
                            "split_write_time", "merge_write_time",
                            "split_seek_time", "merge_seek_time"])

        #create the runs to execute
        runs = list()
        possible_dirs = [path for path in [args['bigBrainSamplDirPathSsd'], args['bigBrainSamplDirPathHdd'], args['bigBrainSamplDirPathTmpfs']] if not path == "none"]
        print("possible dirs:", possible_dirs)
        for fileName in os.listdir(possible_dirs[0])[:int(args['nbSamplesToTreat'])]: # testing on 5 samples should be enough, at least for the moment
            if not fileName.endswith("nii"):
                continue

            for strategy in list(map(lambda x: translator[x], args['strategies'])):
                for i in range(int(args['nbRuns'])):
                    if not args['bigBrainSamplDirPathTmpfs'] == "none":
                        runs.append({'fileName':fileName,
                                        'strategy':strategy,
                                        'hardware':'ram',
                                        'filesystem':'tmpfs',
                                        'bigBrainSamplDir':args['bigBrainSamplDirPathTmpfs'],
                                        'splitDir':args['splitsDirPathTmpfs']})
                    if not args['bigBrainSamplDirPathHdd'] == "none":
                        runs.append({'fileName':fileName,
                                        'strategy':strategy,
                                        'hardware':'hdd',
                                        'filesystem':'ext4',
                                        'bigBrainSamplDir':args['bigBrainSamplDirPathHdd'],
                                        'splitDir':args['splitsDirPathHdd']})

                    if not args['bigBrainSamplDirPathSsd'] == "none":
                        runs.append({'fileName':fileName,
                                        'strategy':strategy,
                                        'hardware':'ssd',
                                        'filesystem':'ext4',
                                        'bigBrainSamplDir':args['bigBrainSamplDirPathSsd'],
                                        'splitDir':args['splitsDirPathSsd']})

        random.shuffle(runs)
        print("start runs")
        #for each input file (sample of big brain)
        for run in runs:
            print("run loop")

            importFile = False
            if run['filesystem'] == 'tmpfs':
                importFile = True

            #do split and merge
            splitStatsDict, mergeStatsDict = apply_split_and_merge(splitDir=run['splitDir'],
                                    filePathHdd = os.path.join(args['bigBrainSamplDirPathHdd'], run['fileName']),
                                    fileToSplitPath=os.path.join(run['bigBrainSamplDir'], run['fileName']),
                                    strategy=run['strategy'],
                                    config=config,
                                    mergeFileName=run['fileName'] + "MergedBack.nii",
                                    importFile=importFile,
                                    flushCaches=True)

            print('Checking output integrity...')
            original_sample = os.path.join(run['bigBrainSamplDir'], run['fileName'])
            reconstructed_sample = os.path.join(run['splitDir'], run['fileName'] + "MergedBack.nii")
            print('original file size: ', os.path.getsize(original_sample))
            print('resulting file size: ', os.path.getsize(reconstructed_sample))

            #write stats
            print(list(mergeStatsDict.keys()))
            writer.writerow([run['fileName'], run['hardware'], run['filesystem'], run['strategy'],
                                    splitStatsDict['split_time'], mergeStatsDict['merge_time'],
                                    splitStatsDict['split_nb_seeks'], mergeStatsDict['merge_nb_seeks'],
                                    splitStatsDict['split_read_time'], mergeStatsDict['merge_read_time'],
                                    splitStatsDict['split_write_time'], mergeStatsDict['merge_write_time'],
                                    splitStatsDict['split_seek_time'], mergeStatsDict['merge_seek_time']])

            #Empty the output split directories
            for f in os.listdir(run['splitDir']):
                os.remove(os.path.join(run['splitDir'], f))

        csvFile.close()


def get_mem_usage():
    '''
    Returns swap, buffer usage and cache usage, in this order.
    '''
    a = subprocess.check_output(['vmstat', '-a', '1', '1']).decode("utf-8")
    print(a)
    a = subprocess.check_output(['vmstat', '1', '1']).decode("utf-8")
    print(a)
    b = a.split('\n')[2].split()
    swap = b[2]
    buff = b[4]
    cache = b[5]
    active = b[7]
    """print("swap")
    print("buff", buff)
    print("cache", cache)
    print("active", active)
    return swap, buff, cache, active
    """
    return

def apply_split_and_merge(splitDir, filePathHdd, fileToSplitPath, strategy, config, mergeFileName, importFile, flushCaches):
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

    if flushCaches:
        os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')

    print("first cache drop")
    get_mem_usage()

    print("splitting...")
    print(strategy.name)
    splitStatsDict = apply_split(config,
                        filePath=fileToSplitPath,
                        outputDir=splitDir,
                        outputFileName=translator[strategy.name], #splitName
                        strategy=strategy.name)


    print("cache after splitting")
    get_mem_usage()

    if importFile == True:
        os.remove(fileToSplitPath) #delete input file to save space
        importFile = False

    if flushCaches:
        os.system('sync; echo 3 | sudo tee /proc/sys/vm/drop_caches')


    print("second cache drop")
    get_mem_usage()

    print("merging...")
    mergeStatsDict = apply_merge(config,
                        outputFilePath=os.path.join(splitDir, mergeFileName),
                        legendFilePath=os.path.join(splitDir, "legend.txt"),
                        strategy= strategy.name)

    print(splitStatsDict, '\n', mergeStatsDict)
    print("after merging")
    get_mem_usage()

    print(splitStatsDict)
    print(mergeStatsDict)

    return splitStatsDict, mergeStatsDict

def apply_split(config, filePath, outputDir, outputFileName, strategy):
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
    naiveFunc = lambda img, config, outputDir, outputFileName, strategy, benchmark : img.split(
                first_dim=int(config['split'][strategy.lower()]["first_dim"]),
                second_dim=int(config['split'][strategy.lower()]["second_dim"]),
                third_dim=int(config['split'][strategy.lower()]["third_dim"]),
                local_dir=outputDir,
                filename_prefix=outputFileName,
                benchmark=benchmark)

    multipleFunc = lambda img, config, outputDir, outputFileName, strategy, benchmark: img.split_multiple_writes(
                Y_splits=int(config['split'][strategy.lower()]["Y_splits"]),
                Z_splits=int(config['split'][strategy.lower()]["Z_splits"]),
                X_splits=int(config['split'][strategy.lower()]["X_splits"]),
                out_dir=outputDir,
                mem=int(config['split']['mem']),
                filename_prefix=outputFileName,
                extension="nii",
                benchmark=benchmark)

    clusteredFunc = lambda img, config, outputDir, outputFileName, strategy, benchmark:img.split_clustered_writes(
                Y_splits=int(config['split'][strategy.lower()]["Y_splits"]),
                Z_splits=int(config['split'][strategy.lower()]["Z_splits"]),
                X_splits=int(config['split'][strategy.lower()]["X_splits"]),
                out_dir=outputDir,
                mem=int(config['split']['mem']),
                filename_prefix=outputFileName,
                extension="nii",
                benchmark=benchmark)

    apply_split={
                Strategy.NAIVE:naiveFunc,
                Strategy.CLUSTERED:clusteredFunc,
                Strategy.MULTIPLE:multipleFunc
    }

    t=time()
    stats_dict = apply_split[Strategy[strategy]](img, config, outputDir, outputFileName, strategy, True) #run the appropriate split algorithm
    t=time()-t
    stats_dict['split_time']=t # add total time
    print("Processing time to split " + filePath + " using " + str(strategy) + ": " + str(t) + " seconds.")
    return stats_dict

def apply_merge(config, outputFilePath, legendFilePath, strategy):
    """ Merge splits that have previously been created from a nii image in order to rebuild the input nii image.
    The splits have been saved into an output folder with a 'legend' text file containing the indexes of the splits to be merged.

    Args:
        outputFilePath: Path and name of the output file to be created by merging the splits of the sample being merged.
        legendFilePath: Path and name of the legend file that contains thenames of the splits of the sample being merged.
        strategy: Algorithm to use for the merging (see Strategy enum at the top of the file).
        nbSlices: Number of slices in the sample being merged. It depends on the extractBigBrainSamples function (in this file).
    """

    #open a new image object
    img = iu.ImageUtils(outputFilePath,
                        first_dim = int(config['merge']["first_dim"]),
                        second_dim = int(config['merge']["second_dim"]),
                        third_dim = int(config['merge']["third_dim"]),
                        dtype = np.uint16)

    #apply the merge
    mergeStrategy = strategy.lower()
    if mergeStrategy == "naive":
        mergeStrategy = "clustered"
    t = time()
    stats_dict = img.merge(legendFilePath, mergeStrategy, int(config['merge']["mem"][strategy.lower()]), benchmark=True) # benchmark=True to get stats dict from function
    t = time() - t
    stats_dict['merge_time']=t
    print("Processing time to merge " + outputFilePath + " using " + str(strategy) +  ": " + str(t) + " seconds." )
    return stats_dict

if __name__ == "__main__":
    print("get arguments")
    args = args_manager()
    print("enter benchmarking process")
    benchmarking(args)
