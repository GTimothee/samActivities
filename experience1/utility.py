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
        print(nbSlicePerSample)

        #continue
        startIndex=endIndex
        endIndex+=nbSlicePerSample
        sampleIndex+=1

    print("Number slices per sample: " + str(nbSlicePerSample))
    return nbSlicePerSample

def buildDataSamples():
    """Split the big brain into slabs

    """
    print("Splitting Big Brain...")
    bigBrainPath = os.path.join("/data", "bigbrain_40microns.nii.gz")
    outputDir = "bigBrainGenSamples" #generated samples
    slabWidth = extractBigBrainSamples(bigBrainPath=bigBrainPath,
                                        outputDir=outputDir,
                                        nbSamples=5,
                                        sampleMaxSize=3.0)
    print("Done. Slab width: " + str(slabWidth))
