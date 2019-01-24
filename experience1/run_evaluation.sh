WORK_DIR=$PWD
python "${WORK_DIR}"/samSpeedComp.py \
    hddWorkDirPath="${WORK_DIR}" \
    tmpfsWorkDirPath="/dev/shm" \
    samplesDirPath="../../bigBrainGenSamples" \
    outputDirPath="../../splittedSamples" \
    outputCsvFile="results.csv" \
    configFilePath="${WORK_DIR}/config.json"/
