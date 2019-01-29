rm /dev/shm/tguedon/splittedSamples/*;
rm /dev/shm/tguedon/bigBrainGenSamples/*;
rm /data/tguedon/splittedSamples/*;
python samSpeedComp.py /dev/shm/tguedon/bigBrainGenSamples /data/tguedon/bigBrainGenSamples output_config2.csv config2.json /dev/shm/tguedon/splittedSamples /data/tguedon/splittedSamples;

