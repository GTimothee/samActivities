rm /dev/shm/tguedon/splittedSamples/*;
rm /dev/shm/tguedon/bigBrainSamples/*;
rm /data/tguedon/splittedSamples/*;
sudo python samSpeedComp.py /dev/shm/tguedon/bigBrainGenSamples /data/tguedon/bigBrainGenSamples output_config2.csv config2.json /dev/shm/tguedon/splittedSamples /data/tguedon/splittedSamples;

