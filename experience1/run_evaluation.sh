rm /mnt/hdd/splitted/*;
rm /data/splitted/*;
python samSpeedComp.py none /home/tim/data/big_brain_samples /mnt/hdd/big_brain_samples output_config2_ssd_hdd.csv config_files/config2.json none /home/tim/data/splitted /mnt/hdd/splitted 1 1;
