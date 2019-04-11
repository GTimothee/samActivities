#!/bin/bash
sam_location="/home/tim/projects/sam"
export PYTHONPATH=${PYTHONPATH}:${sam_location}

script_name="samSpeedComp.py"
runs_file_path="runs.json"
run_id="samples_1"
python $script_name $runs_file_path $run_id
