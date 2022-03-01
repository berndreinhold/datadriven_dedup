```
start date: 20.1.2022
last edit: 28.2.2022
author: Bernd Reinhold
```
# README.md for analyzing the OPENonOH and OpenAPS data sets

## process flow

<img src="ProcessFlow.png" alt="ProcessFlow" width="1000px"/>


## 0. preparation

### folder structure
![FolderStructure](FolderStructure.png)
- **csv_per_measurements** contains files, where one entry corresponds to one measurement of the sensor/device, etc.
- **csv_per_day** contains files, where one entry corresponds to the aggregate per day

Please create this data structure manually.

**ToDo: need to create a directory structure as part of the preprocessing**

### dependencies
```
pip3 install -r requirements.txt
```
(requirements.txt was generated with `pipreqs .` + some manual adjustment)

## 1. preprocessing step: `python3 preprocessing.py`
preprocessing.py takes the json.gz files of OPENonOH_Data, gunzips them to json-files and selects **noise, sgv, date, dateString** and writes them into csv-files in the _csv_per_measurement_-subdirectory.

Preprocessing.ipynb took rather long (maybe 20 min) to process all 161 entries_*.json.gz files. Therefore the switch to a python script 
_Possible improvement: only a subset of all variables is stored in the csv-output files, store all variables._

One entry in the csv output file is one measurement. One file corresponds to one json file. 

_Maybe one could also share these csv-files in the google drive?_

Some statistics on the output is provided in [csv_statistics_count_days.txt](csv_statistics_count_days.txt): Total: more than 8 Million measurements

## 2. aggregation_step.ipynb: aggregate into statistics per days: mean, rms (or stddev), min, max, count
Read the csv-files in csv_per_measurement as input.
Output is one csv-file containing approx. 8000 lines, with the aggregate per days and statistical variables.
(Aggregate per week is possible as well.)

_Paths need to be adjusted to your local environment._

## 3. duplicates: combine with the OpenAPS data file in a smart way.
Basic idea: if the statistical variables per day are very __"similar"__ in both datasets (OpenAPS and OPENonOH), then these are duplicate candidates. A distance metric to define __"similar"__ needs to be worked out.

The algorithm to detect duplicates applied here is a join by date between the Open Humans and the OpenAPS data commons dataset and then calculating the quadratic difference between the daily mean(plasma glucose), stddev, min, max and requiring a certain threshold.
A requirement here is that datetimes are consistent in both datasets, otherwise the daily mean, stddev, min, max are different even if the underlying data is identical except for the offset in time. An offset in time could lead to false negatives. This is achieved by calculating a UTC time from the unix_timestamps ("date"-column) and taking the date from that UTC timestamp.

False positives could arise if the threshold is chosen too loose.  

_Paths need to be adjusted to your local environment._

## naming conventions
following: https://pythonguides.com/python-naming-conventions/

functions and variables: snake case
classes: camelCase, 
	starting with a lowercase letter

## Datasets

### OpenAPS Data Commons
Compressed: 9.6 GB
Uncompressed: ~120 GB (!) 

## tested with
python 3.8.10
see requirements.txt