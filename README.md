```
start date: 20.1.2022 (European date format)
last edit: 14.4.2022
author: Bernd Reinhold
```
# README.md for analyzing the OPENonOH and OpenAPS data sets

## links
- [README2.md](README2.md) (general pipeline)
- [OPEN_visualisations.md](OPEN_visualisations.md)

## process flow

<img src="doc/ProcessFlow.png" alt="ProcessFlow" width="1000px"/>

(generated from [process_flow.odp](doc/process_flow.odp))

_needs update_

## background
- it should scale to an arbitrary number of datasets
- you provide constraints through duplicate criteria (see pairwise)
- subselections

## 0. preparation

### folder structure
![FolderStructure](doc/FolderStructure.png)
- **csv_per_measurements** contains files, where one entry corresponds to one measurement of the sensor/device, etc.
- **csv_per_day** contains files, where one entry corresponds to the aggregate per day

The folder structure is created in the scripts, if they do not yet exist.

### dependencies
```
pip3 install -r requirements.txt
```
(requirements.txt was generated with `pipreqs .` + some manual adjustment)

## 1. preprocessing step: `python3 preprocessing.py` (per dataset)
preprocessing.py takes the json.gz files of OPENonOH_Data, gunzips them to json-files and selects **noise, sgv, date, dateString** and writes them into csv-files in the _csv_per_measurement_-subdirectory.
 
_Possible improvement: only a subset of all variables is stored in the csv-output files, store all variables._

One entry in the csv output file is one measurement. One file corresponds to one json file. 

_Maybe one could also share these csv-files in the google drive?_

Some statistics on the output is provided in [csv_statistics_count_days.txt](csv_statistics_count_days.txt): Total: more than 8 Million measurements

This brings the different formats into one common format: csv file with noise, bg, date, dateString.

## 2. aggregation_step.ipynb: aggregate into statistics per days: mean, rms (or stddev), min, max, count (per dataset)
Read the csv-files in csv_per_measurement as input.
Output is one csv-file containing approx. 8000 lines, with the aggregate per days and statistical variables.
(The aggregation per day is a somewhat arbitrary choice. Since one has up to 285 measurements per day, the error on the statistical measures becomes small. For higher granularity the statistical error becomes bigger, increasing the likelihood of false positive duplicates. Aggregate per week would be possible as well.)

_Paths need to be adjusted to your local environment._

## 3. self-duplicates (per dataset)


## 4. pairwise duplicates: combine with the OpenAPS data file in a smart way.
<img src="doc/ProcessFlow.png" alt="ProcessFlow" width="1000px"/>

Basic idea: if the statistical variables per day are very __"similar"__ in both datasets (OpenAPS and OPENonOH), then these are duplicate candidates. A distance metric to define __"similar"__ needs to be worked out.

The algorithm to detect duplicates applied here is a join by date between the Open Humans and the OpenAPS data commons dataset and then calculating the quadratic difference between the daily mean(plasma glucose), stddev, min, max and requiring a certain threshold.
A requirement here is that datetimes are consistent in both datasets, otherwise the daily mean, stddev, min, max are different even if the underlying data is identical except for the offset in time. An offset in time could lead to false negatives. This is achieved by calculating a UTC time from the unix_timestamps ("date"-column) and taking the date from that UTC timestamp.

False positives could arise if the threshold is chosen too loose.  

_Paths need to be adjusted to your local environment._

Here use the generic dataset_1,_2,_3 rather than the specific OpenAPS_NS (nightscout), OPENonOH, in the processing.
Because for duplicates it just matters, that they are distinct datasets, while in the previous steps the different format specific to each dataset was relevant.

## naming conventions
following: https://pythonguides.com/python-naming-conventions/

- functions and variables: snake case
- classes: camelCase, starting with a lowercase letter

## Datasets

### OpenAPS Data Commons
- Compressed: 9.6 GB
- Uncompressed: ~120 GB (!) 

## tested with
- python 3.8.10
- see requirements.txt