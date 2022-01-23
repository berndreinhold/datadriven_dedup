# date: 20.1.2021
# author: Bernd Reinhold (bernd.reinhold@eddimed.eu)
# README.md for analyzing the OPENonOH data


## 1. preprocessing step: preprocessing.ipynb
preprocessing.ipynb takes the json.gz files of OPENonOH_Data, gunzips them to json-files and selects noise, sgv, date, dateString and writes them into csv-files." preprocessing.ipynb 
[master (Root-Commit) 61626d8] feat(preprocessing.ipynb): preprocessing.ipynb takes the json.gz files of OPENonOH_Data, gunzips them to json-files and selects noise, sgv, date, dateString and writes them into csv-files.

Preprocessing.ipynb takes maybe 20 min to process all 161 entries_*.json.gz files. (A python script or C++ program might be more efficient, if this needs to be repeated more than once.) - Possible improvement: only a subset of all variables is stored in the csv-output files.

One entry in the csv output file is one measurement.

Maybe one could also share these csv-files in the google drive?

Some statistics on the output is provided in [csv_statistics_count_days.txt](csv_statistics_count_days.txt): Total: more than 8 Million measurements

**ToDo: need to create a directory structure as part of the preprocessing**

## 2. aggregation_step.ipynb: aggregate into statistics per days: mean, rms (or stddev), min, max, count
Output is one csv-file containing approx. 8000 lines, with the aggregate per days and statistical variables.
Aggregate per week is possible as well.

## 3. duplicates: combine with the OpenAPS data file in a smart way.
Basic idea: if the statistical variables per day are very __"similar"__ in both datasets (OpenAPS and OPENonOH), then these are duplicate candidates. A distance metric to define __"similar"__ needs to be worked out. A join between the two datasets can involve the date.


## naming conventions
following: https://pythonguides.com/python-naming-conventions/

functions and variables: snake case
classes: camelCase, 
	starting with a lowercase letter


