# date: 20.1.2021
# author: Bernd Reinhold (bernd.reinhold@eddimed.eu)
# README.md for analyzing the OPENonOH data


## 1. preprocessing step
preprocessing.ipynb takes the json.gz files of OPENonOH_Data, gunzips them to json-files and selects noise, sgv, date, dateString and writes them into csv-files." preprocessing.ipynb 
[master (Root-Commit) 61626d8] feat(preprocessing.ipynb): preprocessing.ipynb takes the json.gz files of OPENonOH_Data, gunzips them to json-files and selects noise, sgv, date, dateString and writes them into csv-files.

Preprocessing.ipynb takes maybe 20 min to process all 161 entries_*.json.gz files. (A python script or C++ program might be more efficient, if this )

Maybe one could also share these csv-files in the google drive?

Some statistics on the output is provided in [csv_statistics_count_days.txt](csv_statistics_count_days.txt)

## aggregate into statistics per days: mean, rms (or stddev), min, max, count

