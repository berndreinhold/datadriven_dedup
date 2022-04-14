```
start date: 14.4.2022 (European date format)
last edit: 14.4.2022
author: Bernd Reinhold
```
# README too - README2.md describes the global pipeline

## links
- [README.md](README.md) (pairwise duplicates)
- [OPEN_visualisations.md](OPEN_visualisations.md)

## process flow

<img src="doc/ProcessFlow.png" alt="ProcessFlow" width="1000px"/>

Assuming you have four datasets that you want to process and find duplicates in.
Here dataset is used in two ways:
- dataset or
- dataset/uploader

(explain a bit more)
## 1. self-duplicates removal (per dataset)

## 2. find pairwise duplicate entries in datasets
see [README.md](README.md)

input: json-files from the various dataset/uploader 

output: csv file duplicates_dataset1_dataset2.csv
Here dataset1 is e.g. OpenAPS_NS a dataset
## 3. table with one entry per day/user-id-pair and table with entry per user-id (all datasets)
These two tables are calculated together.
They represent the relationship between different user-ids on a per-user-id- and per-user-id/day-level across different datasets. They do not contain data themselves.


## 3a. additional filters
Additional filters can be introduced via lists of user_ids. These lists are used to filter the content of the two tables above via "inner joins".
## 4. [visualisations](OPEN_visualisations.md)
The two tables from above can then be used to visualize the relationship between the datasets.
Venn-diagrams or [Upset plots](https://pypi.org/project/UpSetPlot/) are used.
Then there are plots showing at a "person_counter vs. date"-level the pairwise duplicates.



## Other
### user_id, person_id, person_counter
Explain the usage of different but closely related variables to identify persons and users in data.

### FAQ
- if you need to implement new ways of determining duplicates, then do it as part of [section 2](#2-find-pairwise-duplicate-entries-in-datasets)
- if you want to add new datasets, you have to adjust the whole chain.