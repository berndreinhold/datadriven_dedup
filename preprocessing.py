# %%
import json
import os
import pandas as pd
import glob


def load_one_json(dir_name, file_name):
    with open(os.path.join(dir_name, file_name)) as f:
        #for line in f.readline():
        #    print(line)
        # entries = json.load(f)
        return json.load(f)
        #print(entries)


# %%
def one_json2csv(entries, dir_name, outfile_name, column_list):
    data = list()
    for i, entry in enumerate(entries):
        if i % 10000 == 0: print(entry)
        try:
            data.append([entry[column] for column in column_list])
        except KeyError as e:
            print(f"{i}, key_error: {e}")
            

    df = pd.DataFrame(data=data, columns=column_list)
    df.to_csv(os.path.join(dir_name, outfile_name))
#print(my_dict[0])

# %%
# gunzip json.gz to json files
dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/00749582/69754"
file_name = "entries__to_2018-06-07.json"
# dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/Open Humans Data/00749582/69756"
# file_name = "profile__to_2018-06-07.json"
    
dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/"
search_string = os.path.join(f"{dir_name}","**","*.json.gz")
print(search_string)
for i, f in enumerate(glob.glob(search_string, recursive=True)):
    os.system(f"gunzip {f}")
    if i % 100 == 0: print(i, f)



# %%
# determine different kinds of files: split filename by "_" and histogram them:
# output: {'file_info': 4912, 'devicestatus': 161, 'entries': 161, 'treatments': 161, 'profile': 162}
def kinds_of_files(dir_name, file_ending):
    file_types = {}
    for i, f in enumerate(glob.glob(os.path.join(f"{dir_name}","**", file_ending), recursive=True)):
        head, tail = os.path.split(f)
        filename, _ = os.path.splitext(tail)
        filename_components = filename.split("_")
        if filename_components[0] == "file":
            filename_components[0] = filename_components[0] + "_" + filename_components[1]

        if filename_components[0] not in file_types.keys():
            file_types[filename_components[0]] = 1
        else:
            file_types[filename_components[0]] += 1

    return file_types

# %%
file_types = kinds_of_files(dir_name, "*.json")
print(file_types)

# %%
def all_entries_json2csv(indir_name, outdir_name, column_list):
    for i, f in enumerate(glob.glob(os.path.join(f"{indir_name}","**", "entries*.json"), recursive=True)):
        head, tail = os.path.split(f)
        sub_dirs = head[len(indir_name):]
        dir_name_components = sub_dirs.split("/")
        # first = 82868075, second = 21672228 in home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/82868075/21672228/entries__to_2020-09-11
        first, second = dir_name_components[0], dir_name_components[1]  
        filename, _ = os.path.splitext(tail)
        entries = load_one_json(head, tail)
        # print(outdir_name, os.path.join(first + "_" + second + "_" + filename + ".csv")
        if len(entries) > 0:
            one_json2csv(entries, outdir_name, first + "_" + second + "_" + filename + ".csv", column_list)
        else:
            print(f"{tail} has 0 entries, therefore no output.")

# %%

def main():
    column_list = ["noise", "sgv", "date", "dateString"]
    all_entries_json2csv(dir_name, "/home/reinhold/Daten/OPEN/OPENonOH_Data/csv_per_measurement/", column_list)


# %%

if __name__ == "__main__":
    main()

