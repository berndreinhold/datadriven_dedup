# %%
import json
import os
import pandas as pd
import glob
import fire

"""
call as: python3 duplicates_preprocessing.py --dataset=[...] [--config_filename=IO.json] [--config_path="."]

takes a directory containing files with a file_ending specified in the config file (typically: "json") and transforms them into csv files.

"""


class duplicates_preprocessing():

    def __init__(self, dataset : str, config_filename : str, config_path : str):
        """
        read a config file and populate input and output file names and paths:
            - output a csv file with key [user_id, date] and one entry per measurement
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.json_input = IO_json["duplicates_preprocessing"][dataset]["input"]
        self.json_output = IO_json["duplicates_preprocessing"][dataset]["output"]

        self.in_dir_name = self.json_input["dir_name"]
        self.columns = self.json_input["columns"]
        self.file_ending = self.json_input["file_ending"]

        self.out_dir_name = self.json_output["dir_name"]

        self.error_statistics = {}
        self.key_error_statistics = {}

    def __del__(self):
        print(self.key_error_statistics)
        print(self.error_statistics)



    def load_one_json(self,  dir_name : str, file_name : str):
        with open(os.path.join(dir_name, file_name)) as f:
            return json.load(f)


    def one_json2csv(self, entries, outfile_name):
        """
        input: this function reads a json-file
        algo:
        - flattens the structure
        - filters a subset of columns
        output: produces an output csv file with one entry being one line in the output file

        """

        data = list()
        for i, entry in enumerate(entries):
            # if i % 10000 == 0: print(i, entry)
            try:
                pd.to_datetime(entry["dateString"])  # raises an exception, if the format is unexpected, thereby avoiding it being appended to data
                data.append([entry[column] for column in self.columns])
            except KeyError as e:
                # print(f"{i}, key_error: {e}")
                if e in self.key_error_statistics.keys():
                    self.key_error_statistics[e] += 1
                else:
                    self.key_error_statistics[e] = 1            
            except Exception as e:
                if e in self.error_statistics.keys():
                    self.error_statistics[e] += 1
                else:
                    self.error_statistics[e] = 1            
                
        df = pd.DataFrame(data=data, columns=self.columns)
        df.to_csv(os.path.join(self.out_dir_name, outfile_name))
        #print(my_dict[0])


    def all_entries_json2csv(self):
        for i, f in enumerate(glob.glob(os.path.join(f"{self.in_dir_name}","**", f"entries*.{self.file_ending}"), recursive=True)):
            head, tail = os.path.split(f)
            sub_dirs = head[len(self.in_dir_name):]
            dir_name_components = sub_dirs.split("/")
            # first = 82868075, second = 21672228 in home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/82868075/21672228/entries__to_2020-09-11
            first, second = dir_name_components[0], dir_name_components[1]  
            filename, _ = os.path.splitext(tail)
            entries = self.load_one_json(head, tail)
            # print(outdir_name, os.path.join(first + "_" + second + "_" + filename + ".csv")
            if len(entries) > 0:
                self.one_json2csv(entries, first + "_" + second + "_" + filename + ".csv")
            else:
                print(f"{tail} has 0 entries, therefore no output.")
            del entries


    def extract_json_gz(self, dir_name):
        # %%
        # gunzip json.gz to json files
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/00749582/69754"
        # file_name = "entries__to_2018-06-07.json"
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/Open Humans Data/00749582/69756"
        # file_name = "profile__to_2018-06-07.json"
            
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/"
        search_string = os.path.join(f"{dir_name}","**","*.json.gz")
        print(search_string)
        for i, f in enumerate(sorted(glob.glob(search_string, recursive=True))):
            os.system(f"gunzip {f}")
            if i % 100 == 0: print(i, f)


    def kinds_of_files(self, dir_name):
        """
        - determine different kinds of files: split filename by "_" and histogram them:
        - output: {'file_info': 4912, 'devicestatus': 161, 'entries': 161, 'treatments': 161, 'profile': 162}
        """
        file_types = {}
        for i, f in enumerate(glob.glob(os.path.join(f"{dir_name}","**", f"*.{self.file_ending}"), recursive=True)):
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


    def main(self):
        # %%
        file_types = self.kinds_of_files(self.in_dir_name)
        print(file_types)
        self.all_entries_json2csv()


# %%
# determine different kinds of files: split filename by "_" and histogram them:
def kinds_of_files_OpenAPS(dir_name, file_ending):
    file_types1, file_types2 = {},{}
    for i, f in enumerate(glob.glob(os.path.join(f"{dir_name}","**", file_ending), recursive=True)):
        head, tail = os.path.split(f)
        filename, _ = os.path.splitext(tail)
        filename_components = filename.split("_")
        if len(filename_components) < 2: 
            if filename not in file_types1.keys():
                file_types1[filename] = 1
            else:
                file_types1[filename] += 1
        else: 
            file_type = filename_components[1]
            if file_type not in file_types2.keys():
                file_types2[file_type] = 1
            else:
                file_types2[file_type] += 1

    return file_types1, file_types2



# %%
def all_entries_json2csv_OpenAPS_part1(indir_name, outdir_name, column_list):
    """
    except AndroidAPS Uploader
    """
    file_list = sorted(glob.glob(os.path.join(f"{indir_name}","**", "*entries*.json"), recursive=True))
    print(len(file_list))
    for i, f in enumerate(file_list):
        head, tail = os.path.split(f)
        if i%10==0: print(i, head, tail)
        filename, _ = os.path.splitext(tail)
        if os.path.isfile(os.path.join(outdir_name, filename + ".csv")): 
            # print(f"{os.path.join(outdir_name, filename + '.csv')} exists already")
            continue
        sub_dirs = head[len(indir_name):]
        dir_name_components = sub_dirs.split("/")
        # first = 82868075, second = 21672228 in home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/82868075/21672228/entries__to_2020-09-11
        first, second = dir_name_components[0], dir_name_components[1]  
        try: 
            if not second == "direct-sharing-31": raise ValueError(f"not second==direct-sharing-31: {head}, {tail}: {first}, {second}")
    
            if first not in filename: raise Exception(f"first not in filename: {head}, {tail}: {first}, {second}")
            entries = load_one_json(head, tail)
            # print(outdir_name, os.path.join(first + "_" + second + "_" + filename + ".csv")
            if len(entries) > 0:
                one_json2csv(entries, outdir_name, filename + ".csv", column_list)
            else:
                print(f"{tail} has 0 entries, therefore no output.")
            del entries

        except ValueError as e:
            if e in error_statistics.keys():
                error_statistics[e] += 1
            else:
                error_statistics[e] = 1

# %%
def main_OpenAPS():
    dir_name = "/home/reinhold/Daten/OPEN/OpenAPS_Data/raw/"
    file_types1, file_types2 = kinds_of_files_OpenAPS(dir_name, "*.json")
    print(file_types1)
    print(file_types2)

    all_entries_json2csv_OpenAPS_part1(dir_name, "/home/reinhold/Daten/OPEN/OpenAPS_Data/csv_per_measurement/")


def main(dataset : str, config_filename : str = "IO.json", config_path : str = "."):
    dpp = duplicates_preprocessing(dataset, config_filename, config_path)
    dpp.main()

if __name__ == "__main__":
    fire.Fire(main)
