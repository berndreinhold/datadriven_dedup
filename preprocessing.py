# %%
from distutils.command.config import config
import json
import os
import pandas as pd
import glob
import fire

"""
call as: python3 duplicates_preprocessing.py --dataset=[...] [--config_filename=IO.json] [--config_path="."]

takes a directory containing files with a file_ending specified in the config file (typically: "json") and transforms them into csv files.

"""


class duplicates_preprocessing(object):
    """
    process the BG data of OPENonOH
    """

    def __init__(self, config_filename : str, config_path : str, dataset : str = "OPENonOH"):
        """
        read a config file and populate input and output file names and paths:
            - output a csv file with key [user_id, date] and one entry per measurement
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.dataset = dataset

        self.json_input = IO_json["duplicates_preprocessing"][self.dataset]["input"]
        self.json_output = IO_json["duplicates_preprocessing"][self.dataset]["output"]


        self.in_dir_name = self.json_input["dir_name"]
        self.columns = self.json_input["columns"]
        self.file_ending = self.json_input["file_ending"]

        self.out_dir_name = self.json_output["dir_name"]

        self.error_statistics = {}
        self.key_error_statistics = {}

    def __del__(self):
        print("key_error_statistics: ", self.key_error_statistics)
        print("error_statistics: ", self.error_statistics)




    def one_json2csv(self, dir_name : str, infile_name : str, outfile_name : str):
        """
        input: this function reads a json-file
        algo:
        - flattens the structure
        - filters a subset of columns
        output: produces an output csv file with one entry being one line in the output file

        """
        data = list()
        with open(os.path.join(dir_name, infile_name)) as f:
            entries = json.load(f)
            if len(entries) < 1:
                print(f"{infile_name} has 0 entries, therefore no output.")
                return

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
            del entries
        del data


    def all_entries_json2csv(self):
        file_list = sorted(glob.glob(os.path.join(f"{self.in_dir_name}","**", f"entries*.{self.file_ending}"), recursive=True))
        print("number of files: ", len(file_list))
        print(file_list[:3])  # head 
        print(file_list[-3:])  # and tail

        for i, f in enumerate(file_list):
            if i % 10 == 0: print(i, f)
            head, tail = os.path.split(f)
            sub_dirs = head[len(self.in_dir_name):]
            dir_name_components = sub_dirs.split("/")
            # first = 82868075, second = 21672228 in home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/82868075/21672228/entries__to_2020-09-11
            first, second = dir_name_components[0], dir_name_components[1]  
            filename, _ = os.path.splitext(tail)
            # print(outdir_name, os.path.join(first + "_" + second + "_" + filename + ".csv")
            self.one_json2csv(head, tail, first + "_" + second + "_" + filename + ".csv")


    def extract_json_gz(self, dir_name):
        # for the OpenAPS dataset
        # gunzip json.gz to json files
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/00749582/69754"
        # file_name = "entries__to_2018-06-07.json"
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/Open Humans Data/00749582/69756"
        # file_name = "profile__to_2018-06-07.json"
            
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/"
        search_string = os.path.join(f"{dir_name}","**",f"*.{self.file_ending}.gz")
        print(search_string)
        json_gz = set([x[:-3] for x in sorted(glob.glob(search_string, recursive=True))])
        json_ = set(sorted(glob.glob(search_string[:-3], recursive=True))) 

        if len(json_gz ^ json_) > 0:
            print(f"json files without corresponding json.gz file: ", json_ - json_gz)
            print(f"json.gz files without corresponding json file: ", json_gz - json_)

        for i, f in enumerate(json_gz - json_):
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


    def loop(self):
        """
        loop through all available files and do the same thing on each file
        """
        file_types = self.kinds_of_files(self.in_dir_name)
        print(file_types)
        self.all_entries_json2csv()


class duplicates_preprocessing_OpenAPS_NS(duplicates_preprocessing):
    """
    OpenAPS Nightscout files: direct-sharing-31
    """
    def __init__(self, config_filename : str, config_path : str):
        self.dataset = "OpenAPS"
        super().__init__(config_filename, config_path, self.dataset)
        

    def kinds_of_files(self, dir_name):
        """
        determine different kinds of files: split filename by "_" and histogram them:
        """
        file_types1, file_types2 = {},{}
        for i, f in enumerate(glob.glob(os.path.join(f"{self.in_dir_name}","**", f"*.{self.file_ending}"), recursive=True)):
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



    def all_entries_json2csv(self):
        """
        focus on NightScout uploads: direct-sharing-31 
        excluded AndroidAPS Uploader ()
        """
        file_list = sorted(glob.glob(os.path.join(f"{self.in_dir_name}","**", "*entries*.json"), recursive=True))
        print("number of files: ", len(file_list))
        print(file_list[:3])  # head 
        print(file_list[-3:])  # and tail
        for i, f in enumerate(file_list):
            head, tail = os.path.split(f)
            if i%10==0: print(i, head, tail)
            filename, _ = os.path.splitext(tail)
            if os.path.isfile(os.path.join(self.out_dir_name, filename + ".csv")): 
                # print(f"{os.path.join(outdir_name, filename + '.csv')} exists already")
                continue
            sub_dirs = head[len(self.in_dir_name):]
            dir_name_components = sub_dirs.split("/")
            # first = 82868075, second = 21672228 in home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/82868075/21672228/entries__to_2020-09-11
            first, second = dir_name_components[0], dir_name_components[1]  
            try: 
                if not second == "direct-sharing-31": raise ValueError(f"not second==direct-sharing-31: {head}, {tail}: {first}, {second}")
        
                if first not in filename: raise Exception(f"first not in filename: {head}, {tail}: {first}, {second}")
                self.one_json2csv(head, tail, first + "_" + second + "_" + filename + ".csv")
                
            except ValueError as e:
                if e in self.error_statistics.keys():
                    self.error_statistics[e] += 1
                else:
                    self.error_statistics[e] = 1


def main(dataset : str, config_filename : str = "IO.json", config_path : str = "."):
    dpp = duplicates_preprocessing(config_filename, config_path)
    if dataset == "OpenAPS_NS":
        dpp = duplicates_preprocessing_OpenAPS_NS(config_filename, config_path)
    dpp.loop()

if __name__ == "__main__":
    fire.Fire(main)
