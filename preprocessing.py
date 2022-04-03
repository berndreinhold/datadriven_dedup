from distutils.command.config import config
import json
import os
from collections import namedtuple
import pandas as pd
import glob
import fire
import logging
import datetime
import numpy as np


"""
call as: python3 duplicates_preprocessing.py --dataset=[...] [--config_filename=IO.json] [--config_path="."]

takes a directory containing files with a file_ending specified in the config file (typically: "json") and transforms them into csv files.

"""
Params = namedtuple("Params", ['head', 'tail', 'filename', 'first', 'second'])

class duplicates_preprocessing(object):
    """
    process the BG data of OPENonOH
    it is on the one hand a base class for the other combinations of (OpenAPS, OPENonOH) and (NightScout (NS), AAPS_Uploader) processing classes and 
    it is the OPENonOH_NS preprocessing class
    """

    def __init__(self, config_filename : str, config_path : str, dataset : str = "OPENonOH", console_log_level = logging.INFO):
        """
        read a config file and populate input and output file names and paths:
            - output a csv file with key [user_id, date] and one entry per measurement
        @param dataset: is used as a key in the json-config-file.
        """

        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.dataset = dataset
        
        datasets = [x for x in IO_json["duplicates_preprocessing"].keys() if "comment" and "logging" not in x]
        assert(self.dataset in datasets)


        self.json_input = IO_json["duplicates_preprocessing"][self.dataset]["input"]
        self.json_output = IO_json["duplicates_preprocessing"][self.dataset]["output"]
        self.json_logging = IO_json["duplicates_preprocessing"]["logging"]

        self.in_dir_name = self.json_input["dir_name"]
        try:
            self.in_columns = IO_json["duplicates_preprocessing"][self.dataset]["columns"]
            self.out_columns = IO_json["duplicates_preprocessing"][self.dataset]["columns"]
        except KeyError as e:
            self.in_columns = self.json_input["columns"]
            self.out_columns = self.json_output["columns"]
                    
        try:
            self.in_columns = self.json_input["columns"]
            self.out_columns = self.json_output["columns"]
        except KeyError as e:
            if self.in_columns is None or self.out_columns is None:
                raise KeyError("Check the config json file. Column names must be present at the one of two levels in the config json file.")

        self.file_ending = self.json_input["file_ending"]

        self.out_dir_name = self.json_output["dir_name"]
        os.makedirs(self.out_dir_name, exist_ok=True)

        # logging
        # adapted from https://github.com/acschaefer/duallog/blob/master/duallog/duallog.py
        os.makedirs(self.json_logging["dir_name"], exist_ok=True)
        # basic config with file handler, that records everything
        logging.basicConfig(filename=os.path.join(self.json_logging["dir_name"], f"{__class__.__name__}.log"), level=logging.DEBUG)
        logger = logging.getLogger("")
        # Set up logging to the console.
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(console_log_level)
        logger.addHandler(stream_handler)

        self.error_statistics = {}
        self.key_error_statistics = {}

        # store the json-config file and the dataset in the logging file
        logging.debug(IO_json)
        logging.debug(self.dataset)

    def __del__(self):
        logging.debug(f"key_error_statistics: {self.key_error_statistics}")
        logging.debug(f"error_statistics: {self.error_statistics}")
        logging.info(self.__class__.__name__)



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
                logging.info(f"{infile_name} has 0 entries, therefore no output.")
                return

            for i, entry in enumerate(entries):
                try:
                    pd.to_datetime(entry["dateString"])  # raises an exception, if the format is unexpected, thereby avoiding it being appended to data
                    data.append([entry[column] for column in self.in_columns])
                except KeyError as e:
                    if e in self.key_error_statistics.keys():
                        self.key_error_statistics[e] += 1
                    else:
                        self.key_error_statistics[e] = 1            
                except Exception as e:
                    if e in self.error_statistics.keys():
                        self.error_statistics[e] += 1
                    else:
                        self.error_statistics[e] = 1            
                    
            df = pd.DataFrame(data=data, columns=self.out_columns)
            df.to_csv(os.path.join(self.out_dir_name, outfile_name))
            del entries
        del data


    def all_entries_json2csv(self):
        file_list = sorted(glob.glob(os.path.join(f"{self.in_dir_name}","**", f"entries*.{self.file_ending}"), recursive=True))
        logging.info(f"number of files: {len(file_list)}")
        logging.info(file_list[:3])  # head 
        logging.info(file_list[-3:])  # and tail

        for i, f in enumerate(file_list):
            if i % 10 == 0: logging.info(f"{i}, {f}")
            head, tail = os.path.split(f)
            sub_dirs = head[len(self.in_dir_name):]
            dir_name_components = sub_dirs.split("/")
            # first = 82868075, second = 21672228 in home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/82868075/21672228/entries__to_2020-09-11
            first, second = dir_name_components[0], dir_name_components[1]  
            filename, _ = os.path.splitext(tail)
            if os.path.isfile(os.path.join(self.out_dir_name, first + "_" + second + "_" + filename + ".csv")): 
                continue
            self.one_json2csv(head, tail, first + "_" + second + "_" + filename + ".csv")
        

    def extract_json_gz(self, dir_name):
        # for the OpenAPS_NS (nightscout) dataset
        # gunzip json.gz to json files
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/00749582/69754"
        # file_name = "entries__to_2018-06-07.json"
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/Open Humans Data/00749582/69756"
        # file_name = "profile__to_2018-06-07.json"
            
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/"
        search_string = os.path.join(f"{dir_name}","**",f"*.{self.file_ending}.gz")
        logging.info(search_string)
        json_gz = set([x[:-3] for x in sorted(glob.glob(search_string, recursive=True))])
        json_ = set(sorted(glob.glob(search_string[:-3], recursive=True))) 

        if len(json_gz ^ json_) > 0:
            logging.debug(f"json files without corresponding json.gz file: ", json_ - json_gz)
            logging.debug(f"json.gz files without corresponding json file: ", json_gz - json_)

        for i, f in enumerate(json_gz - json_):
            os.system(f"gunzip {f}")
            if i % 100 == 0: logging.info(f"{i}, {f}")

    def extract_AAPS_Uploader_zip(self, dir_name, unzip_option : str = "-n -q"):
        """
        for the AAPS Uploader zip archives (both OpenAPS and OPENonOH)
        $ unzip upload-num4-ver1-date20201019T153559-appid39dd3841ed5d4f2a820102fbd0a8880a.zip
        Archive:  upload-num4-ver1-date20201019T153559-appid39dd3841ed5d4f2a820102fbd0a8880a.zip
            inflating: BgReadings.json         
            inflating: APSData.json            
            inflating: TemporaryBasals.json    
            inflating: Treatments.json         
            inflating: TempTargets.json        
            inflating: CareportalEvents.json   
            inflating: ProfileSwitches.json    
            inflating: ApplicationInfo.json    
            inflating: Preferences.json        
            inflating: DeviceInfo.json         
            inflating: DisplayInfo.json        
            inflating: UploadInfo.json

        for the options see "man unzip": -n never overwrite existing files.  If a file already exists, skip the extraction of that file without prompting
        """
        search_string = os.path.join(dir_name,"**",f"*.zip")
        logging.info(search_string)
        # zipped and zipped_files are necessary, since the BgReadings.json and the upload...zip are in the same directory, 
        # so in order to decide, which zip files to unzip, one needs to compare the directories, but in order to actually unzip, one needs the files
        zipped = set([os.path.split(x)[0] for x in sorted(glob.glob(search_string, recursive=True))])  # just record the directory
        zipped_files = set(sorted(glob.glob(search_string, recursive=True)))  # just record the directory
        search_string2 = os.path.join(dir_name,"**",f"*BgReadings.json")
        extracted = set([os.path.split(x)[0] for x in sorted(glob.glob(search_string2, recursive=True))]) 

        

        if len(zipped ^ extracted) > 0:
            logging.debug(f"zipped files without corresponding BgReadings.json file: {zipped - extracted}")
            logging.debug(f"BgReadings.json files without zip file in the same directory: {extracted - zipped}")

        for i, f in enumerate(zipped_files):
            head, tail = os.path.split(f)
            if head not in zipped - extracted: continue
            os.system(f"unzip {unzip_option} {f} -d {head}")
            if i % 100 == 0: logging.info(f"{i}, {f}")

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
        if "_NS" in self.dataset: self.extract_json_gz(self.in_dir_name)
        elif "_AAPS_Uploader" in self.dataset: self.extract_AAPS_Uploader_zip(self.in_dir_name)
        file_types = self.kinds_of_files(self.in_dir_name)
        logging.info(file_types)
        self.all_entries_json2csv()


class duplicates_preprocessing_OpenAPS_NS(duplicates_preprocessing):
    """
    OpenAPS Nightscout files: direct-sharing-31
    """

    def __init__(self, config_filename : str, config_path : str, dataset : str = "OpenAPS_NS", console_log_level = logging.INFO):
        super().__init__(config_filename, config_path, dataset, console_log_level)
        

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
        file_list = sorted(glob.glob(os.path.join(f"{self.in_dir_name}","**", "direct-sharing-31","**", "*entries*.json"), recursive=True))
        logging.info(f"number of files: {len(file_list)}")
        logging.info(file_list[:3])  # head 
        logging.info(file_list[-3:])  # and tail
        for i, f in enumerate(file_list):
            # if i<80: continue
            head, tail = os.path.split(f)
            #if i%10==0: logging.info(f"{i}, {head}, {tail}")
            filename, _ = os.path.splitext(tail)
            if os.path.isfile(os.path.join(self.out_dir_name, filename + ".csv")): 
                continue
            sub_dirs = head[len(self.in_dir_name):]
            sub_dirs = sub_dirs.strip('/')
            dir_name_components = sub_dirs.split("/")
            # first = 96254963, second = direct-sharing-31 in /home/reinhold/Daten/OPEN/OpenAPS_Data/raw/96254963/direct-sharing-31
            first, second = dir_name_components[0], dir_name_components[1]  

            #if i%10==0: logging.info(", ".join([f"{x}: {params[i]}" for i,x in enumerate(params._asdict().keys())]))
            try: 
                if first not in filename: raise Exception(f"first not in filename: {head}, {tail}: {first}, {second}")
                self.one_json2csv(head, tail, filename + ".csv")
                
            except (ValueError, Exception) as e:
                if e in self.error_statistics.keys():
                    self.error_statistics[e] += 1
                else:
                    self.error_statistics[e] = 1

class duplicates_preprocessing_OpenAPS_AAPS_Uploader(duplicates_preprocessing_OpenAPS_NS):
    """
    OpenAPS AAPS_Uploader: direct-sharing-396
    """
    def __init__(self, config_filename : str, config_path : str, dataset = "OpenAPS_AAPS_Uploader", console_log_level = logging.INFO):
        super().__init__(config_filename, config_path, dataset, console_log_level)

    def kinds_of_files(self, dir_name):
        """
        - determine different kinds of files: split filename by "_" and histogram them:
        - output: {'file_info': 4912, 'devicestatus': 161, 'entries': 161, 'treatments': 161, 'profile': 162}
        """
        file_types = {}
        for i, f in enumerate(glob.glob(os.path.join(f"{dir_name}","**", f"*.{self.file_ending}"), recursive=True)):
            head, tail = os.path.split(f)
            filename, _ = os.path.splitext(tail)

            if filename not in file_types.keys():
                file_types[filename] = 1
            else:
                file_types[filename] += 1

        return file_types



    def one_json2csv(self, dir_name : str, infile_name : str, outfile_name : str):
        """
        input: this function reads a json-file
        algo:
        - flattens the structure
        - filters a subset of columns
        output: produces an output csv file with one entry being one line in the output file

        """
        data = list()

        in_columns = ["value", "date"]
        with open(os.path.join(dir_name, infile_name)) as f:
            entries = json.load(f)
            if len(entries) < 1:
                logging.info(f"{infile_name} has 0 entries, therefore no output.")
                return
            for i, entry in enumerate(entries):
                try:
                    #pd.to_datetime(entry["date"])  # raises an exception, if the format is unexpected, thereby avoiding it being appended to data
                    # noise, 
                    row = ["1"]
                    row.extend([entry[column] for column in in_columns])
                    # unix_timestamp in ms is a 13 digit number, in s it is a 10 digit number (in 2022)
                    date_factor = 1.0
                    if np.log10(entry["date"]) > 12 and np.log10(entry["date"]) < 13: 
                        date_factor = 0.001
                    row.append(datetime.datetime.utcfromtimestamp(int(entry["date"]*date_factor)).strftime('%Y-%m-%d %H:%M:%S'))  # date is in msec
                    data.append(row)
                except KeyError as e:
                    if e in self.key_error_statistics.keys():
                        self.key_error_statistics[e] += 1
                    else:
                        self.key_error_statistics[e] = 1            
                except Exception as e:
                    if e in self.error_statistics.keys():
                        self.error_statistics[e] += 1
                    else:
                        self.error_statistics[e] = 1            
                    
            df = pd.DataFrame(data=data, columns=self.out_columns)
            df.to_csv(os.path.join(self.out_dir_name, outfile_name))
            del entries
        del data


    def all_entries_json2csv(self):
        """
        focus on AndroidAPS Uploader uploads: direct-sharing-396
        in_dir_name: AndroidAPS_Uploader/ (see IO.json)
        """
        file_list = sorted(glob.glob(os.path.join(f"{self.in_dir_name}","**", "direct-sharing-396","**", "*BgReadings.json"), recursive=True))
        logging.info(f"number of files: {len(file_list)}")
        logging.info(file_list[:3])  # head 
        logging.info(file_list[-3:])  # and tail
        for i, f in enumerate(file_list):
            # if i<80: continue
            head, tail = os.path.split(f)
            if i%10==0: logging.info(f"{i}, {head}, {tail}")
            filename, _ = os.path.splitext(tail)
            sub_dirs = head[len(self.in_dir_name):]
            sub_dirs = sub_dirs.strip('/')
            dir_name_components = sub_dirs.split("/")
            # first = 98120605, second = upload-num181-ver1-date20210322T001829-appid4bf04b5c787e4200a085419a8a32049f in 98120605/direct-sharing-396/upload-num181-ver1-date20210322T001829-appid4bf04b5c787e4200a085419a8a32049f/BgReadings.json
            first, second = dir_name_components[0], dir_name_components[2]
            if os.path.isfile(os.path.join(self.out_dir_name, first + "_entries_" + second + ".csv")): 
                continue

            params = Params(head, tail, filename, first, second)

            if i%10==0: logging.info(", ".join([f"{x}: {params[i]}" for i,x in enumerate(params._asdict().keys())]))
            try: 
                self.one_json2csv(head, tail, first + "_entries_" + second + ".csv")
                
            except (ValueError, Exception) as e:
                if e in self.error_statistics.keys():
                    self.error_statistics[e] += 1
                else:
                    self.error_statistics[e] = 1

class duplicates_preprocessing_OPENonOH_AAPS_Uploader(duplicates_preprocessing_OpenAPS_AAPS_Uploader):
    """
    OPENonOH AAPS_Uploader: direct-sharing-396
    """
    def __init__(self, config_filename : str, config_path : str, dataset = "OPENonOH_AAPS_Uploader", console_log_level = logging.INFO):
        super().__init__(config_filename, config_path, dataset, console_log_level)


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


    def all_entries_json2csv(self):
        """
        focus on AndroidAPS Uploader uploads: 
        """
        file_list = sorted(glob.glob(os.path.join(f"{self.in_dir_name}","**", "*BgReadings.json"), recursive=True))
        logging.info(f"number of files: {len(file_list)}")
        logging.info(file_list[:3])  # head 
        logging.info(file_list[-3:])  # and tail
        for i, f in enumerate(file_list):
            # if i<80: continue
            head, tail = os.path.split(f)
            if i%10==0: logging.info(f"{i}, {head}, {tail}")
            filename, _ = os.path.splitext(tail)
            sub_dirs = head[len(self.in_dir_name):]
            sub_dirs = sub_dirs.strip('/')
            dir_name_components = sub_dirs.split("/")
            # first = 98120605, second = upload-num181-ver1-date20210322T001829-appid4bf04b5c787e4200a085419a8a32049f in AndroidAPS_Uploader/98120605/direct-sharing-396/upload-num181-ver1-date20210322T001829-appid4bf04b5c787e4200a085419a8a32049f/BgReadings.json
            first, second = dir_name_components[0], dir_name_components[1]
            if os.path.isfile(os.path.join(self.out_dir_name, first + "_entries_" + second + ".csv")): 
                continue

            params = Params(head, tail, filename, first, second)

            if i%10==0: logging.info(", ".join([f"{x}: {params[i]}" for i,x in enumerate(params._asdict().keys())]))
            try: 
                self.one_json2csv(head, tail, first + "_entries_" + second + ".csv")
                
            except (ValueError, Exception) as e:
                if e in self.error_statistics.keys():
                    self.error_statistics[e] += 1
                else:
                    self.error_statistics[e] = 1




def main(dataset : str, config_filename : str = "IO.json", config_path : str = ".", console_log_level : str = "info"):
    if not console_log_level == "info":
        print("not yet implemented: requires translation of info to logging.INFO, etc.")

    if dataset == "OpenAPS_NS":  # is used as a key in the IO.json-file
        dpp = duplicates_preprocessing_OpenAPS_NS(config_filename, config_path)
        dpp.loop()
    elif dataset == "OpenAPS_AAPS_Uploader": 
        dpp = duplicates_preprocessing_OpenAPS_AAPS_Uploader(config_filename, config_path)
        dpp.loop()
    elif dataset == "OPENonOH":
        dpp = duplicates_preprocessing(config_filename, config_path)
        dpp.loop()
    elif dataset == "OPENonOH_AAPS_Uploader":
        dpp = duplicates_preprocessing_OPENonOH_AAPS_Uploader(config_filename, config_path)
        dpp.loop()
    else: 
        logging.error("unknown dataset: {dataset}, should be one of ['OpenAPS_NS', 'OPENonOH', 'OpenAPS_AAPS_Uploader']")

if __name__ == "__main__":
    fire.Fire(main)
