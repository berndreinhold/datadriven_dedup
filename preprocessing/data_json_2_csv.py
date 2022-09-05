from distutils.command.config import config
import json
import os
import zipfile
from collections import namedtuple
import pandas as pd
import glob
import fire
import logging
import datetime
import numpy as np
import shutil

"""
call as: python3 duplicates_json2csv.py --dataset=[...] [--config_filename=IO.json] [--config_path="."]

takes a directory containing files with a file_ending specified in the config file (typically: "json") and transforms them into csv files.

"""
Params = namedtuple("Params", ['head', 'tail', 'filename', 'first', 'second'])


def search_key(json_input, search_key_ : str, data_ : list, cols : list):
    """
    search all instances of search_key_ in json_input, which can be of different data types
    if it is a list, call search_key() recursively 
    in case of a dict, either call search_key() on the value of the (key,value)-pair

    expects timestamp to comply with ISO8601 format, which e.g. 2019-02-03T09:38:20.000Z does

    Actually match just the keys in a key-value pair.
    Do it in a case-insensitive manner.

    cols: a list of column names expected to be "rate", "duration", "timestamp"
    """
    search_key_ = search_key_.lower()
    assert(cols[0]=="rate")
    assert(cols[1]=="duration")
    assert(cols[2]=="timestamp")
    if isinstance(json_input, list):
        for item in json_input:
            search_key(item, search_key_, data_, cols)
    elif isinstance(json_input, dict):
        for key_, value_ in json_input.items():
            if search_key_ in key_.lower():
                unix_ts = -1
                try:
                    # raises an exception, if the format is unexpected
                    # recognized format: 2019-02-03T09:38:20.000Z
                    unix_ts = (pd.to_datetime(value_[cols[2]], infer_datetime_format=True) - pd.Timestamp("1970-01-01", tz='UTC')) / pd.Timedelta('1s')
                except Exception as e:
                    logging.debug(f"Error(unix_ts): {e}, {key_}, {value_}")

                try:
                    data_.append([value_[cols[0]], value_[cols[1]], unix_ts, value_[cols[2]]])
                except KeyError as e:
                    logging.debug(f"KeyError: {e}, {key_}, {value_}")
                except Exception as e:
                    logging.debug(f"Error: {e}, {key_}, {value_}")
            else:
                search_key(value_, search_key_, data_, cols)
    else:
        raise TypeError(f"json_input should be either a list or dict, but was {type(json_input)}: {json_input}")



class duplicates_json2csv(object):
    """
    process the BG data of OPENonOH
    it is on the one hand a base class for the other combinations of (OpenAPS, OPENonOH) and (NightScout (NS), AAPS_Uploader) processing classes and 
    it is the OPENonOH_NS json2csv class
    """

    def __init__(self, config_filename: str, config_path: str, dataset: str = "OPENonOH_NS", console_log_level=logging.INFO):
        """
        read a config file and populate input and output file names and paths:
            - output a csv file with key [user_id, date] and one entry per measurement
        @param dataset: is used as a key in the json-config-file.
        """

        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)
        self.dataset = dataset
        self.root_data_dir_name = IO_json["root_data_dir_name"]

        datasets = [x for x in IO_json["duplicates_json2csv"].keys() if "comment" and "logging" not in x]
        assert(self.dataset in datasets)

        self.json_input = IO_json["duplicates_json2csv"][self.dataset]["input"]
        self.json_output = IO_json["duplicates_json2csv"][self.dataset]["output"]
        self.json_logging = IO_json["duplicates_json2csv"]["logging"]

        self.in_dir_name = os.path.join(
            self.root_data_dir_name, self.json_input["dir_name"])
        self.in_columns = IO_json["in_columns"]
        self.out_columns = IO_json["out_columns"] 

        self.file_ending = self.json_input["file_ending"]

        self.out_dir_name = self.json_output["dir_name"]
        os.makedirs(self.out_dir_name, exist_ok=True)

        # logging
        # adapted from https://github.com/acschaefer/duallog/blob/master/duallog/duallog.py
        os.makedirs(self.json_logging["dir_name"], exist_ok=True)
        # basic config with file handler, that records everything
        logging.basicConfig(filename=os.path.join(
            self.json_logging["dir_name"], f"{__class__.__name__}.log"), level=logging.DEBUG)
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

    def one_json2csv(self, dir_name: str, infile_name: str, outfile_name: str, i : int):
        """
        input: this function reads a json-file
        algo:
        - flattens the structure
        - filters a subset of columns
        output: produces an output csv file with one entry being one line in the output file

        # search for the "enacted" keyword
        """
        keyword_instances = list()
        with open(os.path.join(dir_name, infile_name)) as f:
            try:
                entries = json.load(f)
            except Exception as e:
                logging.debug(f"Error: {e}")
            if len(entries) < 1:
                logging.info(
                    f"{i}, {infile_name} has 0 entries, therefore no output.")
                return
            else:
                logging.info(
                    f"{i}, {infile_name} has {len(entries)} entries.")

            search_key(entries, "Setting temp basal", keyword_instances, self.in_columns)  # rate, duration, unix_timestamp, datetime (string)
            #print(enacted_instances)

            if len(enacted_instances) < 1: 
                print(f"no 'enacted' found in file {infile_name}.")
                #if not entry["type"] == "sgv":
                #    continue
            for i in range(0):
                if i < 1: continue
                try:
                    # raises an exception, if the format is unexpected, thereby avoiding it being appended to data
                    pd.to_datetime(entry["timestamp"])
                    # noise, sgv, date, dateString 
                    data.append([entry["enacted"]["rate"], entry["enacted"]["duration"], -1, entry["enacted"]["timestamp"], infile_name])
                    
                    if 0:
                        if "noise" in entry:
                            data.append([entry[column]
                                        for column in self.in_columns])
                        else:
                            # put the noise variable to -1, if it is not present
                            new_entry = [-1]
                            # add the columns except noise
                            new_entry.extend([entry[column] for column in self.in_columns if "noise" not in column])
                            data.append(new_entry)
                except KeyError as e:
                    logging.debug("KeyError: ", e, entry)
                    if e in self.key_error_statistics.keys():
                        self.key_error_statistics[e] += 1
                    else:
                        self.key_error_statistics[e] = 1
                except Exception as e:
                    logging.debug("Error: ", e, entry["dateString"])
                    if e in self.error_statistics.keys():
                        self.error_statistics[e] += 1
                    else:
                        self.error_statistics[e] = 1

            df = pd.DataFrame(data=enacted_instances, columns=self.out_columns)
            os.makedirs(os.path.join(self.root_data_dir_name,
                        self.out_dir_name), exist_ok=True)
            df.to_csv(os.path.join(self.root_data_dir_name,
                      self.out_dir_name, outfile_name))
            #logging.info(f"{os.path.join(self.root_data_dir_name, self.out_dir_name, outfile_name)} written.")
            del entries
        del enacted_instances

    def all_entries_json2csv(self):
        file_list = sorted(glob.glob(os.path.join(
            f"{self.in_dir_name}", "**", "direct-sharing-31", "**", f"devicestatus*.{self.file_ending}"), recursive=True))
        logging.info(f"number of files: {len(file_list)}")
        logging.info(file_list[:3])  # head
        logging.info(file_list[-3:])  # and tail
        #logging.info(file_list)

        for i, f in enumerate(file_list):
            if not (i > 27  and i <= 28): continue
            #logging.info(f"{i}, {f}")
            head, tail = os.path.split(f)
            sub_dirs = head[len(self.in_dir_name):]
            dir_name_components = sub_dirs.split("/")
            # first = 82868075, second = 21672228 in home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/82868075/21672228/entries__to_2020-09-11
            # home/reinhold/Daten/OPEN_4ds/n=101_OPENonOH_07.07.2022/00749582/direct-sharing-31
            if 0:  # 2020 data
                first, second = dir_name_components[0], dir_name_components[1]
                filename, _ = os.path.splitext(tail)
                if os.path.isfile(os.path.join(self.out_dir_name, first + "_" + second + "_" + filename + ".csv")):
                    continue
                self.one_json2csv(head, tail, first + "_" +
                                second + "_" + filename + ".csv", i)
            else:  # 2022 data: n=101_OPENonOH_07.07.2022 
                first = dir_name_components[1]
                filename, _ = os.path.splitext(tail)
                #if os.path.isfile(os.path.join(self.out_dir_name, first + "_" + filename + ".csv")):
                #    continue
                self.one_json2csv(head, tail, first + "_" + filename + ".csv", i)
    
    def extract_json_gz(self, dir_name):
        # for the OpenAPS_NS (nightscout) dataset
        # gunzip json.gz to json files
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/00749582/69754"
        # file_name = "entries__to_2018-06-07.json"
        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/Open Humans Data/00749582/69756"
        # file_name = "profile__to_2018-06-07.json"

        # dir_name = "/home/reinhold/Daten/OPEN/OPENonOH_Data/OpenHumansData/"
        search_string = os.path.join(
            f"{dir_name}", "**", f"*.{self.file_ending}.gz")
        logging.info(search_string)
        json_gz = set([x
                      for x in sorted(glob.glob(search_string, recursive=True))])
        print(json_gz)
        json_ = set(sorted(glob.glob(search_string[:-3], recursive=True)))

        if len(json_gz ^ json_) > 0:
            logging.debug(
                f"json files without corresponding json.gz file: {json_ - json_gz}")
            logging.debug(
                f"json.gz files without corresponding json file: {json_gz - json_}")

        for i, f in enumerate(json_gz - json_):
            os.system(f"gunzip {f}")
            if i % 100 == 0:
                logging.info(f"{i}, {f}")

    def extract_AAPS_Uploader_zip(self, dir_name, unzip_option: str = "-n -q"):
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
        file_list = ["BgReadings.json", "APSData.json", "TemporaryBasals.json", "Treatments.json", "TempTargets.json", "CareportalEvents.json", \
            "ProfileSwitches.json", "ApplicationInfo.json", "Preferences.json", "DeviceInfo.json", "DisplayInfo.json", "UploadInfo.json"]

        search_string = os.path.join(dir_name, "**", f"*.zip")
        logging.info(search_string)
        # zipped and zipped_files are necessary, since the BgReadings.json and the upload...zip are in the same directory,
        # so in order to decide, which zip files to unzip, one needs to compare the directories, but in order to actually unzip, one needs the files
        zipped = set([os.path.split(x)[0] for x in sorted(
            glob.glob(search_string, recursive=True))])  # just record the directory
        # just record the directory
        zipped_files = set(sorted(glob.glob(search_string, recursive=True)))
        search_string2 = os.path.join(dir_name, "**", f"*BgReadings.json")
        extracted = set([os.path.split(x)[0] for x in sorted(
            glob.glob(search_string2, recursive=True))])

        if len(zipped ^ extracted) > 0:
            logging.debug(
                f"zipped files without corresponding BgReadings.json file: {zipped - extracted}")
            logging.debug(
                f"BgReadings.json files without zip file in the same directory: {extracted - zipped}")

        """
        for i, f in enumerate(zipped_files):
            head, tail = os.path.split(f)
            if head not in zipped - extracted:
                continue
            os.system(f"unzip {unzip_option} {f} -d {head}")
            if i % 100 == 0:
                logging.info(f"{i}, {f}")
        """

        for i, f in enumerate(sorted(glob.glob(search_string, recursive=True))):   # get the list of files
            head, tail = os.path.split(f)
            pm_id = head[len(self.root_data_dir_name):]
            pm_id = pm_id.split("/")[1]
            if tail.startswith("nightscout"): continue
            #if head not in zipped - extracted:
            #    continue
            if zipfile.is_zipfile(f): # if it is a zipfile, extract it
                
                with zipfile.ZipFile(f) as item: # treat the file as a zip
                    item.extractall()  # extract it in the working directory
                    if i % 100 == 0:
                        logging.info(f"{i}, {f}")
                    #print(file)
                    x = tail.split("-")
                    num = int(x[1].replace('num', ''))
                    date = x[3].replace('date', '')

                    for file_ in file_list:
                        if os.path.exists(file_):
                            shutil.move(file_, os.path.join("/media/reinhold/Elements/OPENonOH_07.07.2022/", f'{self.dataset}_{pm_id}_{num:03}_{date}_{file_}'))

    def kinds_of_files(self, dir_name):
        """
        - determine different kinds of files: split filename by "_" and histogram them:
        - output: {'file_info': 4912, 'devicestatus': 161, 'entries': 161, 'treatments': 161, 'profile': 162}
        """
        file_types = {}
        for i, f in enumerate(glob.glob(os.path.join(f"{dir_name}", "**", "direct-sharing-31", "**", f"*.{self.file_ending}"), recursive=True)):
            head, tail = os.path.split(f)
            filename, _ = os.path.splitext(tail)
            filename_components = filename.split("_")
            if filename_components[0] == "file":
                filename_components[0] = filename_components[0] + \
                    "_" + filename_components[1]

            if filename_components[0] not in file_types.keys():
                file_types[filename_components[0]] = 1
            else:
                file_types[filename_components[0]] += 1

        return file_types

    def loop(self, unzip : bool = False):
        """
        loop through all available files and do the same thing on each file
        """
        if "_NS" in self.dataset:
            if unzip: 
                self.extract_json_gz(self.in_dir_name)
            else: 
        elif "_AAPS_Uploader" in self.dataset:
            if unzip: 
                self.extract_AAPS_Uploader_zip(self.in_dir_name)
            else: 
                print("self.extract_AAPS_Uploader_zip(self.in_dir_name) temporarily disabled!")

        file_types = self.kinds_of_files(self.in_dir_name)
        logging.info(file_types)
        self.all_entries_json2csv()


class duplicates_json2csv_OpenAPS_NS(duplicates_json2csv):
    """
    OpenAPS Nightscout files: direct-sharing-31
    """

    def __init__(self, config_filename: str, config_path: str, dataset: str = "OpenAPS_NS", console_log_level=logging.INFO):
        super().__init__(config_filename, config_path, dataset, console_log_level)

    def kinds_of_files(self, dir_name):
        """
        determine different kinds of files: split filename by "_" and histogram them:
        """
        file_types1, file_types2 = {}, {}
        for i, f in enumerate(glob.glob(os.path.join(f"{self.in_dir_name}", "**", "direct-sharing-31", "**", f"*.{self.file_ending}"), recursive=True)):
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
        file_list = sorted(glob.glob(os.path.join(
            f"{self.in_dir_name}", "**", "direct-sharing-31", "**", "*entries*.json"), recursive=True))
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
                if first not in filename:
                    raise Exception(
                        f"first not in filename: {head}, {tail}: {first}, {second}")
                self.one_json2csv(head, tail, filename + ".csv")

            except (ValueError, Exception) as e:
                if e in self.error_statistics.keys():
                    self.error_statistics[e] += 1
                else:
                    self.error_statistics[e] = 1


class duplicates_json2csv_OpenAPS_AAPS_Uploader(duplicates_json2csv_OpenAPS_NS):
    """
    OpenAPS AAPS_Uploader: direct-sharing-396
    """

    def __init__(self, config_filename: str, config_path: str, dataset="OpenAPS_AAPS_Uploader", console_log_level=logging.INFO):
        super().__init__(config_filename, config_path, dataset, console_log_level)

    def kinds_of_files(self, dir_name):
        """
        - determine different kinds of files: split filename by "_" and histogram them:
        - output: {'file_info': 4912, 'devicestatus': 161, 'entries': 161, 'treatments': 161, 'profile': 162}
        """
        file_types = {}
        for i, f in enumerate(glob.glob(os.path.join(f"{dir_name}", "**", "direct-sharing-396", "**", f"*.{self.file_ending}"), recursive=True)):
            head, tail = os.path.split(f)
            filename, _ = os.path.splitext(tail)

            if filename not in file_types.keys():
                file_types[filename] = 1
            else:
                file_types[filename] += 1

        return file_types

    def one_json2csv(self, dir_name: str, infile_name: str, outfile_name: str):
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
                logging.info(
                    f"{infile_name} has 0 entries, therefore no output.")
                return
            for i, entry in enumerate(entries):
                try:
                    # pd.to_datetime(entry["date"])  # raises an exception, if the format is unexpected, thereby avoiding it being appended to data
                    # noise,
                    row = ["1"]
                    row.extend([entry[column] for column in in_columns])
                    # unix_timestamp in ms is a 13 digit number, in s it is a 10 digit number (in 2022)
                    date_factor = 1.0
                    if np.log10(entry["date"]) > 12 and np.log10(entry["date"]) < 13:
                        date_factor = 0.001
                    row.append(datetime.datetime.utcfromtimestamp(
                        int(entry["date"]*date_factor)).strftime('%Y-%m-%d %H:%M:%S'))  # date is in msec
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
            os.makedirs(os.path.join(self.root_data_dir_name,
                        self.out_dir_name), exist_ok=True)
            df.to_csv(os.path.join(self.root_data_dir_name,
                      self.out_dir_name, outfile_name))
            del entries
        del data

    def all_entries_json2csv(self):
        """
        focus on AndroidAPS Uploader uploads: direct-sharing-396
        in_dir_name: AndroidAPS_Uploader/ (see IO.json)
        """
        file_list = sorted(glob.glob(os.path.join(
            f"{self.in_dir_name}", "**", "direct-sharing-396", "**", "*BgReadings.json"), recursive=True))
        logging.info(f"number of files: {len(file_list)}")
        logging.info(file_list[:3])  # head
        logging.info(file_list[-3:])  # and tail
        for i, f in enumerate(file_list):
            # if i<80: continue
            head, tail = os.path.split(f)
            if i % 10 == 0:
                logging.info(f"{i}, {head}, {tail}")
            filename, _ = os.path.splitext(tail)
            sub_dirs = head[len(self.in_dir_name):]
            sub_dirs = sub_dirs.strip('/')
            dir_name_components = sub_dirs.split("/")
            # first = 98120605, second = upload-num181-ver1-date20210322T001829-appid4bf04b5c787e4200a085419a8a32049f in 98120605/direct-sharing-396/upload-num181-ver1-date20210322T001829-appid4bf04b5c787e4200a085419a8a32049f/BgReadings.json
            first, second = dir_name_components[0], dir_name_components[2]
            if os.path.isfile(os.path.join(self.out_dir_name, first + "_entries_" + second + ".csv")):
                continue

            params = Params(head, tail, filename, first, second)

            if i % 10 == 0:
                logging.info(", ".join(
                    [f"{x}: {params[i]}" for i, x in enumerate(params._asdict().keys())]))
            try:
                self.one_json2csv(head, tail, first +
                                  "_entries_" + second + ".csv")

            except (ValueError, Exception) as e:
                if e in self.error_statistics.keys():
                    self.error_statistics[e] += 1
                else:
                    self.error_statistics[e] = 1


class duplicates_json2csv_OPENonOH_AAPS_Uploader(duplicates_json2csv_OpenAPS_AAPS_Uploader):
    """
    OPENonOH AAPS_Uploader: direct-sharing-396
    """

    def __init__(self, config_filename: str, config_path: str, dataset="OPENonOH_AAPS_Uploader", console_log_level=logging.INFO):
        super().__init__(config_filename, config_path, dataset, console_log_level)

    def kinds_of_files(self, dir_name):
        """
        - determine different kinds of files: split filename by "_" and histogram them:
        - output: {'file_info': 4912, 'devicestatus': 161, 'entries': 161, 'treatments': 161, 'profile': 162}
        """
        file_types = {}
        for i, f in enumerate(glob.glob(os.path.join(f"{dir_name}", "**", "direct-sharing-396", "**", f"*.{self.file_ending}"), recursive=True)):
            head, tail = os.path.split(f)
            filename, _ = os.path.splitext(tail)
            filename_components = filename.split("_")
            if filename_components[0] == "file":
                filename_components[0] = filename_components[0] + \
                    "_" + filename_components[1]

            if filename_components[0] not in file_types.keys():
                file_types[filename_components[0]] = 1
            else:
                file_types[filename_components[0]] += 1

        return file_types

    def all_entries_json2csv(self):
        """
        focus on AndroidAPS Uploader uploads: 
        """
        file_list = sorted(glob.glob(os.path.join(
            f"{self.in_dir_name}", "**", "direct-sharing-396", "**", "*APSData.json"), recursive=True))  # BGReadings.json for bg data analysis, APSData for pump info
        logging.info(f"number of files: {len(file_list)}")
        logging.info(file_list[:3])  # head
        logging.info(file_list[-3:])  # and tail
        for i, f in enumerate(file_list):
            # if i<80: continue
            head, tail = os.path.split(f)
            if i % 10 == 0:
                logging.info(f"{i}, {head}, {tail}")
            filename, _ = os.path.splitext(tail)
            sub_dirs = head[len(self.in_dir_name):]
            sub_dirs = sub_dirs.strip('/')
            dir_name_components = sub_dirs.split("/")
            # first = 98120605, second = upload-num181-ver1-date20210322T001829-appid4bf04b5c787e4200a085419a8a32049f in AndroidAPS_Uploader/98120605/direct-sharing-396/upload-num181-ver1-date20210322T001829-appid4bf04b5c787e4200a085419a8a32049f/BgReadings.json
            first, second = dir_name_components[0], dir_name_components[1]
            if os.path.isfile(os.path.join(self.out_dir_name, first + "_entries_" + second + ".csv")):
                continue

            params = Params(head, tail, filename, first, second)

            if i % 10 == 0:
                logging.info(", ".join(
                    [f"{x}: {params[i]}" for i, x in enumerate(params._asdict().keys())]))
            try:
                self.one_json2csv(head, tail, first +
                                  "_entries_" + second + ".csv")

            except (ValueError, Exception) as e:
                if e in self.error_statistics.keys():
                    self.error_statistics[e] += 1
                else:
                    self.error_statistics[e] = 1


def main(dataset: str, config_filename: str = "IO.json", config_path: str = ".", console_log_level: str = "info"):
    if not console_log_level == "info":
        print("not yet implemented: requires translation of info to logging.INFO, etc.")

    if dataset == "OpenAPS_NS":  # is used as a key in the IO.json-file
        dpp = duplicates_json2csv_OpenAPS_NS(config_filename, config_path)
        dpp.loop()
    elif dataset == "OpenAPS_AAPS_Uploader":
        dpp = duplicates_json2csv_OpenAPS_AAPS_Uploader(
            config_filename, config_path)
        dpp.loop()
    elif dataset == "OPENonOH" or dataset == "OPENonOH_NS":
        dpp = duplicates_json2csv(config_filename, config_path)
        dpp.loop()
    elif dataset == "OPENonOH_AAPS_Uploader":
        dpp = duplicates_json2csv_OPENonOH_AAPS_Uploader(
            config_filename, config_path)
        dpp.loop()
    else:
        logging.error(
            f"unknown dataset: {dataset}, should be one of ['OpenAPS_NS', 'OPENonOH' or 'OPENonOH_NS', 'OpenAPS_AAPS_Uploader']")


if __name__ == "__main__":
    fire.Fire(main)
