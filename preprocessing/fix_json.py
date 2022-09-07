#! /usr/bin/env python3
import os
import glob
import json
import pandas as pd

def json_str_recursive(filename : str, f_str : str, length : int):
    try:
        print(filename, length)
        entries = json.loads(f_str[:length])
        return entries, length
    except json.JSONDecodeError as e:
        return json_str_recursive(filename, f_str, length - 1)

def json_str(filename : str, f_str : str, length : int):
    #max_offset = int(0.2*length)
    max_offset = 10
    for offset in range(max_offset):
        #if offset % 100 == 0:
        #    print(filename, length, offset)
        try:
            entries = json.loads(f_str[:length-offset])
            return entries, length-offset
        except json.JSONDecodeError as e:
            pass
            #if offset % 100 == 0:
            #    print(e, filename, length, offset)
    return "", length-offset
    

def main():
    dir_name = "/media/reinhold/Elements/OPENonOH_AAPS_Uploader_07.07.2022/"
    data_ = []
    for i,infile_name in enumerate(glob.glob(os.path.join(dir_name, "*APSData.json"))):
        head, tail = os.path.split(infile_name)
        with open(os.path.join(dir_name, infile_name)) as f:
            f_str = f.read()
            entries, length = json_str(infile_name, f_str, len(f_str))
            data_.append([tail, head, len(f_str), entries != ""])
            if i % 100 == 0: 
                print([tail, head, len(f_str), entries != ""])
            
    df = pd.DataFrame(data = data_, columns=["infile_name", "infile_dir", "file_size", "well formed JSON"])
    df.to_csv("OPENonOH_07.07.2022_AAPS_Uploader_APSData_well_formed_JSON.csv")

if __name__ == "__main__":
    main()