#/usr/bin/env python3
import pandas as pd
import os
import json
import fire

"""
call as: python3 generate_config_json.py

Read a config_master.json file and generate 
three config json files: 
1. for processing of each individual dataset: config_sim_individual_datasets.json or config_individual_datasets.json
2. for pairwise processing: config_ 
3. for all together


output:




"""

class generate_config_json():
    def __init__(self, config_filename : str, config_path : str):
        f = open(os.path.join(config_path, config_filename))
        # reading the IO_json config file
        IO_json = json.load(f)
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.core = IO_json["core"]
        self.steps = IO_json["steps"]

        # self.output = IO_json["output"]
        self.count_datasets = len(self.core["input"])
        self.IO_json = IO_json
        self.validate_config_file()

        # output
        self.output = {}

    def __del__(self):
        # create output directories and write dataframes to disk
        with open(os.path.join(self.root_data_dir_name,"test.json"), "w") as f:
            json.dump(self.output, f, indent=4, sort_keys=True)
        print(f"outfile created: {os.path.join(self.root_data_dir_name, 'test.json')}")

    def validate_config_file(self):
        # check for example, that the dimension of the matrix is compatible with the number of datasets
        print(self.IO_json)

    def individual_dataset(self):
        pass

    def duplicates_pairwise_processing(self):

        self.output["duplicates_pairwise"] = dict()
        self.output["duplicates_pairwise"]["diff_svg_threshold"] = 1e-4,
        self.output["duplicates_pairwise"]["IO"] = []


    def all(self):
        pass

    def loop(self):
        """
        loop through all steps in the config file
        """
        for step in self.steps:
            if step == "individual_dataset":
                pass
            elif step == "pairwise":
                json_output = self.duplicates_pairwise_processing()
                print(json_output)
            elif step == "all":
                self.all()



def main(config_filename : str = "config_master_sim.json", config_path : str = "."):
    # print("you can run it on one duplicate plot-pair, or you run it on all of them as they are listed in config.json. See class all_duplicates.")
    ad = generate_config_json(config_filename, config_path)
    ad.loop()

if __name__ == "__main__":
    fire.Fire(main)
