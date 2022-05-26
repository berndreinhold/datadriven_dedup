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
        self.output = { "root_data_dir_name": self.root_data_dir_name}

    def __del__(self):
        # create output directories and write dataframes to disk
        out_dir = os.path.join(self.root_data_dir_name, "generated_config")
        os.makedirs(os.path.join(out_dir), exist_ok=True)
        with open(os.path.join(out_dir, "config_pairwise.json"), "w") as f:
            json.dump(self.output, f, indent=4, sort_keys=True)
        print(f"outfile created: {os.path.join(out_dir, 'config_pairwise.json')}")

    def validate_config_file(self):
        # check for example, that the dimension of the matrix is compatible with the number of datasets
        print(self.IO_json)

    def individual_dataset(self):
        pass

    def duplicates_pairwise_processing(self):

        self.output["duplicates_pairwise"] = dict()
        self.output["duplicates_pairwise"]["diff_svg_threshold"] = 1e-4,  # is stored as list in the output json-file. A bug in the json lib?
        self.output["duplicates_pairwise"]["IO"] = self.list_pairwise_duplicates()

    def list_pairwise_duplicates(self) -> list:
        """
        return a list of the pairwise duplicates for the datasets to be written to the config file
        """
        pairwise_duplicates = []
        for i,ds in enumerate(self.core["input"]):
            for i2, ds2 in enumerate(self.core["input"]):
                one_pair = {}
                if i < i2:
                    one_pair["input"] = [ds, ds2]
                    one_pair["output"] = ["",f"duplicates_{ds[2]}_{ds2[2]}_per_day.csv", f"duplicates_{ds[2]}_{ds2[2]}"]
                    pairwise_duplicates.append(one_pair)
        return sorted(pairwise_duplicates, key=lambda x: x["output"][2])

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
