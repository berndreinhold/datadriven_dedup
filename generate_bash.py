#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
from copy import deepcopy

"""
call as: python3 generate_bash.py [arguments]

Read a config_master.json file and generate 
a master bash script to run the sequence of scripts

In case of simulation all the steps are included in the bash script, 
while in case of real data the sequence of preprocessing steps depends on the data itself. 
Therefore only the later stage steps are in the bash script.
The distinction between real data and sim is based on the presence of a "artificial_data" section in the config_master json file.

output:
generate bash script in the current directory to run the later stage (real data), if not all steps (sim) of the processing pipeline.


"""

class generateBash():
    def __init__(self, config_filename : str, config_path : str):
        f = open(os.path.join(config_path, config_filename))
        # reading the IO_json config file
        IO_json = json.load(f)
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.root_software_dir_name = IO_json["root_software_dir_name"]
        self.master_script = IO_json["master_script"]

        try: 
            IO_json["artificial_data"]
            self.sim = True
        except KeyError as e:
            self.sim = False


    def command_string_sim(self):
        """
        set of command strings for simulated data.
        """
        buffer = f"""
#!/bin/bash
python3 {self.root_software_dir_name}/generate_artificial_data.py config_master_sim_4ds.json {self.root_software_dir_name}/config/ 
python3 {self.root_software_dir_name}/generate_config_json.py config_master_sim_4ds.json {self.root_software_dir_name}/config/

python3 {self.root_software_dir_name}/pairwise_duplicates.py config_pairwise.json {self.root_data_dir_name}/generated_config/
python3 {self.root_software_dir_name}/link_all_datasets.py config_all.json {self.root_data_dir_name}/generated_config/
python3 {self.root_software_dir_name}/plot/upset_venn3_plot.py config_viz.json {self.root_data_dir_name}/generated_config/
echo "call 'days_per_person_n_dataset.ipynb' separately (paths need to be adjusted inside this notebook)"
        """
        return buffer

    def command_string_real(self):
        """
        set of command strings for "real" data.
        """
        buffer = f"""
#!/bin/bash
python3 {self.root_software_dir_name}/generate_config_json.py config_master_sim_4ds.json {self.root_software_dir_name}/config/

python3 {self.root_software_dir_name}/pairwise_duplicates.py config_pairwise.json {self.root_data_dir_name}/generated_config/
python3 {self.root_software_dir_name}/link_all_datasets.py config_all.json {self.root_data_dir_name}/generated_config/
python3 {self.root_software_dir_name}/plot/upset_venn3_plot.py config_viz.json {self.root_data_dir_name}/generated_config/
this bash script assumes, that the processing steps of each individual datasets has already taken place (see your config master json file)
        """
        return buffer


    def __del__(self):
        with open(os.path.join(self.root_software_dir_name, self.master_script), "w") as f:
            if self.sim: f.write(self.command_string_sim())
            else: f.write(self.command_string_real())
        print(f"master bash script created: {os.path.join(self.root_software_dir_name, self.master_script)}")




def main(config_filename : str = "config_master_sim.json", config_path : str = "config/"):
    gb = generateBash(config_filename, config_path)
    

if __name__ == "__main__":
    fire.Fire(main)
