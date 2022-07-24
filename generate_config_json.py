#/usr/bin/env python3
import pandas as pd
import os
import json
import fire
from copy import deepcopy

"""
call as: python3 generate_config_json.py

Read a config_master.json file and generate 
three config json files: 
1. for processing of each individual dataset: config_sim_individual_datasets.json or config_individual_datasets.json
2. for pairwise processing: config_ 
3. for all together


output:
generate config_viz.json and other config files so that only one config file has to be loaded for upsetplot_venn3.ipynb, pairwise_plot.py

"""

class generate_config_json():
    def __init__(self, config_filename : str, config_path : str):
        f = open(os.path.join(config_path, config_filename))
        # reading the IO_json config file
        IO_json = json.load(f)
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        self.core = IO_json["core"]
        self.steps = IO_json["steps"]
        self.logging = IO_json["logging"]

        # self.output = IO_json["output"]
        self.count_datasets = len(self.core["individual"])
        self.IO_json = IO_json
        self.validate_config_file()

        # output
        self.output = { "root_data_dir_name": self.root_data_dir_name}

    def __del__(self):
        # create output directories and write dataframes to disk
        out_dir = os.path.join(self.root_data_dir_name, "generated_config")
        os.makedirs(os.path.join(out_dir), exist_ok=True)
        output_json = { "root_data_dir_name": self.root_data_dir_name}
        output_json["duplicates_pairwise"] = deepcopy(self.output["duplicates_pairwise"])
        output_json["logging"] = deepcopy(self.logging)
        self.save_config_json(output_json, "config_pairwise.json")
        output_json = { "root_data_dir_name": self.root_data_dir_name}
        output_json["link_all_datasets"] = deepcopy(self.output["link_all_datasets"])
        output_json["logging"] = deepcopy(self.logging)
        self.save_config_json(output_json, "config_all.json")
        output_json = { "root_data_dir_name": self.root_data_dir_name}
        output_json["summary_plots"] = deepcopy(self.output["summary_plots"])
        output_json["pairwise_plots"] = deepcopy(self.output["pairwise_plots"])
        output_json["logging"] = deepcopy(self.logging)
        self.save_config_json(output_json, "config_viz.json")
        output_json = { "root_data_dir_name": self.root_data_dir_name}
        output_json["columns"] = ["noise", "sgv", "date", "dateString"]
        output_json["duplicates_json2csv"] = deepcopy(self.output["duplicates_json2csv"])
        output_json["duplicates_aggregation"] = deepcopy(self.output["duplicates_aggregation"])
        output_json["self_duplicates"] = deepcopy(self.output["self_duplicates"])
        output_json["logging"] = deepcopy(self.logging)
        self.save_config_json(output_json, "config_preprocessing.json")



    def save_config_json(self, output_json, json_filename):
        out_dir = os.path.join(self.root_data_dir_name, "generated_config")
        with open(os.path.join(out_dir, json_filename), "w") as f:
            json.dump(output_json, f, indent=4, sort_keys=True)
        print(f"config file created: {os.path.join(out_dir, json_filename)}")


    def validate_config_file(self):
        # check for example, that the dimension of the matrix is compatible with the number of datasets
        print(self.IO_json)

    def config_individual_dataset(self):
        self.output["duplicates_json2csv"] = dict()
        self.output["duplicates_json2csv"] = self.dict_individual_json2csv()
        self.output["duplicates_json2csv"]["logging"] = self.logging
        self.output["duplicates_aggregation"] = dict()
        self.output["duplicates_aggregation"] = self.dict_individual_aggregation()
        self.output["duplicates_aggregation"]["logging"] = self.logging
        self.output["self_duplicates"] = dict()
        self.output["self_duplicates"] = self.dict_self_duplicates()


    def dict_individual_json2csv(self) -> dict:
        """
        return a dict of the json2csv of the individual datasets
        """
        individual_ds = dict()
        for i,ds in enumerate(self.core["individual"]):
                individual_ds[ds[3]] = dict()
                individual_ds[ds[3]]["input"] = dict()
                individual_ds[ds[3]]["input"]["file_ending"] = "json"
                individual_ds[ds[3]]["input"]["dir_name"] = "n=101_OPENonOH_07.07.2022"
                individual_ds[ds[3]]["output"] = dict()
                individual_ds[ds[3]]["output"]["dir_name"] = os.path.join(ds[0], "per_measurement_csv")
        return individual_ds


    def dict_individual_aggregation(self) -> dict:
        """
        return a dict of the aggregation of the individual datasets
        """
        individual_ds = dict()
        for i,ds in enumerate(self.core["individual"]):
                individual_ds[ds[3]] = dict()
                individual_ds[ds[3]]["input"] = dict()
                individual_ds[ds[3]]["input"]["file_pattern"] = "*entries*.csv"
                individual_ds[ds[3]]["input"]["dir_name"] = os.path.join(ds[0].strip(), "per_measurement_csv")
                individual_ds[ds[3]]["output"] = dict()
                individual_ds[ds[3]]["output"]["dir_name"] = ds[0]
                individual_ds[ds[3]]["output"]["file_name"] = f"{ds[3].strip()}_per_day_with-self-duplicates.csv"
        return individual_ds

    def dict_self_duplicates(self) -> dict:
        """
        return a dict of the self duplicates of the individual datasets
        """
        individual_ds = dict()
        for i,ds in enumerate(self.core["individual"]):
                individual_ds[ds[3]] = dict()
                individual_ds[ds[3]]["input"] = dict()
                individual_ds[ds[3]]["dir_name"] = os.path.join(ds[0].strip())
                individual_ds[ds[3]]["in_file_name"] = f"{ds[3]}_per_day_with-self-duplicates.csv"
                individual_ds[ds[3]]["out_file_name"] = f"{ds[3]}_per_day.csv"
                assert individual_ds[ds[3]]["out_file_name"] == ds[1]

        return individual_ds

    def config_pairwise_json(self):

        self.output["duplicates_pairwise"] = dict()
        self.output["duplicates_pairwise"]["diff_svg_threshold"] = 1e-4,  # is stored as list in the output json-file. A bug in the json lib?
        self.output["duplicates_pairwise"]["IO"] = self.list_pairwise_duplicates()

    def list_pairwise_duplicates(self) -> list:
        """
        return a list of the pairwise duplicates for the datasets to be written to the config file
        """
        pairwise_duplicates = []
        for i,ds in enumerate(self.core["individual"]):
            for i2, ds2 in enumerate(self.core["individual"]):
                one_pair = {}
                if i < i2:
                    one_pair["input"] = [ds, ds2]
                    one_pair["output"] = ["",f"duplicates_{ds[3]}_{ds2[3]}_per_day.csv", f"duplicates ({ds[2]}-{ds2[2]})", f"duplicates_{ds[3]}_{ds2[3]}", f"{i}-{i2}"]
                    pairwise_duplicates.append(one_pair)
        return sorted(pairwise_duplicates, key=lambda x: x["output"][3])


    def config_all_json(self):
        """
        create a config file for the combination and aggregation of all datasets
        """

        # comment for the created config_all.json file
        self.output["link_all_datasets"] = {}
        comment_keys = []
        for key in self.core:
            if "comment" in key:
                self.output["link_all_datasets"][key] = self.core[key]
                comment_keys.append(key)
        #if "comment0" in self.output["link_all_datasets"]:  # let's avoid overwriting comments in the config file
        #    raise KeyError(f"comment0 already in config file: {self.output['link_all_datasets']['comment0']}")
        new_comment_key = ""
        for i in range(10000):
            if f"comment{i}" in comment_keys: continue
            else: 
                new_comment_key = f"comment{i}"
                break

        self.output["link_all_datasets"][new_comment_key] = "duplicate datasets: [dir_name, file_name, human-readable label, machine-readable label, id]"
        self.output["link_all_datasets"]["individual"] = self.core["individual"]
        duplicates = [x["output"] for x in self.list_pairwise_duplicates()]
        self.output["link_all_datasets"]["duplicate"] = duplicates
        self.output["link_all_datasets"]["output"] = self.core["output"]


    def config_viz_json(self):
        """
        create a config file for the visualisation
        """
        self.output["summary_plots"] = {}
        self.output["summary_plots"]["input"] = self.core["output"]
        self.output["summary_plots"]["dataset_labels"] = [x[2] for x in self.core["individual"]]
        self.output["summary_plots"]["upsetplot_output"] = self.upsetplot_output()
        self.output["summary_plots"]["days_per_person_output"] = ["img/", "days_per_person_n_dataset.png", "histogram of days per person in the respective datasets"]

        self.output["venn3_plots"] = self.venn3_plots()
        self.output["pairwise_plots"] = self.pairwise_plots()

    def upsetplot_output(self):
        out = {}
        out["per_pm_id"] = ["img/", "upsetplot_per_pm_id.png", "persons in the respective datasets", "per_pm_id"]
        out["per_pm_id_date"] = ["img/", "upsetplot_per_pm_id_date.png", "person-days in the respective datasets", "per_pm_id_date"]
        out["comment"] = "[dir_name, file_name, title], paths are os.path.join('root_data_dir_name','dir_name', 'filename'), paths should end on '/'"
        return out

    def venn3_plots(self):
        """
            a venn3 diagram involves three datasets.
            If more datasets are present then all datasets except three need to be held fixed for one given venn3-plot. 
            Then the variables, that are being held fixed, need to be looped through. 
        """

        out = {}
        out["input"] = self.core["output"]
        for var in ["per_pm_id", "per_pm_id_date"]:
                out[f"output_{var}"] = self.list_venn3(var)    
                
        return out

    def list_venn3(self, var):
        for outside_venn3_index in range(len()):
            self.list_venn3(self, var, outside_venn3_index)
        

    def list_venn3(self, var, outside_venn3_index) -> list:
        """
        return a list of the venn3 duplicates for the datasets to be written to the config file
        """
        plot_pairs = []
        for i,ds in enumerate(self.core["individual"]):
            for i2, ds2 in enumerate(self.core["individual"]):
                one_pair = {}
                if i < i2:
                    one_pair["data"] = [i, i2, f"{i}-{i2}"]
                    one_pair["axis_label"] = [f"{ds[2]}", f"{ds2[2]}"]
                    one_pair["img"] = ["img/",f"pairwise_plot_{ds[3]}_{ds2[3]}_per_day.png", f"duplicates ({ds[2]}-{ds2[2]})", f"duplicates_{ds[3]}_{ds2[3]}", f"{i}-{i2}"]
                    if i==0: one_pair["comment"] = "the list of keys in 'data' refer back to section 'link_all_datasets' -> 'input' and 'plots' -> 'dataset_labels'"
                    plot_pairs.append(one_pair)
        
        return sorted(plot_pairs, key=lambda x: x["data"])


    def pairwise_plots(self):
        """
        create a config file section for the pairwise plots
        """
        out = {}
        out["input"] = self.core["output"]["per_pm_id_date"]
        
        out["output"] = self.list_plot_pairs()
        # out_pc: output plot_config
        out_pc = {"title_prefix" : "two datasets and their duplicates"}
        print("TODO: Warning! This code has no colors beyond 4 datasets")
        out_pc["colors"] = {"0" : "green", "1" : "red", "2" : "blue", "3" : "orange"}
        out_pc["colors"]["duplicates"] = "black"
        out_pc["colors"]["comment"] = "colors are the same per dataset in all plots"
        out["plot_config"] = out_pc
        return out

    def list_plot_pairs(self) -> list:
        """
        return a list of the pairwise duplicates for the datasets to be written to the config file
        """
        plot_pairs = []
        for i,ds in enumerate(self.core["individual"]):
            for i2, ds2 in enumerate(self.core["individual"]):
                one_pair = {}
                if i < i2:
                    one_pair["data"] = [i, i2, f"{i}-{i2}"]
                    one_pair["axis_label"] = [f"{ds[2]}", f"{ds2[2]}"]
                    one_pair["img"] = ["img/",f"pairwise_plot_{ds[3]}_{ds2[3]}_per_day.png", f"duplicates ({ds[2]}-{ds2[2]})", f"duplicates_{ds[3]}_{ds2[3]}", f"{i}-{i2}"]
                    if i==0: one_pair["comment"] = "the list of keys in 'data' refer back to section 'link_all_datasets' -> 'input' and 'plots' -> 'dataset_labels'"
                    plot_pairs.append(one_pair)
        
        return sorted(plot_pairs, key=lambda x: x["data"])


    def loop(self):
        """
        loop through all steps in the config file
        """
        for step in self.steps:
            if step == "individual_dataset":
                # only required for "real" data (in contrast to "artificial" data simulated in this package) 
                self.config_individual_dataset()
            elif step == "pairwise":  
                # this is not on the pairwise_plots, rather on pairwise dayly dataset similarity calculation
                # for pairwise plots see config_viz__json()
                self.config_pairwise_json()
            elif step == "all":
                self.config_all_json()
            elif step == "viz":
                self.config_viz_json()  # includes the pairwise plots


def main(config_filename : str = "config_master_sim.json", config_path : str = "config/"):
    # print("you can run it on one duplicate plot-pair, or you run it on all of them as they are listed in config.json. See class all_duplicates.")
    ad = generate_config_json(config_filename, config_path)
    ad.loop()

if __name__ == "__main__":
    fire.Fire(main)
