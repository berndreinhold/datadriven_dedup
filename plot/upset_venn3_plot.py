from distutils.command.config import config
import pandas as pd
from matplotlib_venn import venn3, venn3_circles
import os
import json
from matplotlib import pyplot as plt
import upsetplot as usp
import fire

class upsetPlot():
    """
    this class produces the upset plot all pairwise plots listed in the generated config_viz.json-file read as input. (see generate_config_json.py and config_master*.json)
    """
    def __init__(self, var : str, config_filename : str, config_path : str):
        """
        read two (generated) config files: 
        """
        f = open(os.path.join(config_path, config_filename))
        self.IO_json = json.load(f)

        # var is either "per_pm_id" or "per_pm_id_date"
        self.root_data_dir_name = self.IO_json["root_data_dir_name"]
        input = self.IO_json["summary_plots"]["input"][var]
        dataset_labels = self.IO_json["summary_plots"]["dataset_labels"]
        self.output = self.IO_json["summary_plots"]["upsetplot_output"][var]

        self.cols = []
        for ds in dataset_labels:
            self.cols.append(ds)

        df = pd.read_csv(os.path.join(self.root_data_dir_name, input[0], input[1]), header=0, parse_dates=[1], index_col=0)
        for i in range(len(dataset_labels)):
            df[self.cols[i]] = ~pd.isna(df[f"pm_id_{i}"])  # check for pm_id being present or not

        self.df = df[self.cols]


    def plot(self):
        df2 = usp.from_indicators(self.df)  # returns a series
        #print(df2)
        #print(df2.axes)

        os.makedirs(os.path.join(self.root_data_dir_name, self.output[0]), exist_ok=True)
        plt.rcParams["font.size"] = 10.0
        plt.rcParams.update({"figure.facecolor" : (1.0, 1.0, 1.0, 1.0), "savefig.facecolor": (1.0, 1.0, 1.0, 1.0)})
        plt.figure(figsize=(15,15))
        plt.tight_layout()
        usp.plot(df2, subset_size='count', show_counts=True)
        plt.title(self.output[2])
        plt.savefig(os.path.join(self.root_data_dir_name, self.output[0], self.output[1]), bbox_inches='tight')
        print(f"created image: {os.path.join(self.root_data_dir_name, self.output[0], self.output[1])}")

        # upset = usp.UpSet(df2, subset_size='count', intersection_plot_elements=3)
        # upset.plot()
        #upset.add_catplot(value='median_value', kind='strip', color='blue')
        #upset.add_catplot(value='AGE', kind='strip', color='black')




class venn3Plot():
    """
    create venn3 diagrams. If more than 3 datasets are combined, e.g. four, 
    the fourth is kept fixed (either True or False) 
    and other three are used to produce the venn3 diagram.

    Very similar to pairwise_plot.py and UpsetPlot()

    The different combinations are read from config_viz.json, which has been generated via generate_config_json.py
    """

    def __init__(self, var : str, config_filename : str, config_path : str):
        """
        open the config file and read relevant parameters
        input is read from the config and the csv file is opened

        output is read from the config file in loop()
        """

        f = open(os.path.join(config_path, config_filename))
        self.IO_json = json.load(f)

        # var is either "per_pm_id" or "per_pm_id_date"
        self.root_data_dir_name = self.IO_json["root_data_dir_name"]
        self.input = self.IO_json["venn3_plots"]["input"][var]
        self.output = self.IO_json["venn3_plots"][f"output_{var}"]
        self.dataset_labels = self.IO_json["summary_plots"]["dataset_labels"]
        
        self.cols = []
        for ds in self.dataset_labels:
            self.cols.append(ds)

        df = pd.read_csv(os.path.join(self.root_data_dir_name, self.input[0], self.input[1]), header=0, parse_dates=[1], index_col=0)
        for i in range(len(self.dataset_labels)):
            df[self.cols[i]] = ~pd.isna(df[f"pm_id_{i}"])  # check for pm_id being present or not

        self.df = df[self.cols]
        #print(type(self.df_venn3))
        #self.df_venn3.info()


    def plot(self, output_ : list, outside_venn3_dataset_indices : list):
        """
        plot produces a venn3 diagram and saves it to disc
        output_ specifies a subdir (relative to root_data_dir_name), filename (for saving) and a title
        outside_venn3_dataset_indices is a list of dataset_indices, they become a list of columns to be selected below 
        """
        #print(self.df_venn3.index)
        #print(self.df_venn3.index.names)
        plt.rcParams["font.size"] = 18.0  # 10 by default
        data = {}
        # cols = [self.cols[i] for i in outside_venn3_dataset_indices]
        cols = "dataset 3"

        for i in self.df_venn3.index:
            i_str = "".join([str(int(i_k)) for i_k in i])
            data[i_str] = self.df_venn3.loc[i][cols]

        plt.figure(figsize=(15,15))
        plt.tight_layout()
        venn3(subsets = data, set_labels=self.df_venn3.index.names)
        plt.title(output_[2])
        plt.savefig(os.path.join(self.root_data_dir_name, output_[0], output_[1]), bbox_inches='tight')
        print(f"created image: {os.path.join(self.root_data_dir_name, output_[0], output_[1])}")

    def loop(self):
        """
        read the venn3_plot-section in config_viz.json and loop through these, both for the person-days and the days-section.
        """
        for item in self.output:
            # read data
            # outside_venn3 are those dataset indices that are kept fixed and did not make it into the venn3-diagram
            # by complement a list of dataset indices to create the venn3 diagram can be created, e.g. [0,1,2]
            outside_venn3 = item["outside_venn3"]  # a dictionary, with dataset_indices as keys

            outside_venn3_dataset_indices = set()
            x = None
            for dataset_index in outside_venn3:
                in_dataset = outside_venn3[dataset_index]  # needs dataset_index as orig.
                
                dataset_index = int(dataset_index)
                outside_venn3_dataset_indices.add(dataset_index)
                if x:
                    x = x & (self.df[self.cols[dataset_index]]==in_dataset) 
                else: 
                    x = (self.df[self.cols[dataset_index]]==in_dataset)
            # ToDo: what happens, if x == None
            # ToDo: better name for x

            venn3_dataset_indices = set(range(len(self.dataset_labels))) - outside_venn3_dataset_indices            
            cols2 = [self.cols[i] for i in venn3_dataset_indices]
            self.df_venn3 = self.df[x].groupby(cols2, dropna=False).agg("count")
            print(self.df_venn3)

            self.plot(item["img"], outside_venn3_dataset_indices)



def main(config_filename : str, config_path : str):
    # load the config files and the variables from the upset_plot-section of the config file
    """
    ups_p = upsetPlot("per_pm_id", config_filename, config_path)
    ups_p.plot()
    ups_p = upsetPlot("per_pm_id_date", config_filename, config_path)
    ups_p.plot()
    """

    venn_3p = venn3Plot("per_pm_id", config_filename, config_path)
    venn_3p.loop()
    #venn_3p = venn3Plot("per_pm_id_date", config_filename, config_path)
    #venn_3p.loop()



if __name__ == "__main__":

    # config_path = "/home/reinhold/Daten/OPEN_4ds/generated_config"
    # config_filename = "config_viz.json"
    fire.Fire(main)

