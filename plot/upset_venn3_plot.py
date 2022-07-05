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
        print(self.cols)

        df = pd.read_csv(os.path.join(self.root_data_dir_name, input[0], input[1]), header=0, parse_dates=[1], index_col=0)
        for i in range(len(dataset_labels)):
            df[self.cols[i]] = ~pd.isna(df[f"pm_id_{i}"])  # check for pm_id being present or not

        self.df = df[self.cols]
        print(self.df)


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




class venn3Plot(upsetPlot):
        # for venn3 diagramm
    def __init__(self, var : str, config_filename : str, config_path : str):
        super().__init__(var, config_filename, config_path)
        self.output = self.IO_json["summary_plots"]["venn3plot_output"][var]
        cols2 = self.cols[0:2]
        cols2.append(self.cols[3])
        print(self.cols,cols2)
        self.df_venn3 = self.df[self.df[self.cols[2]]==False].groupby(cols2, dropna=False).agg("count")
        print(self.df_venn3)
        #print(type(self.df_venn3))
        #self.df_venn3.info()


    def plot(self):
        #print(self.df_venn3.index)
        #print(self.df_venn3.index.names)
        plt.rcParams["font.size"] = 18.0  # 10 by default
        data = {}
        for i in self.df_venn3.index:
            #print(i, self.df_venn3.loc[i])
            i_str = "".join([str(int(i_k)) for i_k in i])
            data[i_str] = self.df_venn3.loc[i]
        print(data)

        plt.figure(figsize=(15,15))
        plt.tight_layout()
        venn3(subsets = data, set_labels=self.df_venn3.index.names)
        plt.title(self.output[2])
        plt.savefig(os.path.join(self.root_data_dir_name, self.output[0], self.output[1]), bbox_inches='tight')
        print(f"created image: {os.path.join(self.root_data_dir_name, self.output[0], self.output[1])}")

def main(config_filename : str, config_path : str):
    # load the config files and the variables from the upset_plot-section of the config file
    ups_p = upsetPlot("per_pm_id", config_filename, config_path)
    ups_p.plot()
    ups_p = upsetPlot("per_pm_id_date", config_filename, config_path)
    ups_p.plot()
    
    venn_3p = venn3Plot("per_pm_id", config_filename, config_path)
    venn_3p.plot()
    venn_3p = venn3Plot("per_pm_id_date", config_filename, config_path)
    venn_3p.plot()



if __name__ == "__main__":

    # config_path = "/home/reinhold/Daten/OPEN_4ds/generated_config"
    # config_filename = "config_viz.json"
    fire.Fire(main)

# %%
