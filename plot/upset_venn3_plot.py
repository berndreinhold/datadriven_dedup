import pandas as pd
from matplotlib_venn import venn3, venn3_circles
import os
import json
from matplotlib import pyplot as plt
import upsetplot as usp

class upsetPlot():
    """
    this class produces the upset plot all pairwise plots listed in the generated config_viz.json-file read as input. (see generate_config_json.py and config_master*.json)
    """
    def __init__(self, var : str, config_filename : str, config_path : str):
        """
        read two (generated) config files: 
        """
        f = open(os.path.join(config_path, config_filename))
        IO_json = json.load(f)

        # var is either "per_pm_id" or "per_pm_id_date"
        self.root_data_dir_name = IO_json["root_data_dir_name"]
        input = IO_json["summary_plots"]["input"][var]
        dataset_labels = IO_json["summary_plots"]["dataset_labels"]
        self.output = IO_json["summary_plots"]["upsetplot_output"][var]

        cols = []
        for ds in dataset_labels:
            cols.append(ds)
        print(cols)

        df = pd.read_csv(os.path.join(self.root_data_dir_name, input[0], input[1]), header=0, parse_dates=[1], index_col=0)
        for i in range(len(dataset_labels)):
            df[cols[i]] = ~pd.isna(df[f"pm_id_{i}"])  # check for pm_id being present or not

        self.df = df[cols]
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
        usp.plot(df2, subset_size='count')
        plt.title(self.output[2])
        plt.savefig(os.path.join(self.root_data_dir_name, self.output[0], self.output[1]), bbox_inches='tight')
        print(f"created image: {os.path.join(self.root_data_dir_name, self.output[0], self.output[1])}")

        # upset = usp.UpSet(df2, subset_size='count', intersection_plot_elements=3)
        # upset.plot()
        #upset.add_catplot(value='median_value', kind='strip', color='blue')
        #upset.add_catplot(value='AGE', kind='strip', color='black')




class venn3Plot(upsetPlot):
        # for venn3 diagramm
        output = IO_json["summary_plots"]["venn3plot_output"][var]
        cols2 = cols[0:2]
        cols2.append(cols[3])
        print(cols,cols2)
        df_v1 = df2[df2[cols[2]]==True].groupby(cols2, dropna=False).agg("count")
        print(df_v1)
        #print(type(df_v1))
        #df_v1.info()


        # %%
        #print(df_v1.index)
        #print(df_v1.index.names)
        plt.rcParams["font.size"] = 18.0  # 10 by default
        data = {}
        for i in df_v1.index:
            print(i, df_v1.loc[i])
            i_str = "".join([str(int(i_k)) for i_k in i])
            data[i_str] = df_v1.loc[i]
        print(data)

        plt.figure(figsize=(15,15))
        plt.tight_layout()
        venn3(subsets = data, set_labels=df_v1.index.names)
        plt.title(output[2])
        plt.savefig(os.path.join(root_data_dir_name, output[0], output[1]), bbox_inches='tight')
        print(f"created image: {os.path.join(root_data_dir_name, output[0], output[1])}")

        #venn3(subsets = (data["100"], data["010"], data["110"], data["001"], data["101"], data["011"], data["111"]), set_labels=("OPENonOH", "OpenAPS_NS", "OPENonOH_AAPS_Uploader"), alpha=0.5)

        # %%
        # just for cross check: should return an empty dataframe for the artificial data, a dataframe with all true for the real data
        df4 = df1[(df1[cols[0]]==True) & (df1[cols[1]]==True) & (df1[cols[2]]==True)]
        print(df4)



def main():
    # load the config files and the variables from the upset_plot-section of the config file
    config_path = "/home/reinhold/Daten/OPEN_4ds/generated_config"
    config_filename = "config_viz.json"
    var = "pm_id_only"  # "pm_id_date"

    pass


if __name__ == "__main__":
    main()
