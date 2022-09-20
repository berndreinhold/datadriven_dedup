#/usr/bin/env python3

import os
import pandas as pd

def main():
    input = ["/home/reinhold/Daten/OPEN_4ds/", "data_per_pm_id_with_project_info_int.csv"]
    input2 = ["/home/reinhold/Daten/dana_processing/OPENonOH_NS_Data", "AID_data_per_pm_id.csv"]

    df = pd.read_csv(os.path.join(input[0], input[1]), header=0)
    #df["project_OPENonOH"] = df["project_OPENonOH"]
    df2 = pd.read_csv(os.path.join(input2[0], input2[1]), header=0)
    print(df.columns, df2.columns)
    
    df2["person_id"] = range(len(df2))
    df2["pm_id_1"] = df2[" pm_id_1"].astype(int)
    df3 = df.merge(df2, left_on="project_OPENonOH", right_on="pm_id_1")

    print(df3)


if __name__ == "__main__":
    main()
