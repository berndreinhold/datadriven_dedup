#/usr/bin/env python3

from ast import parse
import os
import pandas as pd
from matplotlib import pyplot as plt

def PGdata() -> pd.DataFrame:
    """
    focus on OPENonOH data
    return separately for the two uploaders (NS and AAPS Uploader)
    grouped by pm_id
    """
    input = ["/home/reinhold/Daten/dana_processing/", "data_per_pm_id_date.csv"]
    df = pd.read_csv(os.path.join(input[0], input[1]), header=0, parse_dates=["date"]) 
    df_groupby = grouping_PGdata(df, "pm_id_1", "OPENonOH_NS_PG_count_days.csv")
    df2_groupby = grouping_PGdata(df, "pm_id_2", "OPENonOH_AAPS_Uploader_PG_count_days.csv")
    return df_groupby, df2_groupby  # NS, AAPS Uploader

def grouping_PGdata(df : pd.DataFrame, pm_id_col : str, out_filename : str) -> pd.DataFrame:
    df_groupby = df.loc[~df[pm_id_col].isnull(), ["date", pm_id_col]].groupby(pm_id_col,as_index=False).count()
    df_groupby.rename(columns={"date": "count_days", pm_id_col: "pm_id"}, inplace=True)
    df_groupby = df_groupby.astype({"pm_id": 'int32'})
    df_groupby.to_csv(out_filename, index=False)
    return df_groupby

def real_date(df : pd.DataFrame, pm_id_col) -> pd.DataFrame:
    """
    fix mistake in the date column
    """
    df['date'] = df['date'].apply(lambda x: x.date())
    df_per_date = df.drop_duplicates(subset=["date", pm_id_col])
    df = df_per_date[[pm_id_col, "date"]]
    return df

def AIDdata() -> pd.DataFrame:
    """
    focus on OPENonOH data
    return separately for the two uploaders (NS and AAPS Uploader)
    grouped by pm_id: count(date)
    """
    input = ["/home/reinhold/Daten/dana_processing/OPENonOH_NS_Data", "AID_data_per_pm_id_date.csv"]
    df = pd.read_csv(os.path.join(input[0], input[1]), header=0, parse_dates=["date"])
    df_groupby = grouping_AIDdata(df, "pm_id", "OPENonOH_NS_AID_count_days.csv")

    
    input2 = ["/home/reinhold/Daten/dana_processing/OPENonOH_AAPS_Uploader_Data", "AID_data_per_pm_id_date.csv"]
    df2 = pd.read_csv(os.path.join(input2[0], input2[1]), header=0, parse_dates=["date"])
    df2_groupby = grouping_AIDdata(df2, "pm_id", "OPENonOH_AAPS_Uploader_AID_count_days.csv")
    return df_groupby, df2_groupby  # NS, AAPS Uploader

def grouping_AIDdata(df : pd.DataFrame, pm_id_col : str, out_filename : str) -> pd.DataFrame:
    df_mod = real_date(df, pm_id_col)
    df_groupby = df_mod.loc[~df[pm_id_col].isnull(), ["date", pm_id_col]].groupby(pm_id_col,as_index=False).count()
    df_groupby.rename(columns={"date": "count_days", pm_id_col: "pm_id"}, inplace=True)
    df_groupby = df_groupby.astype({"pm_id": 'int32'})
    df_groupby.to_csv(out_filename, index=False)
    return df_groupby
    

def plot(df : pd.DataFrame, title : str, img_prefix : str):
    """
    plot AAPS and NS data each with three subfigures
    first subfigure: count_days_PG vs count_days_AID
    second subfigure: count_days_PG
    third subfigure: count_days_AID
    PG: plasma glucose data, AID: automated insulin delivery data
    """
    fig, ax = plt.subplots(1, 3, figsize=(15, 4))
    fig.suptitle(title)

    xy = df.loc[~df["count_days_PG"].isnull() & ~df["count_days_AID"].isnull(), ["count_days_PG", "count_days_AID"]].values
    ax[0].scatter(xy[:,0],xy[:,1], label="PG vs AID")
    ax[0].grid(True)
    ax[0].set_xlabel("PG count_days")
    ax[0].set_ylabel("AID count_days")

    PG = df.loc[~df["count_days_PG"].isnull() & df["count_days_AID"].isnull(), ["count_days_PG"]].values
    ax[1].set_title("PG count_days (no AID data present at all)")
    ax[1].hist(PG, bins=20, label="PG data, no AID data")
    ax[1].set_xlabel("PG count_days")
    ax[1].set_ylabel("count/bin")

    AID = df.loc[df["count_days_PG"].isnull() & ~df["count_days_AID"].isnull(), ["count_days_AID"]].values
    ax[2].set_title("AID count_days (no PG data present at all)")
    ax[2].hist(AID, bins=20, label="no PG data, AID data")
    ax[2].set_xlabel("AID count_days")
    ax[2].set_ylabel("count/bin")

    fig.tight_layout()
    fig.savefig(f"{img_prefix}_PG_AID.png")

def main():
    df_PG_NS, df_PG_AAPS = PGdata()
    df_AID_NS, df_AID_AAPS = AIDdata()
    
    #df["project_OPENonOH"] = df["project_OPENonOH"]
    df_NS = df_PG_NS.merge(df_AID_NS, on="pm_id", how="outer", suffixes=("_PG", "_AID"))
    df_AAPS = df_PG_AAPS.merge(df_AID_AAPS, on="pm_id", how="outer", suffixes=("_PG", "_AID"))

    plot(df_NS, "NS PG and AID data", "NS")
    plot(df_AAPS, "AAPS PG and AID data", "AAPS")

    df_NS.to_csv("NS_PG_AID_count_days.csv", index=False)
    df_AAPS.to_csv("AAPS_Uploader_PG_AID_count_days.csv", index=False)

if __name__ == "__main__":
    main()
