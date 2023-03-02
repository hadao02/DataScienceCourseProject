# Packages
import pandas as pd
import itertools
import numpy as np
import os

# Display up to 500 columns with print()
pd.options.display.max_columns = 500

################## Part I: combine data sets #########################

# Read Twitter text data
tw = pd.read_pickle("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\intermediate_data\\tweets.pkl")
tw.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\final_data\\tweets.xlsx", sheet_name="Tweets", index=False)

# Read cross-section data
cs = pd.read_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\generated_data\\house.xlsx")

# Drop redundant columns from cs and twitter data
cs.drop(columns=["Unnamed: 0"], inplace=True)
cand_vars = ["candidate_name", "state", "district", "Cand_Party_Affiliation", "incumbent_from_results", "Percent of Votes", "Total_Receipt"]
twit_vars = ["Campaign Twitter_handle", "Official Twitter_handle", "Personal Twitter", "no_twitter"]
acs_vars = ["hh_size", "foreign_born_%", "out_of_state_born_%", "educ_bsc_%", "med_income", "unemployment_%", "poverty_line_%"]
varlists = [cand_vars, acs_vars, twit_vars]
all_vars = []
for list in varlists:
    for var in list:
        all_vars.append(var)
cs = cs[all_vars]
#tw = tw[["user", "tweet"]]

# Fill in "NaN" positions
cs["no_twitter"].fillna(0, inplace=True) # 1 if true
cs["Percent of Votes"].fillna(0, inplace=True)
cs = cs.groupby(["state", "district"]).apply(lambda x: x.fillna(value=x.iloc[0]))

# Rename variables for convenience and consistency
tw.rename(columns={tw.columns[0]: "user"}, inplace=True)
tw.rename(columns={tw.columns[1]: "tweet"}, inplace=True)
cs.rename(columns={cs.columns[0]: "name"}, inplace=True)
cs.rename(columns={cs.columns[2]: "distr"}, inplace=True)
cs.rename(columns={cs.columns[3]: "party"}, inplace=True)
cs.rename(columns={cs.columns[4]: "incumbent"}, inplace=True)
cs.rename(columns={cs.columns[5]: "votes"}, inplace=True)
cs.rename(columns={cs.columns[6]: "receipt"}, inplace=True)
cs.rename(columns={cs.columns[14]: "campaign_twitter"}, inplace=True)
cs.rename(columns={cs.columns[15]: "official_twitter"}, inplace=True)
cs.rename(columns={cs.columns[16]: "personal_twitter"}, inplace=True)

# Convert bool statements to binary in incumbent var
cs.incumbent = cs.incumbent.astype(int)

# Save intermediate df as pkl and run script to fill in missing values from the acs csv
cs.to_pickle("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\intermediate_data\\cross_section.pkl")
os.system("fill_in.py") # run script to fill in missing values from ACS data set
cs = pd.read_pickle("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\intermediate_data\\cross_section.pkl") # load updated data
cs.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\intermediate_data\\filled_in.xlsx", sheet_name="Twitter data with controls", index=False)

# Check for missing values (result: one missing observation)
    #cs.med_income.fillna(0, inplace=True)
    #no_income = []
    #for x in cs.med_income:
    #    if x == 0:
    #        no_income.append(x)
    #print(len(no_income))
# Drop the missing observation
cs.dropna(subset=["med_income"], how="any", inplace=True)

# Round total receipt & convert median income to int
cs.receipt = round(cs.receipt, 0).astype(int)
cs.med_income = cs.med_income.astype(int)

# Construct a Boolean district winner variable
grouped = cs.groupby(["state", "distr"])
cs["winner"] = grouped["votes"].transform(lambda x: x == x.max()).astype(int) # produces weird results

##################################################################

#  Construct a winning margin and a Boolean close election (margin <3pp) variables (didnt work yet)
#cs["vote_margin"] = None # margin column
#cs["votes_2nd"] = cs.groupby(["state", "distr"])["votes"].apply(lambda x: x.nlargest(2).iloc[1] if len(x) > 1 and x.nunique() > 1 else None).reset_index(drop=True) # second highest vote number
#cs["vote_margin"] = abs(cs["votes"] - cs["votes_2nd"]) # difference with 2nd highest votes (vote margin for losers)
#winners = cs[cs["winner"] == 1] # temporary df with election winners
#cs.loc[cs["winner"] == 1, "vote_margin"] = abs(cs[cs["winner"] == 1]["votes"] - winners["votes"]) # vote margin for winners
#cs.drop(columns=["votes_2nd"], inplace=True) # drop column of 2nd highest votes

#################################################################

#  Construct a vote margin and a Boolean close race (margin <3pp) variables
# Delete all non-dem or -rep candidates -> delete all races with less than two cands
cs = cs[cs["party"].isin(["DEM", "REP"])] # drop all non-dem or -rep candidates
#cs = cs[~cs[["state", "distr"]].duplicated()] # drop all rows with one candidate
cs = cs.groupby(["state", "distr"]).filter(lambda x: len(x) > 1)
#cs["vote_margin"] = cs.groupby(["state", "distr"])["votes"].transform(lambda x: abs(x[x[cs["party"]] == "DEM"] - x[x[cs["party"]] == "REP"]))
cs["vote_margin"] = cs.groupby(["state", "distr"])["votes"].transform(lambda x: abs(x.iloc[0] - x.iloc[1]))
cs["close_race"] = None
cs["close_race"] = cs["vote_margin"].where(cs["vote_margin"] < 0.03, 0).fillna(1)
cs.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\intermediate_data\\margin.xlsx", sheet_name="xx", index=False)

# Note: compute number of left out observations

#################################################################

# Construct a winning margin and a Boolean close election (margin <3pp) variables
#cs["vote_margin"] = grouped.apply(lambda x: abs(x["votes"] - x[x["winner"] == 1]["votes"].iloc[0])).reset_index(drop=True)
#print(cs[["winner", "vote_margin"]])
#print(cs)

# Remove "@" in Twitter handle columns (and link for "personal_twitter" column)
cs[["campaign_twitter", "official_twitter"]] = cs[["campaign_twitter", "official_twitter"]].apply(lambda x: x.str[1:])
cs["personal_twitter"] = cs["personal_twitter"].astype(str)
cs["personal_twitter"] = cs["personal_twitter"].str.replace("https://www.twitter.com/", "")

# Check
print(cs)

# Merge the two data frames (how="inner" -> delete unmatched rows)
campaign_tweets = tw.merge(cs, left_on="user", right_on=["campaign_twitter"], how="inner")
official_tweets = tw.merge(cs, left_on="user", right_on=["official_twitter"], how="inner") # note: zero Tweets contained
personal_tweets = tw.merge(cs, left_on="user", right_on=["personal_twitter"], how="inner")
print(len(personal_tweets))
tweets = [campaign_tweets, official_tweets, personal_tweets]
data = pd.concat(tweets)

# Check
print(data)

# Save complete data set (note: maybe change to csv if loading turns out to be slow)
data.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\final_data\\data.xlsx", sheet_name="Twitter data with controls", index=False)

################## Part II: describe data #########################

# Create data sets of candidates w and w/out Twitter data
cs_no_tw = cs[cs.no_twitter == 1]
cs_w_tw = cs[cs.no_twitter == 0]

# Generate descriptive statistics (later check for selection bias)
stats = ["median", "mean", "variance", "min", "max"]
descr_all = cs.iloc[:, 4:13].apply(lambda x: [x.median(), x.mean(), x.var(), x.min(), x.max()])
descr_all.insert(0, "statistic", stats)
descr_tw = cs_w_tw.iloc[:, 4:13].apply(lambda x: [x.median(), x.mean(), x.var(), x.min(), x.max()])
descr_tw.insert(0, "statistic", stats)
weighted_descr_tw = data.iloc[:, 6:15].apply(lambda x: [x.median(), x.mean(), x.var(), x.min(), x.max()])
weighted_descr_tw.insert(0, "statistic", stats) # weighted by number of Tweets

# Save descriptive statistics to a single xlsx file with three sheets
descr_all.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\final_data\\data_summary.xlsx", sheet_name="summary, all candidates", index=False)
descr_tw.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\final_data\\data_summary.xlsx", sheet_name="summary, candidates with Twitter activity (unweighted)", index=False)
weighted_descr_tw.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\final_data\\data_summary.xlsx", sheet_name="summary, candidates with Twitter activity (weighted)", index=False)

#Check
print(descr_all)
print(descr_tw)
print(weighted_descr_tw)


