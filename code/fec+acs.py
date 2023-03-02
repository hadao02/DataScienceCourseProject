# Packages
import pandas as pd
import numpy as np

# Display up to 500 columns with print()
pd.options.display.max_columns = 500

# Suppress scientific float notation
pd.set_option('display.float_format', lambda x: '%.3f' % x)

##### ACS

# Read in data sets
acs02 = pd.read_csv("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\raw_data\\ACS2021data\\ACSDP1Y2021.DP02-Data.csv")
acs03 = pd.read_csv("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\raw_data\\ACS2021data\\ACSDP1Y2021.DP03-Data.csv")

# Select and rename variables of interest
vars02 = ["NAME", "DP02_0016E", "DP02_0094PE", "DP02_0092PM", "DP02_0065PE"]
vars03 = ["NAME", "DP03_0062E", "DP03_0009PE", "DP03_0128PE"]
acs02 = acs02.loc[:, vars02]
acs02.rename(columns={acs02.columns[1]: "hh_size"}, inplace=True)
acs02.rename(columns={acs02.columns[2]: "foreign_born_%"}, inplace=True)
acs02.rename(columns={acs02.columns[3]: "out_of_state_born_%"}, inplace=True)
acs02.rename(columns={acs02.columns[4]: "educ_bsc_%"}, inplace=True)
acs03 = acs03.loc[:, vars03]
acs03.rename(columns={acs03.columns[1]: "med_income"}, inplace=True)
acs03.rename(columns={acs03.columns[2]: "unemployment_%"}, inplace=True)
acs03.rename(columns={acs03.columns[3]: "poverty_line_%"}, inplace=True)

# Merge on NAME
acs = pd.merge(acs02, acs03, on='NAME')

# Drop Puerto Rico
acs = acs.drop(acs[acs["NAME"].str.contains("Puerto Rico")].index)

# Drop description row
acs.drop(0, inplace=True)

# Replace states with abbreviations
abbrevs = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
}
def convert_state(state):
    state_name = state.split(",")[-1].strip()
    abbreviation = abbrevs[state_name]
    return ", ".join([state.split(", ")[0], abbreviation])
acs["NAME"] = acs["NAME"].apply(convert_state)

# Generate congr. distr. index
acs["words"] = acs["NAME"].str.split()
acs["distr"] = acs["words"].apply(lambda x: f"{x[-1]}-{x[2]}")
acs.drop(["words", "NAME"], axis=1, inplace=True) # drop old name columns
acs["distr"] = acs["distr"].apply(lambda x: x.replace("(at", "0")) # rename at-large districts
acs.insert(0, "distr", acs.pop("distr"))

# Convert observations to numbers
acs.iloc[:, 1:] = acs.iloc[:, 1:].astype(float)
acs.med_income = round(acs.med_income, 0).astype(int) # round median income var

# Control results
print(acs)
descr = acs.loc[:, acs.columns != "distr"].apply(lambda x: [x.median(), x.mean(), x.var(), x.min(), x.max()])
stats = ["median", "mean", "variance", "min", "max"]
descr.insert(0, "statistic", stats)
print(descr)

# Save data
acs.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\generated_data\\acs.xlsx", sheet_name="selected ACS 2021 variables", index=False)
descr.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\generated_data\\acs_descr.xlsx", sheet_name="selected ACS 2021 variables, description", index=False)


####### FEC

# Read in data set
fec = pd.read_csv("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\raw_data\\candidate_summary_2022.csv")

# Select variables of interest
fec = fec.drop(fec.columns[9:], axis=1).drop(fec.columns[:1], axis=1)

# Drop observations with no district IDs and senate candidates
fec.dropna(subset=["Cand_Office_Dist"], how="any", inplace=True)
fec = fec.drop(fec[fec["Cand_Office"].str.contains("S")].index)

# Create unique district ID
fec["Cand_Office_Dist"] = fec["Cand_Office_Dist"].astype(int).astype(str) # convert district id to string
fec["distr"] = fec["Cand_Office_St"].str.cat(fec["Cand_Office_Dist"], sep="-")
fec.drop(["Cand_Office_St", "Cand_Office_Dist"], axis=1, inplace=True) # drop old name columns
fec.insert(0, "distr", fec.pop("distr"))

# Save FEC file
fec.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\generated_data\\fec.xlsx", sheet_name="FEC midterms 2022", index=False)

# Check
print(fec)


###### MERGED

# Implement many-to-one merge
merged = fec.merge(acs, on="distr", how="left")

# Delete candidates with zero and n.a. campaign spending
merged = merged[merged["Total_Receipt"] != 0]
merged.dropna(subset=["Total_Receipt"], how="any", inplace=True)
print(merged)

# Save merged data set
merged.to_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\generated_data\\fec+acs.xlsx", sheet_name="FEC & ACS data", index=False)
