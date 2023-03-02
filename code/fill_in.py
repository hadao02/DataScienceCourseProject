# Packages
import pandas as pd

# Display up to 500 columns with print()
pd.options.display.max_columns = 500

# Load data
df1 = pd.read_pickle("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\intermediate_data\\cross_section.pkl")
df2 = pd.read_excel("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\generated_data\\fec+acs.xlsx")

# Construct district ID in df1
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
df1["state"] = df1["state"].apply(convert_state) # convert states to abbrevs using dictionary
df1["distr"] = df1["distr"].astype(str) # convert district id to string
df1["distr_id"] = df1["state"].str.cat(df1["distr"], sep="-")

# Fill in missing values using df2
df1.fillna(df2, inplace=True)

# Drop district id var again
df1.drop(["distr_id"], axis=1, inplace=True)

# Update intermediate data file
df1.to_pickle("C:\\Users\\kiral\\Documents\\GitHub\\ECON5166Project\\intermediate_data\\cross_section.pkl")
