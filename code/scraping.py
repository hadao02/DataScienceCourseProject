# Scraping including: candidate name, state, district id, twitter id (official, campaign, personal) and voting rate

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

house_url = "https://ballotpedia.org/United_States_House_of_Representatives_elections,_2022"
house_page = requests.get(house_url)

house_soup = BeautifulSoup(house_page.content, "html.parser")

house_infobox = house_soup.find("table", class_="infobox")

# Find the row with text "House Elections By State"
house_tr_header = house_infobox.find(lambda x: x.name == "tr" and
                                               x.find(text="U.S. House Elections by State"))

# The row after that one is a list of links to state election pages
house_tr_list = house_tr_header.find_next("tr")

# Get the states, mapping the name of the state to its URL
states = {a.text: "https://ballotpedia.org" + a.get("href") for a in house_tr_list.find_all("a")}
print(states)

def parse_state(url, state_name=None):
    names = []
    ballotpedia_pages = []
    district_nums = []

    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    district_tags = soup.find_all(lambda x: x.name == "p" and
                                            x.get_text().strip() == "General election candidates")
    # Loop through each district
    for i, district_tag in enumerate(district_tags):
        # The first <ul> after the district header is a list of candidates in general election
        ul_tag = district_tag.find_next("ul")
        if ul_tag is None:
            print("Error in", url)
            print("\tNo ul tag")
            return None, None, None

        # Each item under the general_tag has a link to a candidate
        for li_tag in ul_tag.find_all("li"):
            candidate_tag = li_tag.find("a")
            if candidate_tag is None:
                print("Error in", url)
                print("\tNo candidate tag")
                return None, None, None
            names.append(candidate_tag.text)
            ballotpedia_pages.append(candidate_tag.get("href"))
            district_nums.append(i + 1)
    ret = pd.DataFrame(columns=["Name", "Ballotpedia Page", "State", "District"])
    ret["Name"] = names
    ret["Ballotpedia Page"] = ballotpedia_pages
    ret["State"] = state_name
    ret["District"] = district_nums
    return ret

votebox_pattern = re.compile(".*General election for U\.S\. House.*")

def parse_candidate(url, name=None):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    # Intialize returning variables to None
    twtr_campaign, twtr_official, twtr_personal, party, is_incumbent, percent = [None] * 6

    # Get the infobox
    info_box = soup.find("div", class_="infobox person")
    if info_box is None:
        print("Error in", url)
        print("\tNo info box")
        return twtr_campaign, twtr_official, twtr_personal, party, is_incumbent, percent

    # Get the name, if not specified
    if name is None:
        # The name is always the first div in the infobox
        name_tag = info_box.find_next("div")
        if name_tag is None:
            print("Error in", url)
            print("\tNo name")
            return twtr_campaign, twtr_official, twtr_personal, party, is_incumbent, percent
        else:
            name = name_tag.get_text().strip()

    # Get the tags for links to the twitter pages
    twtr_campaign = info_box.find("a", text="Campaign Twitter")
    twtr_official = info_box.find("a", text="Official Twitter")
    twtr_personal = info_box.find("a", text="Personal Twitter")

    # If the tags exist, get the links
    if twtr_campaign is not None:
        twtr_campaign = twtr_campaign.get("href")
    if twtr_official is not None:
        twtr_official = twtr_official.get("href")
    if twtr_personal is not None:
        twtr_personal = twtr_personal.get("href")

    # Extract Party fomr infobox
    # This is underneath the photo of the candidate
    # (Or, the generic image used if there is no photo)
    # There's a <div> with the photo; the next div's text is the name of the party
    photo_div = info_box.find(lambda x: x.name == "div" and
                                        x.find("img"))
    if photo_div is None:
        print("Error in", url)
        print("\tNo photo")
        return twtr_campaign, twtr_official, twtr_personal, party, is_incumbent, percent

    party_div = photo_div.find_next("div")
    if party_div is None:
        print("Error in", url)
        print("\tNo party")
        return twtr_campaign, twtr_official, twtr_personal, party, is_incumbent, percent

    party = party_div.get_text().strip()

    # Get the table which contains the result of the election
    vote_box = soup.find(lambda x: x.find("h5", text=votebox_pattern),
                         class_="votebox")

    if vote_box is None:
        print("Error in", url)
        print("\tNo votebox")
        return twtr_campaign, twtr_official, twtr_personal, party, is_incumbent, percent

    # Find the cell which contains their name
    name_cell = vote_box.find(lambda x: name in x.get_text(),
                              class_="votebox-results-cell--text")
    if name_cell is None:
        print("Error in", url)
        print("\tNo name cell")
        return twtr_campaign, twtr_official, twtr_personal, party, is_incumbent, percent

    # If the candidate's name is underlined and bolded, they were an incumbent
    is_incumbent = name_cell.find("b") is not None and name_cell.find("u") is not None

    # The next cell contains the percent of votes they recived
    percent_cell = name_cell.find_next(class_="votebox-results-cell--number")
    if percent_cell is None:
        print("Error in", url)
        print("\tNo percent cell")
        return twtr_campaign, twtr_official, twtr_personal, party, is_incumbent, percent

    percent = percent_cell.get_text()
    if percent is not None:
        percent = percent.strip()
        try:
            percent = float(percent) / 100
        except ValueError:
            percent = 0
    else:
        percent = 0

    return twtr_campaign, twtr_official, twtr_personal, party, is_incumbent, percent


##Build a df with rows for each candidate and have their Name, Ballotpedia Page, State, and Twitter Pages

# Define the df
candidate_cols = ["Campaign Twitter", "Official Twitter", "Personal Twitter", "Party", "Incumbent?", "Percent of Votes"]
cols = ["Name", "Ballotpedia Page", "State", "District"] + candidate_cols
df = pd.DataFrame(columns=cols)
frames = []
# Loop through each state
for state_name, state_page in states.items():
    print(state_name)
    # Extract the names and ballotpedia pages for each candidate in the state
    state_df = parse_state(state_page)

    # Extract the twitters for each candidate
    state_candidates = [parse_candidate(candidate_page) for candidate_page in state_df["Ballotpedia Page"]]
    state_candidates_df = pd.DataFrame(state_candidates, columns=candidate_cols)

    # Expand the state dataframe to include state name and twitters for candidates
    state_df["State"] = state_name
    state_df = pd.merge(state_df, state_candidates_df, left_index=True, right_index=True)
    # print(state_df)
    # add the state df to the full df
    frames.append(state_df)
    # print(frames)
df = pd.concat(frames)
df.to_csv("df.csv")
# pd.read_csv('df.csv')