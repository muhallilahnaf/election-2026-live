import requests
from bs4 import BeautifulSoup as bs
import re
import pandas as pd
from time import sleep


dfraw = pd.read_csv('data/votes_raw.csv')

# collect data from api
dfd = pd.read_csv('data/dt_seats.csv')
rstore = []
count = 5
for i, row in dfd.iterrows():
    res = requests.get(f'https://election.dhakatribune.com/get-candidate/{row.seat_no}')
    # print(row.seat_name, res.status_code)
    if res.status_code == 200:
        data = res.json()
        for candidate in data:
            rstore.append(candidate)
    count = count - 1
    if count == 0:
        # print('sleeping')
        sleep(2)
        count = 5

dfr = pd.DataFrame(rstore)

# map party name
party_mapping = {
    '38': 'BNP',
    '36': 'Jamaat',
    '66': 'AB Party',
    '37': 'Islami Andolan Bangladesh',
    '5': 'JaPa',
    '47': 'CPB',
    '90': 'Ganosamhati Andolon (Saki)',
    '63': 'NCP',
    '50': 'Bangladesh Khelafat Majlish',
    '115': 'Gano Forum',
    '99': 'Jamiat Ulema-e-Islam Bangladesh',
    '45': 'Khelafat Majlish',
    '65': 'BJP'
}

def map_party(x):
    x = str(x)
    if x in party_mapping.keys():
        return party_mapping[x]
    return None

dfr['party'] = dfr.party_id.apply(map_party)

# drop less important parties
dfr = dfr.dropna(subset=['party'])

# merge to get seat name
df = pd.merge(dfr, dfd, how='left', left_on='seat_id', right_on='seat_no')
df = df[['seat_name', 'party', 'name', 'vote', 'center', 'division_id', 'total_center']]

# map seat name
seat_mapping = {
    'Chapainawabganj': 'Chapai Nawabganj',
    'Jhalakathi': 'Jhalokati'
}

def map_seat(x):
    name, number = x.split('-')
    if name in seat_mapping.keys():
        return seat_mapping[name] + '-' + number
    return x

df['seat_name'] = df.seat_name.apply(map_seat)

# add timestamp and source
df['time'] = pd.to_datetime('now', utc=True)
df['source'] = 'dt'

# append to votes_raw.csv
dfnew = pd.concat([dfraw, df], ignore_index=True)

# save
dfnew.to_csv('data/votes_raw.csv', index=False)