import pandas as pd


# get vote pc
def get_vote_pc(row):
    try:
        return round(row.vote / row.voters * 100, 2)
    except:
        return 0


# read data
dfv = pd.read_csv('data/votes_raw_pa.csv') # seat_no,name,vote,party,alliance,seat,district,division,region,voters,male_voters,female_voters,thirdgender_voters,time


# get seat-wise and party-wise latest vote count
idx = dfv.groupby(['seat', 'party'])['time'].idxmax()
dfvc = dfv.loc[idx]
dfvc['vote_pc'] = dfvc.apply(get_vote_pc, axis=1)

# save seat-wise and party-wise data
dfvc.to_csv('data/seat_votes_pa.csv', index=False)


# get party-wise vote_pc
total_voters = dfv.groupby('seat')['voters'].first().sum()

# get total vote pc
def get_total_vote_pc(row):
    try:
        return round(row.vote / total_voters * 100, 2)
    except:
        return 0
    
dfpc = dfvc.groupby('party').agg({'vote': 'sum', 'voters': 'sum'}).reset_index()
dfpc['avg_vote_pc'] = dfpc.apply(get_vote_pc, axis=1)
dfpc['total_vote_pc'] = dfpc.apply(get_total_vote_pc, axis=1)

# get seat-wise party summary
latest_time = dfvc.groupby('seat')['time'].max().reset_index()
df_latest = dfvc.merge(latest_time, on=['seat', 'time'])
dfps = df_latest.sort_values(['seat', 'vote'], ascending=[True, False]).drop_duplicates('seat')
dfps = dfps.groupby('party')['seat'].count().reset_index()
dfp = pd.merge(dfpc, dfps, how='left', on='party')
dfp = dfp.rename(columns={'seat': 'count'})

# save party-wise data
dfp.to_csv('data/party_votes_pa.csv', index=False)