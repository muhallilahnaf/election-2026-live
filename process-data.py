import pandas as pd


# get vote pc
def get_vote_pc(row):
    try:
        return round(row.vote / row.voters * 100, 2)
    except:
        return 0


# read data
dfv = pd.read_csv('data/votes_raw.csv') # seat_name,party,name,vote,center,division_id,total_center,time,source
dfs = pd.read_csv('data/seat_voters_2026.csv') # seat,voters,male_voters,female_voters,thirdgender_voters


# get seat-wise and party-wise latest vote count
idx = dfv.groupby(['seat_name', 'party'])['time'].idxmax()
dfvc = dfv.loc[idx]
dfvpc = pd.merge(dfvc, dfs, how='left', left_on='seat_name', right_on='seat')
dfvpc['vote_pc'] = dfvpc.apply(get_vote_pc, axis=1)
dfvpc = dfvpc.drop(columns=['seat'])

# save seat-wise and party-wise data
dfvpc.to_csv('data/seat_votes.csv', index=False)


# get party-wise summary
dfpc = dfvpc.groupby('party').agg({'time': 'max', 'vote': 'sum', 'voters': 'sum'}).reset_index()
dfpc['vote_pc'] = dfpc.apply(get_vote_pc, axis=1)

# get seat-wise party summary
latest_time = dfvpc.groupby('seat_name')['time'].max().reset_index()
df_latest = dfvpc.merge(latest_time, on=['seat_name', 'time'])
dfps = df_latest.sort_values(['seat_name', 'vote'], ascending=[True, False]).drop_duplicates('seat_name')
dfps = dfps.groupby('party')['seat_name'].count().reset_index()
dfp = pd.merge(dfpc, dfps, how='left', on='party')
dfp = dfp.rename(columns={'seat_name': 'count'})

# save party-wise data
dfp.to_csv('data/party_votes.csv', index=False)