import pandas as pd


# get vote pc
def get_vote_pc(row):
    try:
        return round(row.votes / row.voters * 100, 2)
    except:
        return 0


# read data
dfv = pd.read_csv('/data/votes_raw.csv')
dfs = pd.read_csv('data/seat_voters_2026.csv')


# get seat-wise and party-wise latest vote count
dfvc = dfv.groupby(['seat', 'party']).agg({'time': 'max', 'votes': 'first'}).reset_index()
dfvpc = pd.merge(dfvc, dfs, how='left', on='seat')
dfvpc['vote_pc'] = dfvpc.apply(get_vote_pc, axis=1)

# save seat-wise and party-wise data
dfvpc.to_csv('data/seat_votes.csv', index=False)


# get party-wise summary
dfpc = dfvc.groupby('party').agg({'time': 'max', 'votes': 'sum', 'voters': 'sum'}).reset_index()
dfpc['vote_pc'] = dfpc.apply(get_vote_pc, axis=1)

# get seat-wise party summary
dfps = dfvc.groupby('seat').agg({'time': 'max', 'votes': 'max', 'party': 'first'}).reset_index()
dfps = dfps.groupby('party')['seat'].count().reset_index()
dfp = pd.merge(dfpc, dfps, how='left', on='party')

# save party-wise data
dfp.to_csv('data/party_votes.csv', index=False)