import requests
import pandas as pd

dfraw = pd.read_csv('data/votes_raw_pa.csv')

# fetch data
res = requests.get('https://election.prothomalo.com/election-result-ajax')
if res.status_code == 200:
    data = res.json()
    try:

        # extract data
        seats = data['resultData']['win_nearest_candidates']
        store = []
        for seat_no, seat in seats.items():
            if 'nearest' in seat:
                nearest = seat['nearest']
                store.append((
                    nearest['area_name'],
                    nearest['area_no'],
                    nearest['candidates_name'],
                    nearest['party'],
                    nearest['vote_cast'],
                    nearest['jote']
                ))
            if 'win' in seat:
                win = seat['win']
                store.append((
                    win['area_name'],
                    win['area_no'],
                    win['candidates_name'],
                    win['party'],
                    win['vote_cast'],
                    win['jote']
                ))

        # create datafrane
        df = pd.DataFrame(store, columns=['seat_bn', 'seat_no', 'name', 'party_bn', 'vote', 'jote'])
        
        # map party names
        party_mapping = {
            'bkm': 'Bangladesh Khelafat Mojlish',
            'bnp': 'BNP',
            'gonoa': 'Ganosamhati Andolon (Saki)',
            'iab': 'Islami Andolan Bangladesh',
            'juib': 'Jamiat Ulema-e-Islam Bangladesh',
            'km': 'Khelafat Mojlish',
            'ncp': 'NCP',
            'sontontro': 'Independent',
            'bjai': 'Jamaat',
            'abp': 'AB Party',
            'bifront': 'Bangladesh Islami Front',
            'gop': 'GOP (Nuru)',
            'bijp': 'BJP',
            'bbwp': 'Revolutionary Workers Party of Bangladesh',
            'bdp': 'Bangladesh Development Party',
            'ldp':'LDP',
        }
        def map_party(x):
            if x in party_mapping.keys():
                return party_mapping[x]
            return x
        df['party'] = df.party_bn.apply(map_party)

        # map alliance names
        alliance_mapping = {
            'bkm': 'Jamaat-NCP',
            'bnp': 'BNP',
            'gonoa': 'BNP',
            'iab': 'no alliance',
            'juib': 'BNP',
            'km': 'Jamaat-NCP',
            'ncp': 'Jamaat-NCP',
            'sontontro': 'no alliance',
            'bjai': 'Jamaat-NCP',
            'abp': 'Jamaat-NCP',
            'bifront': 'no alliance',
            'gop': 'BNP',
            'bijp': 'BNP',
            'bbwp': 'BNP',
            'bdp': 'Jamaat-NCP',
            'ldp':'Jamaat-NCP',
        }
        def map_alliance(x):
            if x in alliance_mapping.keys():
                return alliance_mapping[x]
            return 'no alliance'
        df['alliance'] = df.party_bn.apply(map_alliance)

        # merge with seat data to get seat name and voter numbers
        dfs = pd.read_csv('data/seat_voters_2026.csv')
        dfm = pd.merge(df, dfs, how='left', on='seat_no')

        # drop redundant columns
        dfm = dfm.drop(columns=['seat_bn', 'party_bn', 'jote'])

        # add timestamp
        dfm['time'] = pd.Timestamp.utcnow()

        # append to votes_raw_pa.csv
        dfnew = pd.concat([dfraw, dfm], ignore_index=True)
        # save
        dfnew.to_csv('data/votes_raw_pa.csv', index=False)
    except Exception as e:
        print(e)
