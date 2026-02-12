import requests
import pandas as pd

dfraw = pd.read_csv('data/seat_votes_pa.csv')

res = requests.get('https://election.prothomalo.com/election-result-ajax')
if res.status_code == 200:
    data = res.json()
    try:
        seats = data['resultData']['win_nearest_candidates']
        store = []
        for seat_no, seat in seats.items():
            if 'nearest' in seat:
                nearest = seat['nearest']
                store.append((
                    nearest['area_name'],
                    nearest['candidates_name'],
                    nearest['party'],
                    nearest['vote_cast'],
                    nearest['jote']
                ))
            if 'win' in seat:
                win = seat['win']
                store.append((
                    win['area_name'],
                    win['candidates_name'],
                    win['party'],
                    win['vote_cast'],
                    win['jote']
                ))
        df = pd.DataFrame(store, columns=['seat', 'name', 'party_bn', 'vote', 'jote'])
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
        }
        def map_party(x):
            if x in party_mapping.keys():
                return party_mapping[x]
            return x
        df['party'] = df.party_bn.apply(map_party)
        df = df[['seat', 'name', 'party', 'vote']]
        df['time'] = pd.Timestamp.utcnow()
        df['source'] = 'pa'
        # append to seat_votes_pa.csv
        dfnew = pd.concat([dfraw, df], ignore_index=True)
        # save
        dfnew.to_csv('data/seat_votes_pa.csv', index=False)
    except Exception as e:
        print(e)
