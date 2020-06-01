# 956448725@qq.com
# 73*Hogreen

import requests
import pandas as pd

url_temp = 'http://pems.dot.ca.gov/?report_form=1&dnode=VDS&content=detector_health&tab=dh_raw&export=text&station_id' \
           '={}&s_time_id={}&e_time_id={}&q={}&gn=sec'

day = 86400  # seconds in a day

# sign in and get cookie
ses = requests.Session()
ses.post('http://pems.dot.ca.gov/', data={'username': '<Redacted!>', 'password': '<Redacted!>', 'login': 'Login'})


# removes redundant headers
def rem_header(x):
    h, t = x.split('\n', 1)
    return t


def execute(stationID, query, days=181):
    global ses
    # stores concatenated results
    full_txt = ''

    stime = 1483228800
    etime = 1483314900

    # 181 days from 1/1/2017 to 6/30/2017
    for i in range(days):
        full_url = url_temp.format(stationID, stime, etime, query)
        print(full_url)

        print('requesting ' + str(i))
        response = ses.get(full_url)
        print('received')

        assert response.status_code == 200

        c_text = response.text

        if i != 0:
            c_text = rem_header(c_text)

        full_txt += c_text

        stime += day
        etime += day

    filename = 'S' + str(stationID) + 'Q' + query
    with open('files/' + filename, 'w') as f:
        f.write(full_txt)


df = pd.read_csv('stationReport', delimiter='\t')

for stationID in df['ID']:
    print('Downloading ' + str(stationID))
    execute(stationID, 'occ')
    execute(stationID, 'gspeed&q2=flow')
    # phase 2: clean up data

    df1 = pd.read_csv('files/S' + str(stationID) + 'Qocc', delimiter='\t')
    df2 = pd.read_csv('files/S' + str(stationID) + 'Qgspeed&q2=flow', delimiter='\t')

    assert (len(df1.index) == len(df2.index))

    df2.drop(columns=['Sample Time'], inplace=True)

    dfAll = pd.concat((df1, df2), axis=1)
    dfAll.to_csv('output/S' + str(stationID) + 'All.csv', index=False)
