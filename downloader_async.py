import io
import os
import requests
import queue
from threading import Thread
import pandas as pd

url_temp = 'http://pems.dot.ca.gov/?report_form=1&dnode=VDS&content=detector_health&tab=dh_raw&export=text&station_id' \
           '={}&s_time_id={}&e_time_id={}&q={}&gn=sec'

DAY_SECONDS = 86400  # seconds in a day

THREADS = 30  # threads

DAYS = 181


# removes redundant headers
def rem_header(x):
    h, t = x.split('\n', 1)
    return t


results = [''] * 181
q = queue.Queue(200)


def thread_work():
    with requests.Session() as ses:
        ses.post('http://pems.dot.ca.gov/',
                 data={'username': '<Redacted!>', 'password': '<Redacted!>', 'login': 'Login'})

        print('session logged in')

        while True:
            day, query = q.get()
            stime = 1483228800 + day * DAY_SECONDS
            etime = 1483314900 + day * DAY_SECONDS
            full_url = url_temp.format(stationID, stime, etime, query)
            response = None

            while response is None:
                try:
                    response = ses.get(full_url, timeout=20)
                except Exception as e:
                    print(e)
                    print('Retrying')

            print('received', day)

            assert response.status_code == 200

            c_text = response.text

            if day != 0:
                c_text = rem_header(c_text)

            results[day] = c_text

            q.task_done()


for i in range(THREADS):
    t = Thread(target=thread_work)
    t.daemon = True
    t.start()


def parallel_execute(stationID, query):
    print('parallel_execute', stationID, query)

    try:
        for i in range(DAYS):
            q.put((i, query))
        q.join()
    except KeyboardInterrupt:
        exit(1)

    full_txt = io.StringIO()

    for i in range(DAYS):
        full_txt.write(results[i])

    full_txt.seek(0)
    return full_txt


os.makedirs('output', exist_ok=True)

if not os.path.exists('prog_state'):
    with open('prog_state', 'w') as f:
        f.write(str(-1))

df = pd.read_csv('stationReport', sep=',')

with open('prog_state', 'r') as f:
    last_fin = int(f.read())

for i in range(len(df.index)):
    if i <= last_fin:
        continue

    stationID = df['ID'][i]

    print('Downloading ' + str(stationID))
    stream1 = parallel_execute(stationID, 'occ')
    stream2 = parallel_execute(stationID, 'gspeed&q2=flow')

    # phase 2: clean up data
    print('Processing Data')

    df1 = pd.read_csv(stream1, delimiter='\t')
    df2 = pd.read_csv(stream2, delimiter='\t')

    assert (len(df1.index) == len(df2.index))

    df2.drop(columns=['Sample Time'], inplace=True)

    dfAll = pd.concat((df1, df2), axis=1)
    dfAll.to_csv('output/Station' + str(stationID) + '.csv', index=False)
    last_fin = i
    with open('prog_state', 'w') as f:
        f.write(str(last_fin))

print('All Done!')
