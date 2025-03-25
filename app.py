import requests
import pandas as pd
import time
from flask import Flask
from threading import Thread

app = Flask('')
replyAP = ''
replyAP_duo = ''
replyUS = ''
replyUS_duo = ''
replyEU = ''
replyEU_duo = ''


@app.route('/')
def main():
    return 'This is a bot to get the leaderboard from Blizzard API.'


@app.route('/AP/')
def AP():
    return replyAP


@app.route('/AP_duo/')
def AP_duo():
    return replyAP_duo


@app.route('/US/')
def US():
    return replyUS


@app.route('/US_duo/')
def US_duo():
    return replyUS_duo


@app.route('/EU/')
def EU():
    return replyEU


@app.route('/EU_duo/')
def EU_duo():
    return replyEU_duo


class MyThread(Thread):
    def __init__(self, start_page, end_page, region, mode):
        Thread.__init__(self)
        self.start_page = start_page
        self.end_page = end_page
        self.region = region
        self.mode = mode
        self.row_list = []
        self.fails = 0

    def run(self):
        for i in range(self.start_page, self.end_page + 1):
            tries = 0
            success = False
            while tries < 3 and (not success):
                tries = tries + 1
                try:
                    page_data = getPage(self.region, self.mode, i)
                    self.row_list = self.row_list + page_data['leaderboard']['rows']
                    # print(f'{self.mode}_{self.region} Page {i} read success!')
                    success = True
                    time.sleep(1)
                except:
                    # print(f'Error: {self.mode}_{self.region} Page {i} waiting 10 seconds to try again')
                    time.sleep(10)
            if not success:
                self.fails = self.fails + 1
                if self.fails >= 3:
                    break


def getPage(region, leaderboardId, pageNumber):
    url = f'https://hearthstone.blizzard.com/en-us/api/community/leaderboardsData?region={region}&leaderboardId={leaderboardId}&page={pageNumber}'
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    if response.status_code == 200:
        page_data = response.json()
        return page_data
    else:
        error = f'Error {response.status_code} - {response.reason}'
        # print(error)
        raise Exception(error)


def getLeaderBoard(region, mode):
    tries = 0
    success = False
    while tries < 3 and (not success):
        tries = tries + 1
        try:
            data = getPage(region, mode, 1)
            success = True
            time.sleep(1)
        except:
            time.sleep(10)
    if not success:
        return
        
    totalPages = data['leaderboard']['pagination']['totalPages']
    totalFails = 0
    rows_list = data['leaderboard']['rows']
    threads = []

    if totalPages > 1:
        if totalPages < 10:
            threads_num = 1
        else:
            threads_num = 10
        page_slice = totalPages // threads_num

        for i in range(threads_num):
            if i == 0:
                start_p = 2
            else:
                start_p = page_slice * i + 1
            if i == (threads_num - 1):
                end_p = totalPages
            else:
                end_p = page_slice * (i + 1)
            threads.append(MyThread(start_p, end_p, region, mode))
            threads[i].start()

        for i in range(threads_num):
            threads[i].join()
            rows_list = rows_list + threads[i].row_list
            totalFails = totalFails + threads[i].fails

    if totalFails < 3:
        df = pd.DataFrame(rows_list)
        try:
            del df['rank']
        except:
            return
        lines = df.to_csv(sep=' ', header=False, index=False).replace('\n', '\n<br />')
        if region == 'AP':
            if mode == 'battlegrounds':
                global replyAP
                replyAP = lines
            else:
                global replyAP_duo
                replyAP_duo = lines
        elif region == 'US':
            if mode == 'battlegrounds':
                global replyUS
                replyUS = lines
            else:
                global replyUS_duo
                replyUS_duo = lines
        else:
            if mode == 'battlegrounds':
                global replyEU
                replyEU = lines
            else:
                global replyEU_duo
                replyEU_duo = lines
    else:
        time.sleep(300)


def run():
    app.run(host="0.0.0.0", port=8080)


if __name__ == '__main__':
    server = Thread(target=run)
    server.start()
    while True:
        getLeaderBoard('AP', 'battlegrounds')
        getLeaderBoard('AP', 'battlegroundsduo')
        getLeaderBoard('US', 'battlegrounds')
        getLeaderBoard('US', 'battlegroundsduo')
        getLeaderBoard('EU', 'battlegrounds')
        getLeaderBoard('EU', 'battlegroundsduo')
        time.sleep(300)
