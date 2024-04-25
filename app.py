import requests
import pandas as pd
import time
from flask import Flask
from threading import Thread
import os

app = Flask('')


@app.route('/')
def main():
    return 'This is a bot to get leaderboard from blizzard api.'


@app.route('/AP/')
def AP():
    reply = ''
    if os.path.exists('battlegrounds_AP.txt'):
        f = open('battlegrounds_AP.txt', 'r', encoding='utf-8')
        lines = f.read()
        f.close()
        newline = '\n<br />'
        lines = lines.split('\n')
        reply = newline.join(lines[1:-1])
        reply += newline
    return reply


@app.route('/AP_duo/')
def AP_duo():
    reply = ''
    if os.path.exists('battlegroundsduo_AP.txt'):
        f = open('battlegroundsduo_AP.txt', 'r', encoding='utf-8')
        lines = f.read()
        f.close()
        newline = '\n<br />'
        lines = lines.split('\n')
        reply = newline.join(lines[1:-1])
        reply += newline
    return reply


@app.route('/US/')
def US():
    reply = ''
    if os.path.exists('battlegrounds_US.txt'):
        f = open('battlegrounds_US.txt', 'r', encoding='utf-8')
        lines = f.read()
        f.close()
        newline = '\n<br />'
        lines = lines.split('\n')
        reply = newline.join(lines[1:-1])
        reply += newline
    return reply


@app.route('/US_duo/')
def US_duo():
    reply = ''
    if os.path.exists('battlegroundsduo_US.txt'):
        f = open('battlegroundsduo_US.txt', 'r', encoding='utf-8')
        lines = f.read()
        f.close()
        newline = '\n<br />'
        lines = lines.split('\n')
        reply = newline.join(lines[1:-1])
        reply += newline
    return reply


@app.route('/EU/')
def EU():
    reply = ''
    if os.path.exists('battlegrounds_EU.txt'):
        f = open('battlegrounds_EU.txt', 'r', encoding='utf-8')
        lines = f.read()
        f.close()
        newline = '\n<br />'
        lines = lines.split('\n')
        reply = newline.join(lines[1:-1])
        reply += newline
    return reply


@app.route('/EU_duo/')
def EU_duo():
    reply = ''
    if os.path.exists('battlegroundsduo_EU.txt'):
        f = open('battlegroundsduo_EU.txt', 'r', encoding='utf-8')
        lines = f.read()
        f.close()
        newline = '\n<br />'
        lines = lines.split('\n')
        reply = newline.join(lines[1:-1])
        reply += newline
    return reply


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
                try:
                    tries = tries + 1
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
        try:
            tries = tries + 1
            data = getPage(region, mode, 1)
            success = True
        except:
            time.sleep(10)
    if not success:
        return
        
    totalPages = data['leaderboard']['pagination']['totalPages']
    totalFails = 0
    rows_list = []
    threads = []

    if totalPages < 10:
        threads_num = 1
    else:
        threads_num = 10

    page_slice = totalPages // threads_num

    for i in range(threads_num):
        start_p = page_slice * i + 1
        end_p = page_slice * (i + 1)
        if i == (threads_num - 1):
            end_p = totalPages
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
        df.to_csv(f'{mode}_{region}.txt', sep=' ', index=False, encoding='utf-8')
    else:
        time.sleep(120)


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
        time.sleep(120)
