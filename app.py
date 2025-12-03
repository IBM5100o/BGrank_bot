import requests
import pandas as pd
import time
from flask import Flask
from threading import Thread
import os
import math

app = Flask('')


@app.route('/')
def main():
    return 'This is a bot to get the leaderboard from Blizzard API.'


@app.route('/AP/')
def AP():
    reply = ''
    if os.path.exists('battlegrounds_AP.txt'):
        f = open('battlegrounds_AP.txt', 'r', encoding='utf-8')
        reply = f.read()
        f.close()
    return reply


@app.route('/AP_duo/')
def AP_duo():
    reply = ''
    if os.path.exists('battlegroundsduo_AP.txt'):
        f = open('battlegroundsduo_AP.txt', 'r', encoding='utf-8')
        reply = f.read()
        f.close()
    return reply


@app.route('/US/')
def US():
    reply = ''
    if os.path.exists('battlegrounds_US.txt'):
        f = open('battlegrounds_US.txt', 'r', encoding='utf-8')
        reply = f.read()
        f.close()
    return reply


@app.route('/US_duo/')
def US_duo():
    reply = ''
    if os.path.exists('battlegroundsduo_US.txt'):
        f = open('battlegroundsduo_US.txt', 'r', encoding='utf-8')
        reply = f.read()
        f.close()
    return reply


@app.route('/EU/')
def EU():
    reply = ''
    if os.path.exists('battlegrounds_EU.txt'):
        f = open('battlegrounds_EU.txt', 'r', encoding='utf-8')
        reply = f.read()
        f.close()
    return reply


@app.route('/EU_duo/')
def EU_duo():
    reply = ''
    if os.path.exists('battlegroundsduo_EU.txt'):
        f = open('battlegroundsduo_EU.txt', 'r', encoding='utf-8')
        reply = f.read()
        f.close()
    return reply


@app.route('/CN/')
def CN():
    reply = ''
    if os.path.exists('battlegrounds_CN.txt'):
        f = open('battlegrounds_CN.txt', 'r', encoding='utf-8')
        reply = f.read()
        f.close()
    return reply


@app.route('/CN_duo/')
def CN_duo():
    reply = ''
    if os.path.exists('battlegroundsduo_CN.txt'):
        f = open('battlegroundsduo_CN.txt', 'r', encoding='utf-8')
        reply = f.read()
        f.close()
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
    totalPages = 0
    totalFails = 0
    rows_list = []
    threads = []

    tries = 0
    success = False
    while tries < 3 and (not success):
        tries = tries + 1
        try:
            data = getPage(region, mode, 1)
            totalPages = data['leaderboard']['pagination']['totalPages']
            rows_list = data['leaderboard']['rows']
            success = True
            time.sleep(1)
        except:
            time.sleep(10)
    if not success:
        return

    if totalPages > 1:
        if totalPages < 20:
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
        try:
            df = pd.DataFrame(rows_list)
            del df['rank']
        except:
            return
        lines = df.to_csv(sep=' ', header=False, index=False, encoding='utf-8').replace('\n', '\n<br />')
        f = open(f'{mode}_{region}.txt', 'w', encoding='utf-8')
        f.write(lines)
        f.close()
    else:
        time.sleep(300)


def getPage_CN(page, mode, seasonId):
    url = f'https://webapi.blizzard.cn/hs-rank-api-server/api/game/ranks?page={page}&mode_name={mode}&season_id={seasonId}'
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    if response.status_code == 200:
        page_data = response.json()
        return page_data
    else:
        error = f'Error {response.status_code} - {response.reason}'
        # print(error)
        raise Exception(error)


def getLeaderBoard_CN(mode):
    seasonId = 0
    totalPages = 0
    rows_list = []

    tries = 0
    success = False
    while tries < 3 and (not success):
        tries = tries + 1
        try:
            data = getPage('AP', 'battlegrounds', 1)
            seasonId = data['seasonId']
            success = True
            time.sleep(1)
        except:
            time.sleep(10)
    if not success:
        return

    tries = 0
    success = False
    while tries < 3 and (not success):
        tries = tries + 1
        try:
            data = getPage_CN(1, mode, seasonId)
            total = data['data']['total']
            totalPages = math.ceil(total / 25.0)
            rows_list = data['data']['list']
            success = True
            time.sleep(1)
        except:
            time.sleep(10)
    if not success:
        return

    for i in range(2, totalPages+1):
        tries = 0
        success = False
        while tries < 3 and (not success):
            tries = tries + 1
            try:
                data = getPage_CN(i, mode, seasonId)
                rows_list = rows_list + data['data']['list']
                success = True
                time.sleep(1)
            except:
                time.sleep(10)
        if not success:
            return

    try:
        df = pd.DataFrame(rows_list)
        del df['position']
    except:
        return
    lines = df.to_csv(sep=' ', header=False, index=False, encoding='utf-8').replace('\n', '\n<br />')
    f = open(f'{mode}_CN.txt', 'w', encoding='utf-8')
    f.write(lines)
    f.close()


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
        getLeaderBoard_CN('battlegrounds')
        getLeaderBoard_CN('battlegroundsduo')
        time.sleep(300)
