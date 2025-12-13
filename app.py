import os
import math
import time
import logging
import aiohttp
import asyncio
import functools
import pandas as pd
from flask import Flask
from threading import Thread
from logging.handlers import RotatingFileHandler

HTTP_SEMAPHORE = asyncio.Semaphore(8)

logger = logging.getLogger('BGrankBot')
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s [%(levelname)s] %(message)s'
)

file_handler = RotatingFileHandler(
    'bgrank.log',
    maxBytes=5 * 1024 * 1024,
    backupCount=5,
    encoding='utf-8'
)

file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

app = Flask('')


@app.route('/')
def home():
    return 'Bot information: https://github.com/IBM5100o/BGrank_bot'


@app.route('/AP/')
def AP():
    return getReply('battlegrounds_AP.txt')


@app.route('/AP_duo/')
def AP_duo():
    return getReply('battlegroundsduo_AP.txt')


@app.route('/US/')
def US():
    return getReply('battlegrounds_US.txt')


@app.route('/US_duo/')
def US_duo():
    return getReply('battlegroundsduo_US.txt')


@app.route('/EU/')
def EU():
    return getReply('battlegrounds_EU.txt')


@app.route('/EU_duo/')
def EU_duo():
    return getReply('battlegroundsduo_EU.txt')


@app.route('/CN/')
def CN():
    return getReply('battlegrounds_CN.txt')


@app.route('/CN_duo/')
def CN_duo():
    return getReply('battlegroundsduo_CN.txt')


def getReply(fileName):
    reply = ''
    try:
        if os.path.exists(fileName):
            with open(fileName, 'r', encoding='utf-8') as f:
                reply = f.read()
    except Exception:
        logger.exception(f'Read {fileName} fail')
    return reply


def async_retry(
    tries=3,
    delay=2,
    backoff=2,
    exceptions=(Exception,),
    logger=None
):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if logger:
                        logger.warning(
                            f'{func.__name__} failed: {e}, retry in {_delay}s'
                        )
                    await asyncio.sleep(_delay)
                    _tries -= 1
                    _delay *= backoff
            return await func(*args, **kwargs)
        return wrapper
    return decorator


@async_retry(tries=3, delay=3, logger=logger)
async def getPage_async(session, region, leaderboardId, pageNumber):
    url = (
        'https://hearthstone.blizzard.com/en-us/api/community/leaderboardsData'
        f'?region={region}&leaderboardId={leaderboardId}&page={pageNumber}'
    )
    # logger.info(f'Async GetPage {region} {leaderboardId} page={pageNumber}')

    async with HTTP_SEMAPHORE:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                raise Exception(f'HTTP {resp.status}')


@async_retry(tries=3, delay=3, logger=logger)
async def getPage_CN_async(session, page, mode, seasonId):
    url = (
        'https://webapi.blizzard.cn/hs-rank-api-server/api/game/ranks'
        f'?page={page}&mode_name={mode}&season_id={seasonId}'
    )
    # logger.info(f'Async GetPage_CN page={page} mode={mode}')

    async with HTTP_SEMAPHORE:
        async with session.get(url, timeout=10) as resp:
            if resp.status != 200:
                raise Exception(f'HTTP {resp.status}')

            data = await resp.json()
            code = data['code']
            if code != 0:
                raise Exception(f'API code {code}')

            return data


async def getLeaderBoard_async(region, mode):
    # logger.info(f'Async update leaderboard: {region} {mode}')

    async with aiohttp.ClientSession(
        headers={'User-Agent': 'Mozilla/5.0'}
    ) as session:

        first = await getPage_async(session, region, mode, 1)
        totalPages = first['leaderboard']['pagination']['totalPages']
        rows = first['leaderboard']['rows']

        tasks = []
        for page in range(2, totalPages + 1):
            tasks.append(
                getPage_async(session, region, mode, page)
            )

        results = await asyncio.gather(*tasks)

        for r in results:
            rows.extend(r['leaderboard']['rows'])

    df = pd.DataFrame(rows)
    if 'rank' in df.columns:
        del df['rank']
    else:
        logger.warning('Column rank not found')
        return

    lines = df.to_csv(
        sep=' ',
        header=False,
        index=False,
        encoding='utf-8'
    ).replace('\n', '\n<br />')

    with open(f'{mode}_{region}.txt', 'w', encoding='utf-8') as f:
        f.write(lines)

    # logger.info(f'Async update success: {region} {mode}')


async def getLeaderBoard_CN_async(mode):
    # logger.info(f'Async update CN leaderboard: {mode}')

    async with aiohttp.ClientSession(
        headers={'User-Agent': 'Mozilla/5.0'}
    ) as session:

        first = await getPage_async(session, 'AP', 'battlegrounds', 1)
        seasonId = first['seasonId']

        data = await getPage_CN_async(session, 1, mode, seasonId)
        total = data['data']['total']
        totalPages = math.ceil(total / 25.0)
        rows = data['data']['list']

        tasks = []
        for page in range(2, totalPages + 1):
            tasks.append(
                getPage_CN_async(session, page, mode, seasonId)
            )

        results = await asyncio.gather(*tasks)

        for r in results:
            rows.extend(r['data']['list'])

    df = pd.DataFrame(rows)
    if 'position' in df.columns:
        del df['position']
    else:
        logger.warning('Column position not found')
        return

    lines = df.to_csv(
        sep=' ',
        header=False,
        index=False,
        encoding='utf-8'
    ).replace('\n', '\n<br />')

    with open(f'{mode}_CN.txt', 'w', encoding='utf-8') as f:
        f.write(lines)

    # logger.info(f'Async CN update success: {mode}')


async def safe_task(coro, name):
    try:
        await coro
        # logger.info(f'{name} success')
    except Exception:
        logger.exception(f'{name} failed')


async def async_update_all():
    await asyncio.gather(
        safe_task(getLeaderBoard_async('AP', 'battlegrounds'), 'AP BG'),
        safe_task(getLeaderBoard_async('AP', 'battlegroundsduo'), 'AP BG Duo'),
        safe_task(getLeaderBoard_async('US', 'battlegrounds'), 'US BG'),
        safe_task(getLeaderBoard_async('US', 'battlegroundsduo'), 'US BG Duo'),
        safe_task(getLeaderBoard_async('EU', 'battlegrounds'), 'EU BG'),
        safe_task(getLeaderBoard_async('EU', 'battlegroundsduo'), 'EU BG Duo'),
        safe_task(getLeaderBoard_CN_async('battlegrounds'), 'CN BG'),
        safe_task(getLeaderBoard_CN_async('battlegroundsduo'), 'CN BG Duo'),
    )


def runFlask():
    app.run(host='0.0.0.0', port=8080)


server = Thread(target=runFlask, daemon=True)
server.start()

while True:
    # logger.info('===== Async global update start =====')
    try:
        asyncio.run(async_update_all())
    except Exception:
        logger.exception('Async update failed (unexpected)')

    # logger.info('===== Async update finished, sleep 300s =====')
    time.sleep(300)
