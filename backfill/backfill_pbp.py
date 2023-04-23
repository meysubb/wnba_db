import asyncio
import time
from httpx import AsyncClient
import nba_scraper.wnba_scrape_functions as ws
import pandas as pd
import json


# scrape wnba pbp
async def scrape(game_id, season):
    game_results = ws.get_wnba_pbp_api(game_id)
    await save_json(game_results, game_id, season)

# save wnba json
async def save_json(pbp_json, game_id, season):
    json_file_name = 'game_'+str(game_id) 
    with open(f'backfill_data/{season}/{json_file_name}.json', 'w') as file:
        json.dump(pbp_json, file)

async def main():
    start_time = time.time()
    # better way to do this by season?
    season = 2022
    game_ids = list(range(int(f"02{season-2000}00001"), int(f"02{season-2000}00204")))
    tasks = []
    for game in game_ids:
        o_game_id = "0"+str(game)
        task = asyncio.create_task(scrape(o_game_id, season))
        tasks.append(task)

    await asyncio.gather(*tasks)
    time_difference = time.time() - start_time
    print(f'Scraping time: %.2f seconds.' % time_difference)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())