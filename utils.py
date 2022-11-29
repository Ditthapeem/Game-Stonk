from dbutils.pooled_db import PooledDB
import pymysql
from config import DB_NAME, DB_HOST, DB_USER, DB_PASSWD

GAME_DICT = {"game_id":[{1: "Counter-Strike: Global Offensive", 2: "Dota 2", 3: "Call of Duty: Modern Warfare II",
                        4: "PUBG: BATTLEGROUNDS", 5: "Apex Legends", 6: "Naraka: Bladepoint", 7: "Grand Theft Auto V",
                        8: "New World", 9: "Dead by Daylight", 10: "Elden Ring", 11: "Monster Hunter Rise", 12: "Project Zomboid"}]}

DICT_PLATFORM = {"twitter": "tweets", "google_trends":"interest", "twitch": "viewers", "steam":"players"}

def get_list(list_id):
    list_game_name = []
    try:
        for i in list_id:
            game_id_dict = GAME_DICT["game_id"]
            list_game_name.append(GAME_DICT["game_id"][0][int(i)])
    except:
        for i in list_id:
            list_game_name.append(i)
    return list_game_name

def try_split(list):
    try:
        return get_list(list.split(","))
    except:
        return [list]

def create_pool():
    pool = PooledDB(  creator=pymysql,
                        host=DB_HOST,
                        user=DB_USER,
                        password=DB_PASSWD,
                        database=DB_NAME,
                        maxconnections=1,
                        blocking=True)  
    return pool
