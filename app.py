from flask import Flask
from flask_restful import Api, Resource
from utils import DICT_PLATFORM, GAME_DICT, create_pool, try_split

app = Flask(__name__)
api = Api(app)

@app.route('/game', methods=['GET'])
def get_game():
    return GAME_DICT

# Google trend
@app.route('/google/hourly/<id>', methods=['GET'])
def get_google_hourly(id):
    list_game_name = try_split(id)
    pool = create_pool()
    result_dict = {}
    for game in list_game_name:
        with pool.connection() as conn, conn.cursor() as cs:
            cs.execute(f"""
                SELECT interest, timestamp
                FROM google_trends
                WHERE game = "{game}"
            """)
            result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {"interest": i[0],
                        "date": i[1]}
                temp_result.append(temp)
            result_dict.update({f"{game}": temp_result})
    return {"game" : list_game_name,
            "date_type": "Hourly interest",
            "result": result_dict}

@app.route('/google/daily/<id>', methods=['GET'])
def get_google_daily(id):
    list_game_name = try_split(id)
    pool = create_pool()
    result_dict = {}
    for game in list_game_name:
        with pool.connection() as conn, conn.cursor() as cs:
            cs.execute(f"""
                SELECT CAST(timestamp AS DATE) AS date, MAX(interest) AS interest
                FROM google_trends
                WHERE game = "{game}"
                GROUP BY date
            """)
            result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {"interest": i[1],
                        "date": i[0]}
                temp_result.append(temp)
            result_dict.update({f"{game}": temp_result})
    return {"game" : list_game_name,
            "date_type": "Peak Daily interest",
            "result": result_dict}

# Steam
@app.route('/steam/hourly/<id>', methods=['GET'])
def get_steam_hourly(id):
    list_game_name = try_split(id)
    pool = create_pool()
    result_dict = {}
    for game in list_game_name:
        with pool.connection() as conn, conn.cursor() as cs:
            cs.execute(f"""
                SELECT players, timestamp
                FROM steam
                WHERE game = "{game}"
            """)
            result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {"players": i[0],
                        "date": i[1]}
                temp_result.append(temp)
            result_dict.update({f"{game}": temp_result})
    return {"game" : list_game_name,
            "date_type": "Hourly Players",
            "result": result_dict}

@app.route('/steam/daily/<id>', methods=['GET'])
def get_steam_daily(id):
    list_game_name = try_split(id)
    pool = create_pool()
    result_dict = {}
    for game in list_game_name:
        with pool.connection() as conn, conn.cursor() as cs:
            cs.execute(f"""
                SELECT CAST(timestamp AS DATE) AS date, MAX(players) AS players
                FROM steam
                WHERE game = "{game}"
                GROUP BY date
            """)
            result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {"players": i[1],
                        "date": i[0]}
                temp_result.append(temp)
            result_dict.update({f"{game}": temp_result})
    return {"game" : list_game_name,
            "date_type": "Peak Daily Players",
            "result": result_dict}

# Twitch
@app.route('/twitch/hourly/<id>', methods=['GET'])
def get_twitch_hourly(id):
    list_game_name = try_split(id)
    pool = create_pool()
    result_dict = {}
    for game in list_game_name:
        with pool.connection() as conn, conn.cursor() as cs:
            cs.execute(f"""
                SELECT viewers, timestamp
                FROM twitch
                WHERE game = "{game}"
            """)
            result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {"viewers": i[0],
                        "date": i[1]}
                temp_result.append(temp)
            result_dict.update({f"{game}": temp_result})
    return {"game" : list_game_name,
            "date_type": "Hourly viewers",
            "result": result_dict}

@app.route('/twitch/daily/<id>', methods=['GET'])
def get_twitch_daily(id):
    list_game_name = try_split(id)
    pool = create_pool()
    result_dict = {}
    for game in list_game_name:
        with pool.connection() as conn, conn.cursor() as cs:
            cs.execute(f"""
                SELECT CAST(timestamp AS DATE) AS date, MAX(viewers) AS viewers
                FROM twitch
                WHERE game = "{game}"
                GROUP BY date
            """)
            result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {"viewers": i[1],
                        "date": i[0]}
                temp_result.append(temp)
            result_dict.update({f"{game}": temp_result})
    return {"game" : list_game_name,
            "date_type": "Peak Daily viewers",
            "result": result_dict}

# twitter
@app.route('/twitter/hourly/<id>', methods=['GET'])
def get_twitter_hourly(id):
    list_game_name = try_split(id)
    pool = create_pool()
    result_dict = {}
    for game in list_game_name:
        with pool.connection() as conn, conn.cursor() as cs:
            cs.execute(f"""
                SELECT tweets, timestamp
                FROM twitter
                WHERE game = "{game}"
            """)
            result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {"tweets": i[0],
                        "date": i[1]}
                temp_result.append(temp)
            result_dict.update({f"{game}": temp_result})
    return {"game" : list_game_name,
            "date_type": "Hourly tweets",
            "result": result_dict}

@app.route('/twitter/daily/<id>', methods=['GET'])
def get_twitter_daily(id):
    list_game_name = try_split(id)
    pool = create_pool()
    result_dict = {}
    for game in list_game_name:
        with pool.connection() as conn, conn.cursor() as cs:
            cs.execute(f"""
                SELECT CAST(timestamp AS DATE) AS date, MAX(tweets) AS tweets
                FROM twitter
                WHERE game = "{game}" 
                GROUP BY date
            """)
            result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {"tweets": i[1],
                        "date": i[0]}
                temp_result.append(temp)
            result_dict.update({f"{game}": temp_result})
    return {"game" : list_game_name,
            "date_type": "Peak Daily tweets",
            "result": result_dict}

@app.route('/relevent/hourly/<platform>/<start_date>/<end_date>/<id>', methods=['GET'])
def get_relevent_hourly(platform, start_date, end_date, id):
    list_game_name = try_split(id)
    list_platform = try_split(platform)
    pool = create_pool()
    result_dict = {}
    list_result = {}
    for platform in list_platform:
        temp_dict = {}
        for game in list_game_name:
            with pool.connection() as conn, conn.cursor() as cs:
                cs.execute(f"""
                    SELECT {DICT_PLATFORM[platform]}, timestamp
                    FROM {platform}
                    WHERE game = "{game}" BETWEEN '{start_date}' AND '{end_date}'
                """)
                result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {f"{DICT_PLATFORM[platform]}": i[0],
                        "date": i[1]}
                temp_result.append(temp)
            list_result.update({f"{game}": temp_result})
        result_dict.update(({f"{platform}": list_result}))      
    return {"game" : list_game_name,
            "platform": list_platform,
            "date_type": "Hourly",
            "result": result_dict}

@app.route('/relevent/daily/<platform>/<start_date>/<end_date>/<id>', methods=['GET'])
def get_relevent_daily(platform, start_date, end_date, id):
    list_game_name = try_split(id)
    list_platform = try_split(platform)
    pool = create_pool()
    result_dict = {}
    list_result = {}
    for platform in list_platform:
        temp_dict = {}
        for game in list_game_name:
            with pool.connection() as conn, conn.cursor() as cs:
                cs.execute(f"""
                    SELECT CAST(timestamp AS DATE) AS date, MAX({DICT_PLATFORM[platform]}) AS {DICT_PLATFORM[platform]}
                    FROM {platform}
                    WHERE game = "{game}" BETWEEN '{start_date}' AND '{end_date}'
                    GROUP BY date
                """)
                result = cs.fetchall()
            temp_result = []
            for i in result:
                temp = {f"{DICT_PLATFORM[platform]}": i[1],
                        "date": i[0]}
                temp_result.append(temp)
            list_result.update({f"{game}": temp_result})
        result_dict.update(({f"{platform}": list_result}))
    return {"game" : list_game_name,
            "platform": list_platform,
            "date_type": "Daily",
            "result": result_dict}

if __name__ == "__main__":
    app.run(debug=True)