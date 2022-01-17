# app.py

# Required imports
import os
from flask import Flask, request, jsonify
from firebase_admin import credentials, firestore, initialize_app
import pandas as pd
import json
from datetime import datetime, timedelta
# Initialize Flask app
app = Flask(__name__)

# Initialize Firestore DB
cred = credentials.Certificate('secrets.json')
default_app = initialize_app(cred)
db = firestore.client()
todo_ref = db.collection('activity')
activity_ref = db.collection('activity')
currentDay = datetime.now().day
currentMonth = datetime.now().month

def filterByTime(time):
    #dataframe is a pandas data
    #time is either (1 for current day ,2 for last 7 days? ,3 for max data)
    if time is not None:
        #lint for an empty dataframe
        if time == 1:
            return (datetime.today()-timedelta(days=7)).isoformat()
        if time == 2:
            return (datetime.today()-timedelta(days=31)).isoformat()
        if time ==3:
            return (datetime.today()-timedelta(days=365)).isoformat()
        else:
            print('time wasn not 1 2 or 3')
    else:
        print('unable to compute because dataframe is empty')

#how can each of these be calculated?
#1. floors most used
#pull the data from firebase
#convert to dataframe
#filter by timeslot***
#group by floor
#perform count 
#{timerange: one day  ,floor1: 153 floor2: 91}

@app.route('/count', methods=['GET'])
def countNum():
    try:
        floor_id = request.args.get('floor', type = str)
        timelapse = request.args.get('time', type= int)
        if timelapse:
            timeDif = filterByTime(timelapse)
            fbData = [doc.to_dict() for doc in activity_ref.stream()]
            df = pd.DataFrame(fbData)
            df['floor'] = df['stallID'].str[0]
            dfTimeFiltered = df[df['time_stamp'] > timeDif]

            if floor_id:
                result = dfTimeFiltered[dfTimeFiltered['floor'].str.contains(floor_id)]
                result = result.groupby(['sessionID','stallID'], as_index=False).agg({'time_stamp': lambda time_stamp: time_stamp.max() - time_stamp.min()}).rename(columns={'time_stamp': 'difference'})
                returnResult =  result.to_json(orient = 'records' , date_format='iso')
                parsed = json.loads(returnResult)
                average = result['difference'].mean()
                print(str(average))
                count = result['difference'].count()
                print('returning time data based on a filtered floor list')
                return jsonify(data = parsed, averageDuration = str(average), number = str(count)), 200
            else:
                result = dfTimeFiltered
                result = result.groupby(['sessionID','stallID'], as_index=False).agg({'time_stamp': lambda time_stamp: time_stamp.max() - time_stamp.min()}).rename(columns={'time_stamp': 'difference'})
                returnResult =  result.to_json(orient = 'records' , date_format='iso')
                parsed = json.loads(returnResult)
                average = result['difference'].mean()
                print(str(average))

                count = result['difference'].count()
                print('returning time data without a floor filter')
                return jsonify(data = parsed, averageDuration = str(average), number = str(count)), 200
        else:
            print('a time variable is required to run this function')
        
    except Exception as e:
        return f"An Error Occured: {e}"

port = int(os.environ.get('PORT', 8080))
if __name__ == '__main__':
    app.run(threaded=True, host='0.0.0.0', port=port)
