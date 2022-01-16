from google.cloud import firestore
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./secrets.json"
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd


firebase_admin.initialize_app()

db = firestore.Client()

# Add a new document
def getActivity():
    activity = list(db.collection(u'activity').stream())
    activity_dict = list(map(lambda x: x.to_dict(), activity))
    return pd.DataFrame(activity_dict)

df = getActivity()
print(df)

df[['floor', 'gender', 'id']] = df['stallID'].str.split('')

print(df)



# Then query for documents
df['floor'] = df['stallID'].str[0]
df['gender'] = df['stallID'].str[1]
df['stall'] = df['stallID'].str[2]

# this is where a function lives that returns the appropriate historic data
def dataframeFilterFloor(floorNumber):
    #we already have df stored as a dataframe. 
    #save only the rows that have floornum = to floorNumber
    #then we use a few methods to show historic data, 
    #and data from the current date
    df2 = df[df['floor'].str.contains(floorNumber)]
    return df2

historicData = dataframeFilterFloor('1')
historicData


#lets group by the 'sessionID' parameter, and then compare the duration. 
#how do we compare two datetime64s?
#THIS IS HOW
df.groupby(by='sessionID').agg({'time_stamp': lambda time_stamp: time_stamp.max() - time_stamp.min()}).rename(columns={'time_stamp': 'difference'})

