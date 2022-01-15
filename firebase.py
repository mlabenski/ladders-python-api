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
