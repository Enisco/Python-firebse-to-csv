
import csv
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

data_headers = ['Document id','Latitude', 'Longitude', 'Image link', 'Meter ID', 'Time of installation', 'Sim inside Meter']
# Use the private key file of the service account directly.
cred = credentials.Certificate(r"C:\Users\HP\Downloads\Documents\Python firebase serl meters details\bloom-meter-installers-app-firebase-adminsdk-kxzqu-88acae3623.json")
app = firebase_admin.initialize_app(cred)
firestore_client = firestore.client()
# Read all files/documents in the db
coll_ref = firestore_client.collection('serl_meters')
docs = coll_ref.stream()

data_list = []
data_values = []

with open('serl_meters_installation_data.csv', mode='w') as serl_meters_installation_data:
    serl_meters_installation_data_writer = csv.writer(serl_meters_installation_data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    serl_meters_installation_data_writer.writerow(data_headers)
    serl_meters_installation_data.close()

for doc in docs:
    address = doc.to_dict()['Location address']
    address_split = address.split(',', 1)
    latitude = address_split[0].split(':', 1)
    longitude = address_split[1].split(':', 1)
    latitude = latitude[1]
    longitude = longitude[1]

    doc_value_list = [ doc.id, latitude, longitude , doc.to_dict()['Image link'], doc.to_dict()['Meter ID'], doc.to_dict()['Time of installation'], doc.to_dict()['Sim inside Meter'] ]

    with open('serl_meters_installation_data.csv', mode='a') as serl_meters_installation_data:
        serl_meters_installation_data_writer = csv.writer(serl_meters_installation_data, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        serl_meters_installation_data_writer.writerow(doc_value_list)
        serl_meters_installation_data.close()
    print (doc_value_list)

# ------------------------------------------------------------------------------------------

# Write files/documents to the db
# doc_ref = firestore_client.collection("iremide").document("test")
# doc_ref.set(
#     {
#         "name": "HP EliteBook Model 1",
#         "brand": "HP",
#     }
# )

# Read files/documents from the db
# doc_ref = firestore_client.collection('serl_meters').document("07030718204")

# print(f"The document id is {doc_ref.id}")
# doc = doc_ref.get()
# print(f"The document is {doc.to_dict()}")
