import dotenv
import nest_asyncio
from gremlin_python.driver import client, serializer
import sys
import traceback
from google.cloud import storage
import os
import shutil
from datetime import datetime,date,timedelta
import json
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
import re
from collections import defaultdict
import partial_sync
import time
import schedule
from config import *

nest_asyncio.apply()
dotenv.load_dotenv()


# DATE CONVERSION TO CHECK IF FILE DOWNLOADED IS DURING LAST 24 HRS
def convert_date(_date):
    return datetime.strptime(_date.strftime(date_format), date_format)


# ADD VERTICES IN COSMOS DB VIA .submitAsync() callback()
def insert_vertices(VERTIECS):
    print("Adding VERTICES...")
    for vertex in VERTICES:
        callback = gremlin_client.submitAsync(vertex)
        if callback.result() is not None:
            print("Inserted this vertex:\n{0}".format(callback.result().one()))
        else:
            print("Something went wrong with this query: {0}".format(vertex))
    print("All Vertices added...")



'''
    ADD EDGES CONNECTING VERTICES WITH ID AS IDUNIQUE
    GREMLIN QUERY : 
        g.V('IdUnique_From').addE('Type').to(g.V('IdUnique_To')).property(key,value)
'''
def insert_edges():
    for _object in edge_jsonObject:
      parts = []

    #   PREPROCESS QUERY FOR ADDING KEY VALUE TO THE QUERY | .PROPERTY(KEY,VALUE)
      for key, value in _object["Property"].items():
        part = ".property('" + str(key) + "','" + str(value) + "')"
        parts.append(part)

      property_query = "".join(parts)

    #   ITERATE OVER ALL THE VERTICES HAVING REQUIRED FROM/TO LABEL/IDOBJECT
      for IdUnique_from in idof[_object["FromLabel"]+_object["FromIdObject"]]:

        for IdUnique_to in idof[_object["ToLabel"]+_object["ToIdObject"]]:
          
          #   CHECK FOR DeDuplication ONLY IF THERE IS AN EDGE 
          if str(IdUnique_from+IdUnique_to) in isedge :
            
            if _object["DeDuplication"] == "TRUE":
              continue
            
            else:
              query = "g.V('" + IdUnique_from + "').addE('" + _object["Type"] + "').to(g.V('" + IdUnique_to + "'))"
 
              final_query = query + property_query
              callback = gremlin_client.submitAsync(final_query)
 
              if callback.result() is not None:
                  print("Inserted this edge:\n{0}".format(callback.result().one()))
              else:
                  print("Something went wrong with this query:\n{0}".format(final_query))
          
          else:  
            
            isedge[IdUnique_from+IdUnique_to] = True

            query = "g.V('" + IdUnique_from + "').addE('" + _object["Type"] + "').to(g.V('" + IdUnique_to + "'))"
            
            final_query = query + property_query
            callback = gremlin_client.submitAsync(final_query)

            if callback.result() is not None:
                print("Inserted this edge:\n{0}".format(callback.result().one()))
            else:
                print("Something went wrong with this query:\n{0}".format(final_query))
    
    print("All Edges added...")


# ITERATE OVER JSON FILES TO ADD VERTICES AND EDGES
def update_edges_and_vertices(fileName):
        with open(fileName) as jsonFile:
                jsonObject = json.load(jsonFile)
                jsonFile.close()

        for _object in jsonObject:
                '''
                    INSERT VERTICES IN COSMOS DB 
                    GREMLIN QUERY:
                        g.V('IdUnique').property('id','IdUnique').property(key,value).property('pk','pk')
                '''
                if _object["Kind"] == "node":
                        parts = []
                        part1 = "g"
                        part2 = ".addV('" + str(_object["IdUnique"]) + "')"
                        part3 = ".property('id','" + str(_object["IdUnique"]) + "')"
                        parts.append(part1)
                        parts.append(part2)
                        parts.append(part3)
                        
                        for key, value in _object["Property"].items():
                                part4 = ".property('" + str(key) + "','" + str(value) + "')"
                                parts.append(part4)

                        lastpart = ".property('pk', 'pk')"
                        parts.append(lastpart)

                        query_insert_vertex = "".join(parts)
                        
                        VERTICES.append(query_insert_vertex)   
                        
                        # CREATE ADD IDUNIQUE IN HASH MAP TO ADD EDGES LATER
                        for label in _object["Label"]:
                                idof[label + str(_object["Property"]["IdObject"])].append(str(_object["IdUnique"]))

                # ADD JSON OBJECTS INTO EDGES FOR PROESSING THEM INTO GREMLIN QUERIES IN INSERT_EDGES()
                else:
                        edge_jsonObject.append(_object)
   

# DOWNLOAD DATA FROM GCP
def sync_files_full_sync(bucket_name):
    makedir = cwd + "\\graph-data/"
    
    try:
        os.mkdir(makedir)
    except Exception:
        pass
    
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    
    for blob in blobs:

        if blob.name != "graph-data/":
            
            destination_file_name = cwd + "\\" + blob.name
            blob.download_to_filename(destination_file_name)
            print("Downloaded")
            update_edges_and_vertices(destination_file_name)
            print("updated_edges_and_vertices")
            insert_vertices(VERTICES)
            insert_edges()

            print("Database Updated Successfully for {} | Full Sync".format(blob.name))

    shutil.rmtree(makedir)
    print("Full Sync Completed...")

# EMPTY LIST OF VERTEX AND EDGE QUERIES
VERTICES = []
edge_jsonObject = []

# DATE TIME TO CHECK INCOMING DATA 
today = datetime.now()
date_format = "%m/%d/%Y, %H:%M:%S"


# SET ENVIRONMENT CREDENTIALS FOR GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_to_gcp_credentials


'''
CALL FUNCTION TO :
1) FETCH JSON FILES FROM GCP
2) ADD VERTICES AND EDGES INTO LIST
3) PROCESS GREMLIN QUERIES TO UPDATE GRAPH IN COSMOS DB
'''
sync_files_full_sync(BUCKET_NAME)


'''
ONCE FULL SYNC IS COMPLETED, 
EVERY NEXT SYNC WOULD BE PARTIAL SYNC
WHICH RUNS partial_sync.sync_files(BUCKET_NAME)
TO UPDATE COSMOS DB
'''
schedule.every(24).hours.do(partial_sync.sync_files(BUCKET_NAME))
while True:
    schedule.run_pending()
    time.sleep(1)