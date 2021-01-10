import os
import dotenv
from collections import defaultdict
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver import client, serializer

dotenv.load_dotenv()
ENDPOINT = os.getenv('ENDPOINT')
DATABASE = os.getenv('DATABASE')
COLLECTION = os.getenv('COLLECTION')
PRIMARY_KEY = os.getenv('PRIMARY_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
cwd = os.getcwd()
path_to_gcp_credentials = cwd + r"\My First Project-4f7663bcebf3.json"
idof = defaultdict(list)
isedge = defaultdict(bool)

gremlin_client = client.Client(
        ENDPOINT , 'g',
        username="/dbs/" + DATABASE + "/colls/" + COLLECTION,
        password=PRIMARY_KEY,
        message_serializer=serializer.GraphSONSerializersV2d0()
)
g = traversal().withRemote(gremlin_client)
