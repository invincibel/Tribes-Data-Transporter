Tribes-Data-Transporter
=======================


Tribes Data Transporter(TDT) is a simple yet powerful *Python Script* which would be running daily at a fixed time and would be responsible for reading JSON data from google cloud storage(link to bucket would be provided below) and writing the data in any of the Gremlin compatible graph databases(for this use-case we would be using Azure’s cosmos db with Gremlin API).



Usage
--------

* `` pip install -r requirements.txt``
* It is preferred to run the .ipynb files in your Jupyter Notebook to avoid Tornado Timeout during HTTPRequests
* Run ``main.py`` which serves as a Full Sync mode Python Operation
* Upon successful Full Sync of GCP | Cosmos DB, the code then runs itself after every 24 hrs thus calling the ``partial_sync`` python script
* The ``config`` file uses the global variables to check for insertions everytime and aviod redundant Edges and Vertices

![Image of Graph in Cosmos DB](/cosmosdbimage1.png)

![Image of Graph in Cosmos DB](/cosmosdbimage2.png)