import hashlib 
import pandas as pd
import datetime

import sys, os
from google.cloud import storage


def download_data_csv(bucket_name, filename, path_local):
	# Initialise a client
	storage_client = storage.Client("de-wallmart")
	# Create a bucket object for our bucket
	bucket = storage_client.get_bucket(bucket_name)
	# Create a blob object from the filepath
	blob = bucket.blob(filename)
	# Download the file to a destination
	blob.download_to_filename(path_local)