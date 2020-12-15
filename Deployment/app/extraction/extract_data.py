from google.cloud import storage


def download_data_csv(bucket_name, filename, path_local):
	storage_client = storage.Client()
	# Create a bucket object for our bucket
	bucket = storage_client.get_bucket(bucket_name)
	# Create a blob object from the filepath
	blob = bucket.blob(filename)
	# Download the file to a destination
	blob.download_to_filename(path_local)