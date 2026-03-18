import requests
import zipfile

def get_gtfs():
	'''Download Regular GTFS Static files from MTA website and unzip'''
	URL = "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip"
	r = requests.get(URL)
	with open("gtfs/gtfs.zip", "wb") as fd:
		fd.write(r.content)
	with zipfile.ZipFile("gtfs/gtfs.zip","r") as zip_ref:
		zip_ref.extractall("gtfs/gtfs_files/")
