from __future__ import annotations
import pandas as pd
import re
import numpy as np
import datetime
import glob
from schedule import Schedule
from loggedtrips import LoggedDay


class Arrivals:
	'''Accumulate logged_days directly and then merge plus remove duplicates'''
	def __init__(self, data_folder:str):
		'''
		Params:
			data_folder (str): Path to the folder that contains the desired `.csv` files from subwaydata.nyc. Use `gtfs_script.get_subwaydata` to download these.
		'''

		self.dates = np.unique([f.split('subwaydatanyc_')[1].split('_')[0] for f in glob.glob(f"{data_folder}*.csv")]).tolist()
		
		self.logdict = {date: LoggedDay(f"{data_folder}/subwaydatanyc_{date}_trips.csv", f"{data_folder}/subwaydatanyc_{date}_stop_times.csv") for date in self.dates}


	def __repr__(self) -> str:
		dict_r = {k: f"<loggedtrips.LoggedDay at {hex(id(v))}>" for k,v in self.logdict.items()}
		return dict_r.__str__()


class Timetable:
	'''Full table of schedules plus arrival data'''
	def __init__(self, data_path: str, gtfs_path: str):
		'''
		Params:
			data_folder (str): Path to the folder that contains the desired `.csv` files from subwaydata.nyc. Use `gtfs_script.get_subwaydata` to download these.
			gtfs_path (str): Path to the folder that contains the GTFS Static files
		'''

		self.gtfs_path = gtfs_path
		self.data_path = data_path
		self.dates = np.unique([f.split('subwaydatanyc_')[1].split('_')[0] for f in glob.glob(f"{data_path}*.csv")]).tolist()
		return