from __future__ import annotations
import pandas as pd
import re
import numpy as np
from src.utils import zip_reduce
import glob
import textwrap
import datetime

class TripDate:
	'''quick object for timestamp boundaries and trip date'''
	def __init__(self, year:int, month:int, day:int):
		self.year = year
		self.month = month
		self.day = day
		self.start_time = int(datetime.datetime(year,month,day).timestamp())
		self.end_time = int((datetime.datetime(year,month,day) + datetime.timedelta(1)).timestamp())
		
	def date(self) -> datetime.datetime:
		'''return datetime object'''
		return datetime.datetime(self.year,self.month,self.day)

	def __repr__(self) -> str:
		return self.date().strftime('%Y-%m-%d')

class LoggedDay:

	'''Class for the subway arrival log trips files'''
	def __init__(self, trip_path: str, stop_path: str):
		'''
		Params:
			data_folder (str): Name of the folder that contains the desired `.csv` files from subwaydata.nyc. Use `gtfs_script.get_subwaydata` to download these.
		'''
		
		self.raw_trips = pd.read_csv(trip_path)
		# tuple unpack shenanigans
		self.date = TripDate(*[int(f) for f in trip_path.split('_')[-2].split('-')])
		self.trips = self._preprocess(self.raw_trips)
		self.lookup = self._merge(self.trips, stop_path)
		return

	def _preprocess(self, raw_trips: pd.DataFrame) -> pd.DataFrame:
		'''Set up IDs to match formatting, see `src/schedule.py` for more'''
		trips = raw_trips.copy()
	
		new_ids = zip_reduce(trips.route_id.to_list(), trips.trip_id.to_list(), lambda id, shape: '.'.join([id, re.split(r"\.",shape,maxsplit=1)[1]]))
		trips['short_id'] = pd.Series(zip_reduce(trips.trip_id.map(lambda s: str(s).split('_')[-2]).to_list(), new_ids, lambda t, id: '_'.join([t,id]))).map(lambda s: re.sub('6X','6',s) if '6X' in s else s)
				# remove trips on end time of day since they are in the next day anyways
		trips = trips[trips.start_time.between(self.date.start_time, self.date.end_time, inclusive='left')]
		def tiny_exists(r):
			try: 
				return re.search(r"(.)*_..?.\.[NS]",str(r)).group() # type: ignore
			except AttributeError:
				print(r)
				raise AttributeError

		trips['tiny_id'] = trips.short_id.map(lambda s: tiny_exists(s)) # type: ignore
		trips['start_time_formatted'] = trips.vehicle_id.map(lambda s: re.split(r"( \d+\+? ?)",s)[1].strip()).map(lambda t: ':'.join(textwrap.wrap(str(t).ljust(6,'0').replace('+','3'),2)))
		return trips

	def _merge(self, trip: pd.DataFrame, stop: str) -> pd.DataFrame:
		'''Join on stop times'''
		stoplog2 = pd.read_csv(stop, dtype=str)
		return trip.copy().set_index('trip_uid').merge(stoplog2, on='trip_uid')
	

	def __getitem__(self, key):
		return self.trips[key]
	def __repr__(self) -> str:
		return self.trips.to_string()
