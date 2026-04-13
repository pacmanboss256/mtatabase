from __future__ import annotations
import pandas as pd
import re
import numpy as np
from src.utils import zip_reduce
import glob
import datetime

class Schedule:
	'''GTFS Schedule class that has a dataframe with every trip and their scheduled stops plus arrival times'''
	def __init__(self, gtfs_folder:str='gtfs/gtfs_files'):
		'''
		Params:
			schedule (str): Filepath to the GTFS Static file folder 
		'''
		self.gtfs_folder = gtfs_folder
		self._raw_trips = pd.read_csv(f'{gtfs_folder}/trips.txt')
		self.trips = self._preprocess(self._raw_trips)
		self.lookup = self._create_id()
		self.route_map = self._create_route_map()
		return
	
	def _preprocess(self, raw: pd.DataFrame) -> pd.DataFrame:
		'''Since the MTA doesn't have any semblence of consistent formatting, the arrival log format is also messed up.
		This method generates a new column of IDs formatted the same way as the arrival trip files which allow for joins later on.
		'''
		def _get_shapes(trips: pd.DataFrame) -> pd.Series:
			'''fix possible nonexistent shape_ids'''
			# will always match since the trip_id field is constructed in a way that it ends with a shape id regardless of format
			new_shape_id = trips.trip_id.map(lambda s: re.search(r"_..?.\.[NS](?:.)*",str(s)).group()[1:]) # type: ignore 
			return new_shape_id
		

		trips = raw.copy()
		trips['shape_id'] = _get_shapes(trips)
		new_ids = zip_reduce(trips.route_id.to_list(), trips.shape_id.to_list(), lambda id, shape: '.'.join([id, re.split(r"\.",shape,maxsplit=1)[1]]))
		trips['shape_route_id'] = new_ids
		times = trips.trip_id.map(lambda s: str(s).split('_')[-2]).to_list()

		trips['short_id'] = pd.Series(zip_reduce(times, new_ids, lambda t, id: '_'.join([t,id])))

		return trips
	
	def _create_id(self):
		'''Creates a unique ID and lookup table since we have to reverse engineer some shape IDs due to duplicate routes/times but starting offset. GTFS does not have a unique ID for real time arrival data so we have to get clever (or jank). This is necessary for linking to the arrival logs, as the shape ID is optional for realtime GTFS.'''

		srd = pd.read_csv(f'{self.gtfs_folder}/stop_times.txt')
		schedcopy = self.trips.copy()
		id_key = schedcopy[['trip_id','shape_route_id','shape_id']]
		id_key['trip_fix'] = zip_reduce(id_key.trip_id.to_list(), id_key.shape_route_id.to_list(), lambda t, s: '_'.join(str(t).split("_", 2)[:2] + [str(s)]))

		# we always have x.values since x is generated from dataframe column grouping
		shape_map = srd.groupby('trip_id').apply(lambda x: x.values).reset_index() # type: ignore

		shape_map.columns = ['trip_id','stop_list']

		# we can always transpose s here since it too is from a grid array
		depth = shape_map.stop_list.map(lambda s: (np.transpose(s)[:3])).values # type: ignore
		tr_map = pd.DataFrame.from_records(depth, columns = ['stops','arr','dep'])
		tr_map.insert(0, 'trip_id', shape_map.trip_id)

		## horrible dataframe joinimg
		joined = schedcopy.join(tr_map.set_index('trip_id'), on='trip_id').set_index('trip_id')
		test2 = id_key.join(joined.drop(['shape_id','shape_route_id'],axis=1), on='trip_id', lsuffix='_')
		return test2
	
	def _create_route_map(self):
		'''Create a table of MTA shape ids and their corresponding stop sequences'''
		rtmap = self.lookup[['shape_id','stops']].copy()
		rtset = set() ## set to easily drop duplicates
		for a,b in rtmap.values:
			rtset.add((a,tuple(b)))
		rtd = pd.DataFrame(rtset, columns=['shape_id','routes'])
		return rtd
	

	def __getitem__(self, key):
		return self.trips[key]
	
	def __repr__(self) -> str:
		return self.trips.to_string(index=False)
	
	

class LoggedTrips:

	'''Class for the subway arrival log trips files'''
	def __init__(self, data_path: str):
		'''
		Params:
			data_folder (str): Name of the folder that contains the desired `.csv` files from subwaydata.nyc. Use `gtfs_script.get_subwaydata` to download these.
		'''
		def _aggregate(data_path: str = data_path) -> tuple[list[pd.DataFrame], list]:
			'''combines trips.csv databases files into a single database'''
			
			all_files = glob.glob(f'data/{data_path}/*_trips.csv') # separate from stop_times.csv files which come later
			dfl =[self._preprocess(pd.read_csv(f)) for f in all_files]
			dates = [f.split('_')[-2] for f in all_files]
			return dfl, dates
		
		
		self.raw_trips, self.dates = _aggregate(data_path)
		self.trips = pd.concat(self.raw_trips, ignore_index=True)
		return

	def _preprocess(self, raw_trips: pd.DataFrame):
		'''Convert timestamp into date-tuples with short ids'''
		def _create_date_id(s:str):
			time, shape = s.split('_')
			dt = datetime.datetime.fromtimestamp(float(time))
			return [dt, shape]
		trips = raw_trips.copy()
		new_df =  pd.DataFrame(map(_create_date_id, trips.trip_uid), columns=['datetime', 'shape_id'])
		return pd.concat([trips,new_df],axis=1)
	

	def __getitem__(self, key):
		return self.trips[key]
	def __repr__(self) -> str:
		return self.trips.to_string(index=False)

class TripDB:
	'''Class for the database of scheduled and actual times'''

	def __init__(self):

		return