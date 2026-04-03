from __future__ import annotations
import pandas as pd
import re
from functools import reduce

class Schedule:
	'''GTFS Schedule class'''
	def __init__(self, gtfs_folder:str='gtfs/gtfs_files'):
		'''
		Params:
			schedule (str): Filepath to the GTFS Static file folder 
		'''
		self._raw_trips = pd.read_csv(f'{gtfs_folder}/trips.txt')
		self.trips = self._preprocess(self._raw_trips)
		return
	
	def _preprocess(self, raw: pd.DataFrame) -> pd.DataFrame:
		'''Since the MTA doesn't have any semblence of consistent formatting, the arrival log format is also messed up.
		This method generates a new column of IDs formatted the same way as the arrival trip files which allow for joins later on.
		'''
		trips = raw.copy()
		new_ids = list(map(lambda _: reduce(lambda id,shape: '.'.join([id, re.split(r"\.",shape,maxsplit=1)[1]]), _),list(zip(trips.route_id.values, trips.shape_id.values))))
		trips['adj_id'] = new_ids




		return trips

	def __repr__(self) -> str:
		return self.trips.to_string(index=False)
	
	
class TripDB:
	'''Class for the database of scheduled and actual times'''

	def __init__(self):

		return