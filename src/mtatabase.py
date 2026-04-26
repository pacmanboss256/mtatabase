from __future__ import annotations
from typing import Any
import pandas as pd
import re
import numpy as np
import datetime
import glob
from schedule import Schedule
from loggedtrips import LoggedDay
from utils import get_DayType



class LogDict:
	'''dict of {date: LoggedDay} for all logged days'''
	def __init__(self, data_path:str, dates: list[str]):
		self.logdict = {date: LoggedDay(f"{data_path}/subwaydatanyc_{date}_trips.csv", f"{data_path}/subwaydatanyc_{date}_stop_times.csv") for date in dates}

	def __getitem__(self, key):
		return self.logdict[key]

	
	def __repr__(self) -> str:
		dict_r = {k: f"<loggedtrips.LoggedDay at {hex(id(v))}>" for k,v in self.logdict.items()}
		return dict_r.__str__()
	
		
class MTAtabase:
	'''Accumulate logged_days and merge with schedule'''
	def __init__(self, data_path:str,gtfs_path:str):
		'''
		Params:
			data_path (str): Path to the folder that contains the desired `.csv` files from subwaydata.nyc. Use `gtfs_script.get_subwaydata` to download these.
			gtfs_path (str): Path to the folder that contains the GTFS Static files
		'''

		self.dates = np.unique([f.split('subwaydatanyc_')[1].split('_')[0] for f in glob.glob(f"{data_path}*.csv")]).tolist()
		self.data_path = data_path
		self.gtfs_path = gtfs_path
		
		self.arrival_logs = LogDict(data_path, self.dates)

		self.schedule = Schedule(gtfs_path)
		self.full_table = self._merge_all()

	def _merge(self, schedule: Schedule, logday: LoggedDay) -> pd.DataFrame:
		'''Merge one day of arrival data with schedule'''
		lkw = schedule.lookup[self.schedule.lookup.service_id == logday.day_type].copy()
		loglk = logday.lookup.copy()

		key_id = []
		for t in zip(loglk.tiny_id, loglk.short_id):
			if t[1] in lkw.short_id.values:
				key_id.append(t[1])
			elif t[0] in lkw.tiny_id.values:
				key_id.append(t[0])
			else:
				key_id.append(pd.NA)

		lkw_key = []
		for t in zip(lkw.tiny_id, lkw.short_id):
			if t[1] in loglk.short_id.values:
				lkw_key.append(t[1])
			elif t[0] in loglk.tiny_id.values:
				lkw_key.append(t[0])
			else:
				lkw_key.append(pd.NA)


		lkw['key_id'] = lkw_key
		loglk['key_id'] = key_id


		lkw_filter = lkw[lkw.key_id.notna()]
		trip_filter = loglk[loglk.key_id.notna()]

		return pd.merge(lkw_filter, trip_filter, on='key_id')

	def _merge_all(self):
		merged_days = []
		for logday in list(self.arrival_logs.logdict.values()):
			merged_days.append(self._merge(self.schedule, logday))
		
		ft = pd.concat(merged_days)
		return merged_days
	