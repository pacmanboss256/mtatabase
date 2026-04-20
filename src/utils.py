from functools import reduce
from typing import Callable
import numpy as np
import datetime
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")


def zip_reduce(a: list, b: list, f: Callable) -> list:
	'''Zip two numpy arrays and reduce by a function'''
	zipped = zip(a,b)
	reduced = map(lambda _: reduce(f, _), zipped)

	return list(reduced)



def timestr_to_mta(t: str) -> str:
	'''Convert a string "HH:MM:SS" into hundredths of a minute past midnight'''
	lt = np.array([int(j) for j in t.split(':')])
	hdm = sum((lt * np.array([60,1,1/60]))*100)
	return str(int(hdm)).rjust(6,'0')


def timestamp_to_mta(ts: int) -> int:
	'''Convert a timestamp value into MTA time (which is in hundredths of a minute past midnight relative to UTC)'''
	t = datetime.datetime.fromtimestamp(ts, tz=UTC).time()
	return int((t.hour*60 + t.minute + t.second/60)*100)