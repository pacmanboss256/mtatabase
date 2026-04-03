from functools import reduce
from types import FunctionType


def zip_reduce(a:list, b:list, f: FunctionType) -> list:
	'''Zip two lists and reduce by a function'''
	zipped = zip(a,b)
	reduced = map(lambda _: reduce(f, _), zipped)

	return list(reduced)