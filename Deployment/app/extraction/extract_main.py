import hashlib 
import pandas as pd
import datetime

import sys
sys.path.append('../')

import os


def read_csv(path):
	df = pd.read_csv(path)
	return df