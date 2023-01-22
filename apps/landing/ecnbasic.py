from requests import get
import json

url = 'https://api.census.gov/data/2017/ecnbasic'
x = get(url).json()
