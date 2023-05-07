"""
Landing job to extracting Grand Exchange data from the OSRS API.  This job returns a json file
that will be flattened into a parquet file during the staging process.
"""

import requests
import json
import configparser
import os
from string import ascii_lowercase


def main():
    # Get our combined json response object 
    combined_results = []
    
  #  for letter in ascii_lowercase:
    category = 1
    page = 1
        
    while True:
        url = f"https://secure.runescape.com/m=itemdb_oldschool/api/catalogue/items.json?category={category}&alpha=a&page={page}"
        response = requests.get(url)
        response_data = response.json()
        
        if response.status_code != 200:
            print(f"Request failed with status code {response.status_code}")
            break
        
        if not response_data["items"]:
            break
        
        combined_results.extend(response_data["items"])
        page += 1
        print(f"incrementing page number {page}")
    # and dump it into target json file    
    with open ("landing_grand_exchange.json", "w") as f:
        json.dump(combined_results, f)
            

if __name__ == "__main__":
    main()