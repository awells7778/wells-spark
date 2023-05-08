"""
Landing job to extracting Grand Exchange data from the public OSRS API.  This job returns a json file
that will be flattened into a parquet file during the staging process.
"""

import requests
import json
import os
import time
from datetime import datetime
from string import ascii_lowercase


def main():
    
    # Configure working directory
    os.chdir("/opt")
    
    # Get our combined json response object 
    combined_results = []

    for letter in ascii_lowercase:
        category = 1 # grand exchange items
        page = 1 # first page only due to request throttle
            
        url = f"https://secure.runescape.com/m=itemdb_oldschool/api/catalogue/items.json?category={category}&alpha={letter}&page={page}"
        response = requests.get(url)
        
        print(f"incrementing page number {letter} - {page}: response code: {response.status_code}")
        
        try :
            response_data = response.json()
            
            if response.status_code != 200:
                print(f"Request failed with status code {response.status_code}")
                break
            
            if len(response_data) > 0:
                combined_results.extend(response_data["items"])

            
        except requests.exceptions.JSONDecodeError as e:
            print("Error decoding JSON:", e)
        
        # API throttles requests at 1 every 5 seconds        
        time.sleep(5)

    # and dump it into target json file 
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    target_file = f"spark-data/datalake/landing/lnd_grand_exchange_{timestamp}.json"       
    with open (target_file, "w") as f:
        json.dump(combined_results, f)

if __name__ == "__main__":
    main()