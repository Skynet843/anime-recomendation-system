import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import re
import pandas as pd


def extract_info(text):
    # Using regular expression to separate "Type" and "Episodes"
    match = re.search(r'(\w+)\s*\((\d+|\?)\s*eps\)', text)

    video_type = None
    episodes = None

    if match:
        video_type = match.group(1)
        episodes_str = match.group(2)
        episodes = int(episodes_str) if episodes_str.isdigit() else None

    return video_type, episodes


def firstStage(min_limit,max_limit):
    anime_data=pd.DataFrame(columns=['name','type','episodes','mal_link'])
    print("Fething Data......")
    for page in tqdm(range(min_limit,max_limit+1,50)):
        site_link=f"https://myanimelist.net/topanime.php?limit={page}"
        r=requests.get(site_link)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
        else:
            print("Request Fail at Page",page)
        sl=soup.select("tr.ranking-list > td:nth-child(2)")
        for item in sl:
            details=item.select(".hoverinfo_trigger")[1]
            name=details.text
            mal_link=details.get('href')
            type,episodes=extract_info(item.select("td > div:nth-child(2) > div:nth-child(4)")[0].text)
            anime_detail=pd.DataFrame({'name':[name],'type':[type],'episodes':[episodes],'mal_link':[mal_link]})
            anime_data = pd.concat([anime_data,anime_detail], ignore_index=True)
    anime_data.to_csv('anime_data.csv',chunksize=100,index=False)
    print("CSV File Save Successfully")
        
if '__main__' == __name__ :
    firstStage(0,50)
        
    
