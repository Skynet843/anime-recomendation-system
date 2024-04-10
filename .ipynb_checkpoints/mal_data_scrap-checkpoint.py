import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import re
import pandas as pd
import time
import json


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
    anime_data=pd.DataFrame(columns=['name','type','episodes','mal_id'])
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
            id_pattern = r'/(\d+)/'
            mal_id = re.search(id_pattern, mal_link).group(1)
            type,episodes=extract_info(item.select("td > div:nth-child(2) > div:nth-child(4)")[0].text)
            anime_detail=pd.DataFrame({'name':[name],'type':[type],'episodes':[episodes],'mal_id':[str(mal_id)]})
            anime_data = pd.concat([anime_data,anime_detail], ignore_index=True)
    anime_data.to_csv('anime_data.csv',chunksize=100,index=False)
    print("CSV File Save Successfully")

import time
def secondStage(data):
    english_titles=[]
    studios=[]
    scores=[]
    descriptions=[]
    generes=[]
    count=0
    for site_id in tqdm(data['mal_id']):
        site_link=f"https://api.jikan.moe/v4/anime/{site_id}"
        time.sleep(0.5)
        count=count+1
        try:
            r=requests.get(site_link)
            while r.status_code==429:
                time.sleep(5)
                r=requests.get(site_link)
            parsed_data = json.loads(r.text)['data']
            english_title=parsed_data.get('title_english',None)
            studio=[]
            for studio_item in parsed_data.get('studios',None):
                studio.append(studio_item.get('name',None))
            score=parsed_data.get('score',None)
            description=parsed_data.get('synopsis',None)
            genere=[]
            for genere_item in parsed_data.get('themes',None):
                genere.append(genere_item.get('name',None))
            for genere_item in parsed_data.get('demographics',None):
                genere.append(genere_item.get('name',None))
            for genere_item in parsed_data.get('genres',None):
                genere.append(genere_item.get('name',None))
            english_titles.append(english_title)
            studios.append(studio)
            scores.append(score)
            generes.append(genere)
            descriptions.append(description)
        except:
            english_titles.append(None)
            studios.append(None)
            scores.append(None)
            generes.append(None)
            descriptions.append(None)
            print("BOOM BUM")
    data['english_title'] = english_titles
    data['studio'] = studios
    data['score'] = scores
    data['genere'] = generes
    data['description']=descriptions
    data.to_csv('anime_data_updated.csv',chunksize=100,index=False)
        
if '__main__' == __name__ :
    # firstStage(0,10000)
    data=pd.read_csv('anime_data.csv')
    secondStage(data)
        
    
