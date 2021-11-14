import requests
import pathlib
import time
import os
import re
import numpy as np

from bs4 import BeautifulSoup
from joblib import Parallel, delayed
from textblob import TextBlob
from functions_q1 import load_anime_list, get_dataset

def get_review(index, row, limit=5):
    """Get first 500 words of top N reviews for a given anime"""
    
    #WAITING TIME IF 403 OCCURED
    wait = 90 
    if os.path.exists(f'./reviews/article_{index}_reviews.txt'):
        return
    
    try:
        *_, url = row
        
        cond = True
        # TRY GET REQUESTS UNTIL WE GEt THE PAGE
        while cond:
            try:
                url = url.rstrip() + '/reviews'
                print(repr(url))
                response = requests.get(url)
                response.raise_for_status()
                cond = False
            except requests.exceptions.HTTPError:
                time.sleep(wait)
                wait += 30 # ADDIG DELAY IF FIRST TRY DOESN'T SUCCEED
        
        comments = []
        bs = BeautifulSoup(response.text, 'html.parser')
        counter = 0
        
        # GET THE COMMENTS FROM THE REVIEW PAGE
        for tag in bs.find_all("div", class_="borderDark", limit=limit):
            try:
                div = list(tag.find_all("div", class_="spaceit"))
                temp = div[-1].text\
                    .rstrip()\
                    .split('                          ')[1]\
                    .replace('\t', '')\
                    .replace('  ', '')\
                    .replace('\n', '')\
                    .replace('\r', '')[:500]
                                
                comments.append(temp)
                
                # TAKE ONYL THE FIRST N VALID COMMENTS
                if counter == limit:
                    break

                counter += 1
                
            except IndexError:
                pass
        
        # CREATE THE .txt FILE WITH THE COMMENTS
        path = f'./reviews/article_{index}_reviews.txt'
        with open(path, 'w', encoding="utf-8") as file:
            file.write("\n\n".join(comments))
    except Exception:
        raise
        

def get_reviews():
    """Retrieve riviews for all the html pages"""
    # MAKE FOLDER IF DOESN'T EXISTS
    path = pathlib.Path('./reviews/')
    path.mkdir(parents=True, exist_ok=True)
    # LOAD THE LIST WITH ALL THE ANIME
    anime_list = load_anime_list()
    
    # RETRIEVE REVIEWS
    Parallel(n_jobs=-1, verbose=10)(delayed(get_review)(index,row) 
                for index, row in enumerate(anime_list, start=1))
    

def pre_process_text(text):
    """Returns the string without special characters"""
    return re.sub('[^a-zA-Z0-9 \n\.!?]', '', text)


def get_sentiment(text):
    """Get sentiment scores (polarity and subjectivity"""
    
    blob = TextBlob(text)
    
    return blob.sentiment


def get_score(text):
    """Given a review, get its score (watch the notebook for additional 
    informations about the score)"""
    
    # GET THE SENTIMENT OUTPUT
    sentiment = get_sentiment(text)
    polarity = sentiment.polarity
    subjectivity = sentiment.subjectivity
    
    # RETURN THE NEW SCORE
    return polarity * (1 + np.log2(2 - subjectivity)) 


def get_article_score(path, expected_reviews=5):
    """Calculate the score for a signle anime"""
    
    # LOAD THE REVIEWS FOR A GIVEN .txt FILE
    with open(path, 'r', encoding='utf-8') as file:
        reviews = file.read().split('\n\n')
    
    scores = []
    
    # GET THE SCORES FOR THE REVIEWS
    for review in reviews:
        scores.append(
            get_score(review)
            )

    # AVERAGE THE SCORES AND ADD PENALIZATION FOR ANIME WITH LESS THEN N REVIEWS
    score = np.mean(scores) * (len(reviews) / expected_reviews)
    
    return  score


def compute_scores(base_path):
    """Calculate the new socre for all the animes"""
    
    reviews_list = os.listdir(base_path)
    output = Parallel(n_jobs=-1, verbose=10)(delayed(get_article_score)(base_path + review_name) 
                for review_name in reviews_list)

    return output


def get_new_dataset():
    """Get the dataset from the .tsv files plus the new score"""
    
    dataset = get_dataset('./htmls')    
    dataset['newScore'] = compute_scores('./reviews/')

    return dataset
    