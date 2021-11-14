import os
import requests
import time
import re
import ast
import random
import shutil
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
from joblib import Parallel, delayed
from collections import OrderedDict

def get_anime_list():
    """Get the list of all the anime of the first 400 pages"""
    
    anime = []

    # DOWNLOAD ALL THE LINKS
    for page in tqdm(range(0, 400)):
        # GET THE INDEX PAGE
        url = 'https://myanimelist.net/topanime.php?limit=' + str(page * 50)
        response = requests.get(url)
        
        # FOR EACH LINK IN THE INDEX PAGE, GET INDEX, NAME AND LINK
        soup = BeautifulSoup(response.text, 'html.parser')
        for tag in soup.find_all('tr'):
            links = tag.find_all('a')
            for link in links:        
                if type(link.get('id')) == str and len(link.contents[0]) > 1:
                    anime.append((page, link.contents[0], link.get('href')) )
    return anime

	
def save_anime_list(anime):
	"""Create a .txt file for the computed anime_list"""
	
	with open('anime_url_list.txt', 'w', encoding="utf-8") as url_list:
		for page_num, name, link in anime:
			#SEPARATE EACH ELEMENT WITH '\t'
			url_list.write(str(page_num) + '\t' + name + '\t' + link + '\n')
			print('CREATED: anime_url_list.txt')
	
			
def load_anime_list(path="", name="./anime_url_list.txt"):
    """Open the .txt file of the anime list and returns a list containing
       the folder name, anime name and link to the page"""
    
    anime = open(path + name, 'r', encoding="utf-8")
    return [row.split('\t') for row in anime]

			
def create_folders():
    """Create the html folder and all its sub-folders"""
    
    try:
        # CREATE THE hmls/ FILDER
        os.mkdir('htmls/')
    
        # FOR EACH INDEX, CREATE A FOLDER
        for index in range(400):
            os.mkdir(f'htmls/{index}/')
    except FileExistsError:
        print('Folders already exist!')
    else:
        ('Folders successfully created!')
  
      
def get_html_page(index, sub_dir, url):
    """Retrieve the HTML page and save it as an .html file"""
    
    # IF FILE EXISTS, SKIP IT
    file_path = f'htmls/{sub_dir}/article_{index}.html'
    if os.path.exists(file_path):
        return
        
    try:
        # WAIT A RANDOM TIME IN ORDER TO AVOID HTTP STATUS 403
        time.sleep(
            random.uniform(0, 3)
        )
        response = requests.get(url)

        # RAISE AN EXCEPTION IF HTTP_STATUS ISN'T 2xx
        response.raise_for_status()

        # SAVE FILE AS *.html FILE
        with open(file_path, 'w', encoding="utf-8") as file:
            file.write(response.text)

    except requests.exceptions.HTTPError:
        # RETURN INFO IF HTTP ISN'T 2xx 
        return index, sub_dir, url
  
    
def get_missing_htmls(anime_list):
    """Check missing htmls, returns a list of missing files"""
    
    missing_files = []
    
    # GIVEN THE RESULT OF get_html_page, CHECK IF EVERYTHING WENT FINE
    for index, row in enumerate(anime_list, start=1):
        sub_dir, name, link = row
        # IF THE SPECIFIC FILES DOESN'T EXIST IN THE FOLDER
        if not os.path.isfile(f'htmls/{sub_dir}/article_{index}.html'): 
            missing_files.append((index, sub_dir, link[:-1],))
    return missing_files


def check_missing_files(files_list, max_retries=5):
    """Check that all the files are downloaded and no http errors occured"""
    
    # CHECK files_list CONTAINS ALL None(I.E.: THE OUTPUT OF THE PARALLEL
    # COMPUTATION WENT FINE) OTHERWISE COMPUTE THE MISSING FILES
    check = all([x is None for x in files_list])
    counter = 0
    
    # UNTIL THE CHECK IS NOT TRUE, RETRY DOWNLOAD
    while not check:
        new_missing_files = [file for file in files_list if file is not None]
        print(f'Still missing: {len(new_missing_files)} html files')
        new_files_list = Parallel(n_jobs=1, verbose=10)(delayed(get_html_page)(index, sub_dir, url) 
                                             for index, sub_dir, url in new_missing_files)
        check = all([file is None for file in new_files_list])
        counter += 1
        
        # WE SET A MAX NUMBER OF RETRIES TO 5
        if counter == max_retries:
            print('Counter exceeded, still missing:', len([file for file in new_files_list if file is not None]))
            

def zip_folders(name):
    """Create a file zip of all the folders with html files"""
    
    shutil.make_archive(name, 'zip', 'htmls/')
    
    # BONUS: DOWNLOAD IF IT'S FROM COLAB
    try:
        from google.colab import files
        files.download(name + '.zip')
    except Exception:
        pass
    

def get_info_dict(soup):
    """Get useful information from the html page as dict"""
    
    child_dict = {}
    divs = soup.find_all("div", class_='spaceit_pad')

    for element in divs:
        name, *info = element.get_text().rstrip().split()
        child_dict[name[:-1]] = " ".join(info)

    return child_dict
	

def get_dates(row):
    """Retrieve info about starting and ending date"""
    
    try:
        start, end = row.split(' to ')
    except ValueError:
        start, end = row, ''
    return start, end
	
    
def get_related_anime(soup):
    """Retrieve info about the related anime"""
    try:
        anime_detail_related = soup.find("table", class_="anime_detail_related_anime")
        return str(
                list(
                    set([s.text for s in anime_detail_related.find_all('a')])
                )
            )
    except AttributeError:
        return ''

def get_chars_and_voices(soup):
    """Retrieve info about the characters and voices"""
    characters_soup = soup.find_all("h3", class_="h3_characters_voice_actors")
    characters = [name.text for name in characters_soup]
    voices_soup = soup.find_all("td", class_="va-t ar pl4 pr4")
    voices = [name.a.text for name in voices_soup]
    return str(characters), str(voices)

def get_staff(soup):
    """Retrieve info about the staff"""
    try:
        chars_soup = list(soup.find_all("div", class_="detail-characters-list clearfix"))
        staff_soup = chars_soup[1].find_all("td")
        return str([
            [name.a.text.rstrip(), name.div.text.rstrip()[1:]] 
            for name in staff_soup if name.a.text.rstrip()
            ])
    except IndexError:
	    return ''

def parse_page(path, index, anime_list):
    """Given a .html file, creates its .tsv"""
    
    out = OrderedDict()
    
    try:
        # OPEN THE FILE
        with open(path, 'r', encoding="utf-8") as file:
            bs = BeautifulSoup(file.read(), 'html.parser')

        # GET INFORMATIONS
        info = get_info_dict(bs)

        # RETRIEVE ALL THE REQUIRED INFORMATIONS
        out['animeTitle'] = str(bs.title.string).replace(' - MyAnimeList.net', '')#.replace('\n', '')
        out['animeType'] = info['Type']
        out['animeNumEpisode'] = info['Episodes']
        out['releaseDate'], out['endDate'] = get_dates(info['Aired'])
        out['animeNumMembers'] = info['Members'].replace(',', '')
        out['animeScore'] = info['Score'].split()[0]
        out['animeUsers'] = re.findall('(?<=by\s).+(?=\susers)', info['Score'])[0].split(',')[0]
        out['animeRank'] = info['Ranked'].split()[0][1:-1]
        out['animePopularity'] = info['Popularity'][1:]
        out['animeDescription'] = bs.find_all("p", itemprop='description')[0].get_text().replace('\t', '')
        out['animeRelated'] = get_related_anime(bs)
        out['animeCharacters'], out['animeVoices'] = get_chars_and_voices(bs)
        out['animeStaff'] = get_staff(bs)
        out['Url'] = anime_list[index - 1][2]

        new_path = path[:-4] + "tsv" # renaming path as .tsv 

        # CREATE THE .tsv FILE
        with open(new_path, 'w', encoding="utf-8") as file:
            file.write("\t".join(out.keys()))
            file.write("\t".join(out.values()))

    except Exception:
        print(path)
        raise
        
        
def get_partial_tsv(base_path, anime_list, index):
    """Load all the .tsv files for a given sub-folder"""

    path = f'{base_path}/{index}/'
    temp_files = os.listdir(path)
    # GET THE NAME OF ALL THE HTML INTO THE FOLDER
    html_files = [file for file in temp_files if file.endswith('html')]
    # GET THE NAME OF ALL THE TSV INTO THE FOLDER
    tsv_files = [file for file in temp_files if file.endswith('tsv')]
		
    for file in html_files:
        tsv_name = file[:-4] + 'tsv'
        # IF THE SPECIFIC TSV DOESN'T EXISTS
        if tsv_name not in tsv_files:
            # TAKE THE ARTICLE NUMBER
            num = int(file.split('_')[-1].split('.')[0])
            # GENERATE THE .tsv FILE
            parse_page(path + file, num, anime_list)
            
	       
def compute_tsv_files(base_path, anime_list):
    """Create all the .tsv files from the htmls"""
    
    # RETRIEVE THE INFO FROM THE .tsv FILES
    Parallel(n_jobs=-1, verbose=10)(delayed(get_partial_tsv)(base_path, anime_list, index) for index in range(400))


def evaluate_str(string):
    """Convert string to its related python data type 
       (e.g. "['cat', 'dog']" ---> ['cat', 'dog'] """
        
    try:
        return ast.literal_eval(string)
    except SyntaxError:
        return None
		
def evaluate_float(number):
    """Convert string to float. Set -1 if NA"""
    try:
        return float(number)
    except ValueError:
        return -1.0

def get_dataset(base_path):
    """Create the dataset for Q2 (as pandas.DataFrame) from .tsv files"""
    
    dataset = []
    
	# RETRIEVE THE INFO FROM THE .tsv FILES
    for i in range(400):       
        tsv_files = [file for file in os.listdir(f'{base_path}/{i}/') if file.endswith('tsv')]
        for file_name in tsv_files:
            with open(f'{base_path}/{i}/{file_name}', 'r', encoding="utf-8") as file:
                data = "".join(file.readlines()[1:]).split('\t')
                dataset.append(data)
				
    cols = [  'animeTitle', 'animeType', 'animeNumEpisode', 'releaseDate',
                 'endDate', 'animeNumMembers', 'animeScore', 'animeUsers', 'animeRank',
                 'animePopularity', 'animeDescription', 'animeRelated', 'animeCharacters',
                 'animeVoices', 'animeStaff', 'Url']
    
	# CREATE THE PADAS DATAFRAME
    df = pd.DataFrame(dataset, columns=cols)
    
    # PRE-PROCESS NAME
    df['animeTitle'] = df['animeTitle'].apply(lambda x: x[:-1])
	
	# TRANSFORM TO DATETIME COLUMN
    df['releaseDate'] = pd.to_datetime(df['releaseDate'], format='%b %d, %Y', errors='coerce')
    df['endDate'] = pd.to_datetime(df['endDate'], format='%b %d, %Y', errors='coerce')
	
    #CONVERT UNKNOWN TO -1 AND COLUMN TO INT
    for col_name, dtype in {
			'animeUsers': int,  
			'animeRank': int,
			'animePopularity': int,  
            'animeNumEpisode': int, 
            'animeNumMembers': int
            }.items():
        df[col_name] = df[col_name]\
	                    .apply(lambda x: x if x.isdigit() else -1)\
						.astype(dtype)     
	
	# CONVERT STRING INFO TO LIST OF STRING
    for col_name in ['animeRelated', 'animeCharacters', 'animeVoices', 'animeStaff']:
        df[col_name] = df[col_name].apply(evaluate_str)
	
    # PRE-PROCESS SCORE AND CONVERT TO FLOAT
    df['animeScore'] = df['animeScore'].apply(evaluate_float).astype(float)
    print('DATASET - LOADED')
    
    return df