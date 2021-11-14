from tqdm import tqdm
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize
from nltk.stem import *
import numpy as np
from numpy import dot
from numpy.linalg import norm



tqdm.pandas()

def preprocess(text):
    ps = PorterStemmer()
    tokens = word_tokenize(text) # Split in words
    
    tokens = [word for word in tokens if word not in stopwords.words('english')] # Remove non-english
    tokens=[word.lower() for word in tokens if word.isalpha()] # Remove punctuation
    tokens = [ps.stem(word) for word in tokens] # Stem
    # lemamtization 
    return ' '.join(tokens)


def get_inverted_index(docs):
    inverted_index = {}
    
    for i, doc in enumerate(tqdm(docs)):
        for word in doc.split():
            # i is the index of the doc
            # doc is the current document
            if word not in inverted_index.keys():
                inverted_index[word] = [i]
            elif i not in inverted_index[word]:
                inverted_index[word].append(i)
    return inverted_index


def query(query, inverted_index):
    """
    @param query: the query specified by user
    @return a list of all documents that contain all words in the query 
    """
    corpus = list((inverted_index.keys()))
    a = np.array(list(inverted_index.values()), dtype=object)
    inters = set(np.concatenate(a).ravel().tolist())
    # print(inters)
    for word in tqdm(query.split()):
        containing_docs = set()
        if word in corpus:
            containing_docs = inverted_index[word]
            containing_docs = set(containing_docs)
        inters = inters & containing_docs
    return list(inters)



def transform_to_df(tfidf_dict):
    """
    @param: tfidf_dict is the tfidf dictonary
    @return: a pandas dataframe representation of the tfidf dictionary, row index indicates document index
    """
    d = {}
    for word in tfidf_dict.keys():
        d[word] = [pair[1] for pair in tfidf_dict[word]]
    return d



def compute_new_tfidf(_query, _tfidf_inverted_index, _inverted_index, idf_vec):
    
    tf_idf = []
    
    for idx, word in enumerate(tqdm(_inverted_index.keys())):
        tf = _query.count(word) / len(_query.split())
        idf = idf_vec[idx]   
        tf_idf.append(tf * idf)
    return tf_idf


def cosine_similarity(a, b):
    """
    Cosine similarity function
    """
    return dot(a, b)/(norm(a)*norm(b))


def eucl(a, b):
    """
    Euclideian distance function
    """
    return np.linalg.norm(a-b)


def new_score(row):
    """
    Computing the new score
    """
    euclid, hamming, tfidf = row
    euclidean_index = euclid**-1
    hamming_index = np.log(1 + hamming)**-1
    new_score = tfidf * euclidean_index * hamming_index
    
    return new_score   