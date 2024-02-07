import pandas as pd
import numpy
import scipy
import csv
import glob
from pdfminer.high_level import extract_text

def read_pdf(filepath):
    #Function to read all pdf files in a folder and parse them into a list of words
    files = glob.glob(filepath)
    
    text_list = []

    for f in files: 
        text = extract_text("pdf1.pdf")
        text = text.split()
        text_list.append(text)

    return text_list


def create_dict():
    #Function to read standard terms based on category (assay, tissue, tumorType) into lists 
    filename ="standard_terms.csv"
    
    df = pd.read_csv("standard_terms.csv")
    assay = []
    tissue = []
    tumorType = []

    assay = df.where(df['category'] == 'assay')['valid_value'].dropna().tolist()
    tissue = df.where(df['category'] == 'tissue')['valid_value'].dropna().tolist()
    tumorType = df.where(df['category'] == 'tumorType')['valid_value'].dropna().tolist()

    return assay, tissue, tumorType


#def annotate():
path = r'./*.pdf'
text = read_pdf(path)

assay, tissue, tumorType = create_dict()


#Matching specific terms in paper to existing list of standard terms 

l = []
for word in text: 
    if word in assay:
        l.append(word)

print(l)









