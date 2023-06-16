import pandas as pd
import numpy
import scipy
import csv
from pdfminer.high_level import extract_text

def read_pdf():
    #Function to parse pdf into a list of words 
    text = extract_text("pdf1.pdf")
    print(text)

    return text.split()


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


def annotate():
text = read_pdf()

assay, tissue, tumorType = create_dict()

l = []
for word in text: 
    if word in assay:
        l.append(word)

print(l)









