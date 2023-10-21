# import packages
import pandas as pd


def run_etl():
    data = pd.read_csv('tweets.csv')
    data.to_csv('output_tweets.csv')
