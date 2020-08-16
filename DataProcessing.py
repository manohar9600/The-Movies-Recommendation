# Standard imports
import json
import ast

# Third party imports
import pandas as pd
from tabulate import tabulate

# Local application imports
from utils.logger import logger


class Dataloding:
    """Loads movie lens and TMDB data from data folder.
    """
    
    def __init__(self, data_folder='data'):
        self.data_folder = data_folder.strip('/')
        self.load_data()

    def load_data(self) -> None:
        ratings_file_path = self.data_folder + '/ratings.csv'
        self.ratings_df = pd.read_csv(ratings_file_path)
        logger.info("ratings: ")
        logger.info(tabulate(self.ratings_df.head(), headers='keys', 
                             tablefmt='pretty'))
        logger.info("successfully loaded ratings. entries: %s" % \
            self.ratings_df.shape[0])

        movies_data_path = self.data_folder + '/movies_metadata.csv'
        self.movies_df = pd.read_csv(movies_data_path)
        self.movies_df = self.transform_movies_df(self.movies_df)
        logger.info("successfully loaded movies metadata. entries: %s" % \
            self.movies_df.shape[0])
        
        keywords_data_path = self.data_folder + '/keywords.csv'
        self.keywords_df = pd.read_csv(keywords_data_path)
        self.keywords_df = self.transform_keywords_df(self.keywords_df)
        logger.info("successfully loaded movie keywords data. entries: %s" \
            % self.keywords_df.shape[0])

        links_data_path = self.data_folder + '/links.csv'
        self.links_df = pd.read_csv(links_data_path)
        logger.info("movie links: ")
        logger.info(tabulate(self.links_df.head(), headers='keys', 
                             tablefmt='pretty'))
        logger.info("successfully loaded movie links data. entries: %s" \
            % self.links_df.shape[0])

        credits_data_path = self.data_folder + '/credits.csv'
        self.credits_df = pd.read_csv(credits_data_path)
        self.credits_df = self.transform_credits_df(self.credits_df)
        logger.info("successfully loaded credits data. entries: %s" \
            % self.credits_df.shape[0])
        
        logger.info("successfully loaded all data")

    def transform_movies_df(self, movies_df) -> pd.DataFrame:
        """Converts non strings like jsons or other data types to string or list.
        and also minimizes data size.

        Args:
            movies (DataFrame): movies data in df format

        Returns:
            DataFrame: dataframe with better data structures.
        """
        self.id_collection = {}
        self.id_genre = {}
        for index, row in movies_df.iterrows():
            if not pd.isna(row['belongs_to_collection']) and \
                    row['belongs_to_collection'].strip():
                collection_str = row['belongs_to_collection']
                collection_json = ast.literal_eval(collection_str)
                movies_df.loc[index, 'belongs_to_collection'] = \
                    collection_json['id']
                self.id_collection[collection_json['id']] = \
                    collection_json['name']
            else:
                movies_df.loc[index, 'belongs_to_collection'] = -1
            
            if not pd.isna(row['genres']) and \
                    row['genres'].strip():
                genres_str = row['genres']
                genres_list = ast.literal_eval(genres_str)
                movies_df.at[index, 'genres'] = [g['id'] for g in genres_list]
                for genre in genres_list:
                    self.id_genre[genre['id']] = genre['name']
            else:
                movies_df.loc[index, 'genres'] = []
        return movies_df
    
    def transform_keywords_df(self, keywords_df) -> pd.DataFrame:
        """Converts keywords data in json format to list format.
        storing only ids in keywords_df and separate dictionary for mappings

        Args:
            keywords_df (pd.DataFrame): raw keywords data

        Returns:
            pd.DataFrame: transformed dataframe
        """
        self.id_keyword = {}
        for index, row in keywords_df.iterrows():
            keywords_json = row['keywords']
            keyword_ids = []
            if keywords_json.strip():
                keywords_json = ast.literal_eval(keywords_json)
                for key in keywords_json:
                    keyword_ids.append(key['id'])
                    self.id_keyword[key['id']] = key['name']
            keywords_df.at[index, 'keywords'] = keyword_ids
        return keywords_df

    def transform_credits_df(self, credits_df) -> pd.DataFrame:
        """Converts json format in df to list format. Stores only ids in df 
        and ids mapping will be self.id_credit(dict)

        Args:
            credits_df (pd.DataFrame): raw credits data

        Returns:
            pd.DataFrame: transformed data
        """
        self.id_credit = {}
        for index, row in credits_df.iterrows():
            cast_json = row['cast']
            if cast_json.strip():
                cast_json = ast.literal_eval(cast_json)
                cast_ids = []
                for cast in cast_json:
                    self.id_credit[cast['id']] = cast['name']
                    cast_ids.append(cast['id'])
                credits_df.at[index, 'cast'] = cast_ids
                
            crew_json = row['crew']
            if crew_json.strip():
                crew_json = ast.literal_eval(crew_json)
                credits_df.at[index, 'crew'] = crew_json
                
        return credits_df


class DataProcessing:

    def __init__(self) -> None:
        pass

if __name__ == '__main__':
    data = Dataloding()
