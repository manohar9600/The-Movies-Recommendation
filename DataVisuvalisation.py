#%%
# Third party imports


# Local application imports
from DataProcessing import Dataloding, DataProcessing

#%%
class DataVisuvalisaion:

    def plot_genre_ratings(self, ratings, movies_df, id_genres):
        genre_rating = self.get_genre_rating(ratings, movies_df, id_genres)
        
    
    def get_genre_rating(self, ratings, movies_df, id_genres):
        genreid_ratings = {}
        for _, rating in ratings:
            movie_info = movies_df[movies_df['id'] == rating['movieId']].iloc[0]
            for genre in movie_info['genres']:
                if genre not in genreid_ratings:
                    genreid_ratings[genre] = []
                genreid_ratings[genre].append(rating['rating'])
        
        genre_avg_rating = {}
        for id in genreid_ratings:
            rating = sum(genreid_ratings[id]) / len(genreid_ratings[id])
            genre_avg_rating[id_genres[id]] = rating
        return genre_avg_rating

#%%
data_path = 'data'
data = Dataloding(data_path)
#%%
visualisation = DataVisuvalisaion()
#%%



# %%
