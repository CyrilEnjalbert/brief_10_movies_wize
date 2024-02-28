from pymongo import MongoClient
import pandas as pd


# Connecting with MongoDB
def mongo_connect(host='localhost', port=27017, db_name='Movielens', users_col_name='users', movies_col_name='movies'):
    # Connecting to the DB
    client = MongoClient(host, port)
    db = client[db_name]

    # Connecting to the different collections
    users_collection = db[users_col_name]
    movies_collection = db[movies_col_name]
    
    movies = movies_collection.find()
    users = users_collection.find()

    return users, movies


# Creating the main dataframe from MongoDB data
def import_dataset(host, port, db_name):
    data = []
    users = mongo_connect(host, port, db_name)[0]
    
    for user in users:
        user_id = user['_id']
        for movie in user['movies']:
            data.append({
                'user_id': user_id,
                'movie_id': movie['movieid'],
                'rating': movie['rating'],
                'timestamp': movie['timestamp']
            })

    df = pd.DataFrame(data)

    return df


# Cleaning the dataframe
def clean_whole_df(df):
    cleaned_df = df.loc[df.sum(axis=1) > 0] # Deleting users that didn't rate any movie  
    return cleaned_df
    
def clean_test_df(train_df, test_df):
    """
    Clean les data_frames "test_df" et "train_df" (mêmes index et colonnes), et rassemble les deux dans un seul dataframe: ' df_test_filtered'

    Args:
    train_df (DataFrame): DataFrame du test.
    test_df (DataFrame): DataFrame du train.
    
    """

    movies_train = set(train_df['movie_id']) 
    movies_test = set(test_df['movie_id']) 
    movies_common = movies_train.intersection(movies_test)
    movies_common_list = list(movies_common)

    users_train = set(train_df['user_id'])
    users_test = set(test_df['user_id'])
    users_common = users_train.intersection(users_test)
    users_common_list = list(users_common)

    df_test_filtered_user = test_df[test_df['user_id'].isin(users_common_list)]
    df_test_filtered = df_test_filtered_user[df_test_filtered_user['movie_id'].isin(movies_common_list)]

    return df_test_filtered

def filter_df(merged_df,
                          movies_threshold=35, 
                          users_threshold=45, 
                          min_mean_rating=1.5, 
                          max_mean_rating=4.5, 
                          movies_few_notes=True, 
                          users_few_notes=True, 
                          users_no_discriminating=True,
                          users_constant_dt=True):

    # Filtrer selon les différents critères
    if movies_few_notes:
        # Compter le nombre de ratings par film
        movies_counts = merged_df['movie_id'].value_counts()
        print('Nombre de ratings par film :')
        print(movies_counts.describe())
        print('\n')
        # Filtrer les films ayant un nombre de ratings supérieur au seuil
        merged_df = merged_df[merged_df['movie_id'].isin(movies_counts[movies_counts > movies_threshold].index)]

    if users_few_notes:
        # Compter le nombre de ratings par utilisateur
        users_counts = merged_df['user_id'].value_counts()
        print('Nombre de ratings par utilisateur :')
        print(users_counts.describe())
        print('\n')
        # Filtrer les utilisateurs ayant un nombre de ratings supérieur au seuil
        merged_df = merged_df[merged_df['user_id'].isin(users_counts[users_counts > users_threshold].index)]

    if users_no_discriminating:
        # Filtrer les utilisateurs basés sur la note moyenne
        merged_df = merged_df.groupby('user_id').filter(lambda x: x['rating'].mean() > min_mean_rating and x['rating'].mean() < max_mean_rating)

    if users_constant_dt:
        # Éliminer les ratings déposés au même moment par le même utilisateur
        merged_df = merged_df.drop_duplicates(subset=['user_id', 'timestamp'], keep=False)

    return merged_df
