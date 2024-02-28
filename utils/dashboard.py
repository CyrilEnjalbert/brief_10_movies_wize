import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error, ndcg_score

options = {
    "use_train": True,
    "mse": True,
    "top_10": True,
    "bottom_10": True,
    "n" : 10,
    "ndcg" : True
}


# def rate(predict_df, train_df, cleaned_test_df, options):
def rate(predict_df, train_df, cleaned_test_df, options):
    compare_df = pd.merge(predict_df, cleaned_test_df, how='inner', on=['user_id', 'movie_id'])
    sorted_df = compare_df.sort_values(by=['user_id', 'predict'], ascending=[True, False]).groupby('user_id')
    print(sorted_df)
    if options["mse"] :
        mse_test = mean_squared_error(compare_df['rating'], compare_df['predict'])
        delta_mse_test = np.sqrt(mse_test)
        return delta_mse_test, mse_test
    if options["top_10"]:
        average_top_rating_test = (sorted_df.head(10))['rating'].mean()
        diff_top_rating_5_test = 5 - average_top_rating_test
        return diff_top_rating_5_test
    if options["bottom_10"]:
        average_worse_rating_test = (sorted_df.tail(10))['rating'].mean()
        diff_worse_rating_2_test = average_worse_rating_test - 2
        return diff_worse_rating_2_test
    if options["ndcg"]:
        ndcg_test = ndcg_score([compare_df['rating'].values], [compare_df['predict'].values])
        return ndcg_test
    if options["use_train"]:
        compare_df = pd.merge(predict_df, train_df, how='inner', on=['user_id', 'movie_id'])
        sorted_df = compare_df.sort_values(by=['user_id', 'predict'], ascending=[True, False]).groupby('user_id')
        if options["mse"]:
            mse_train = mean_squared_error(compare_df['rating'], compare_df['predict'])
            delta_mse_train = np.srqt(mse_train)
            return delta_mse_train, mse_train
        if options["top_10"]:
            average_top_rating_train = (sorted_df.head(10))['rating'].mean()
            diff_top_rating_5_train = 5 - average_top_rating_train
            return diff_top_rating_5_train
        if options["bottom_10"]:
            average_worse_rating_train = (sorted_df.tail(10))['rating'].mean()
            diff_worse_rating_2_train = average_worse_rating_train- 2
            return diff_worse_rating_2_train
        if options["ndcg"]:
            ndcg_train = ndcg_score([train_df['rating'].values], [predict_df['predict'].values])
            return ndcg_train
    
        
