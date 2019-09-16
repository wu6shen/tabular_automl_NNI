# Copyright (c) Microsoft Corporation
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
# to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
# BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import logging
import pandas as pd
import numpy as np 
from sklearn.model_selection import KFold
from sklearn.decomposition import TruncatedSVD
from multiprocessing import Pool
import gc
import time

from const import FeatureType, AGGREGATE_TYPE
from groupby import *
import logging

logger = logging.getLogger('fe_util')

def timmer(func):
    def warpper(*args, **kwargs):
        start_time = time.time()
        r = func(*args, **kwargs)
        stop_time = time.time()
        print("[Success] Info: function: {}() done".format(func.__name__))
        print("the func run time is %.2fs" % (stop_time - start_time))
        return r
    return warpper

def left_merge(data1, data2, on):
    """
    merge util for dataframe
    """
    if type(on) != list:
        on = [on]
    if (set(on) & set(data2.columns)) != set(on):
        data2_temp = data2.reset_index()
    else:
        data2_temp = data2.copy()
    columns = [f for f in data2.columns if f not in on]
    #columns = list(set(data2.columns) - set(on))
    result = []
    result = data1.merge(data2_temp, on = on, how='left')
    result = result[columns]
    return result


def concat(L):
    """
    tools for concat some dataframes into a new dataframe.
    """
    result = None
    for l in L:
        if l is None:
            continue
        if result is None:
            result = l
        else:
            result[l.columns.tolist()] = l
    return result


def name2feature_multi(df, feature_space, target_name='label'):
    assert isinstance(feature_space, list)
    pool = Pool(4)
    commands = []
    params = []
    for key in feature_space:
        temp = key.split('_')
        assert len(temp) > 1

        op_name = temp[0]
        if len(temp) == 2:
            i = temp[1]
            params.append([df.loc[:,[i,target_name]], i])
            command = op_name
        elif len(temp) == 3:
            i, j = temp[1], temp[2]
            params.append([df.loc[:,[i,j,target_name]], [i,j]])
            command = op_name
        elif len(temp) == 4:
            stat, i, j = temp[1], temp[2], temp[3]
            params.append([df.loc[:,[i,j,target_name]], i, j, [stat]])
            command = op_name
        else:
            raise RuntimeError('Do not support this OP: ' + str(key))
        commands.append(command)

    result = []
    for i in range(len(commands)):
        if commands[i] == 'count':
            result.append(pool.apply_async(m_count, params[i]))
        elif commands[i] == 'crosscount':
            result.append(pool.apply_async(m_crosscount, params[i]))
        elif commands[i] == 'aggregate':
            result.append(pool.apply_async(m_aggregate, params[i]))
    pool.close()
    pool.join()
    result = [aresult.get() for aresult in result]
    result.append(df)
    df = concat(result)
    return df


def name2feature(df, feature_space, target_name='label'):
    assert isinstance(feature_space, list)

    logger.info("name feature:\n", feature_space)
    for key in feature_space:
        temp = key.split('_')
        assert len(temp) > 1

        op_name = temp[0]
        if len(temp) == 2:
            i = temp[1]
            command = op_name + '(df, i)'
        elif len(temp) == 3:
            i, j = temp[1], temp[2]
            command = op_name + '(df, [i, j])'
        elif len(temp) == 4:
            stat, i, j = temp[1], temp[2], temp[3]
            command = op_name + '(df, i, j, [stat])'
        else:
            raise RuntimeError('Do not support this OP: ' + str(key))

        df = eval(command)
    
    return df

def m_count(df, col):
    """
    tools for count encode
    """
    df['count_{}'.format(col)] = df.groupby(col)[col].transform('count')
    return df[['count_{}'.format(col)]]


def m_crosscount(df, col_list):
    """
    tools for multy thread bi_count
    """
    assert isinstance(col_list, list)
    assert len(col_list) >= 2
    name = "count_"+ '_'.join(col_list)
    df[name] = df.groupby(col_list)[col_list[0]].transform('count')
    return df[[name]]


def m_aggregate(df, num_col, col, stat_list = AGGREGATE_TYPE):
    agg_dict = {}
    for i in stat_list:
        agg_dict['AGG_{}_{}_{}'.format(i, num_col, col)] = i
    tmp = df.groupby([col])[num_col]
    tmp = pd.concat({k: tmp.transform(v) for k, v in agg_dict.items()}).unstack(0)
    return tmp

@timmer
def count(df, col):
    """
    tools for count encode
    """
    df['count_{}'.format(col)] = df.groupby(col)[col].transform('count')
    return df


@timmer
def crosscount(df, col_list):
    """
    tools for multy thread bi_count
    """
    print(col_list)
    assert isinstance(col_list, list)
    assert len(col_list) >= 2
    name = "count_"+ '_'.join(col_list)
    #df[name] = df.groupby(col_list)[col_list[0]].transform('count')
    df[col_list] = df[col_list].fillna('aaaaaaa')
    import libaa
    print(type(df[col_list[0]].values[0]))
    libaa.func(df[col_list[0]].values)
    #lib.func(df[col_list[0]].values, len(df[col_list[0]]))
    #df[name] = Groupby(df[col_list]).apply(len, df[col_list[0]], broadcast=False)
    #df[name] = pd.DataFrame(Groupby(df[col_list]).apply(len, df[col_list[0]], broadcast=False))
    #df.groupby(col_list)[col_list[0]].transform('count')
    #df[name] = df.groupby(col_list)[col_list[0]].apply('count')
    #print(df[col_list[0]].dtype)
    #a = df[col_list[0]].values + df[col_list[1]].values
    #np.savetxt('in',a,fmt='%s')
    #start_time = time.time()
    #_,r=np.unique(a, return_inverse=True)
    #stop_time = time.time()
    #print(stop_time-start_time)
    #y = np.bincount(r)
    #r = y[r]

    
    return df


@timmer
def aggregate(df, num_col, col, stat_list = AGGREGATE_TYPE):
    print(num_col, col, stat_list)
    agg_dict = {}
    for i in stat_list:
        agg_dict['AGG_{}_{}_{}'.format(i, num_col, col)] = i
    #agg_result = df.groupby([col])[num_col].agg(agg_dict)
    #r = left_merge(df, agg_result, on = [col])
    #df = concat([df, r])
    tmp = df.groupby([col])[num_col]
    tmp = pd.concat({k: tmp.transform(v) for k, v in agg_dict.items()}).unstack(0)
    df = concat([df, tmp])
    return df


def nunique(df, id_col, col):
    """
    get id group_by(id) nunique
    """
    agg_dict = {}
    agg_dict['NUNIQUE_{}_{}'.format(id_col, col)] = 'nunique'
    agg_result = df.groupby([col])[id_col].agg(agg_dict)
    r = left_merge(df, agg_result, on = [col])
    df = concat([df, r])
    return df


def histstat(df, id_col, col, stat_list = AGGREGATE_TYPE):
    """
    get id group_by(id) histgram statitics
    """
    agg_dict = {}
    for i in stat_list:
        agg_dict['HISTSTAT_{}_{}_{}'.format(i, id_col, col)] = i
    df['temp_count'] = df.groupby(id_col)[id_col].transform('count')
    agg_result = df.groupby([col])['temp_count'].agg(agg_dict)
    r = left_merge(df, agg_result, on = [col])
    df = concat([df, r])
    del df['temp_count']
    return df


def base_embedding(x, model, size):
    """
    embedding helper for bagofwords
    """
    vec = np.zeros(size)
    x = [item for item in x if model.wv.__contains__(item)]
    for item in x:
        vec += model.wv[str(item)]
    if len(x) == 0:
        return vec
    else:
        return vec / len(x)


def embedding(df, col):
    """
    This is the tool for multi-categories embedding encode.
    embedding for one single multi-categories column.
    """
    from gensim.models.word2vec import Word2Vec

    input_ = df[col].fillna('NA').apply(lambda x: str(x).split(' '))
    model = Word2Vec(input_, size=12, min_count=2, iter=5, window=5, workers=4)
    data_vec = []
    for row in input_:
        data_vec.append(base_embedding(row, model, size=12))
    svdT = TruncatedSVD(n_components=6)
    data_vec = svdT.fit_transform(data_vec)
    column_names = []
    for i in range(6):
        column_names.append('embedding_{}_{}'.format(col, i))
    data_vec = pd.DataFrame(data_vec, columns=column_names)
    df = pd.concat([df, data_vec], axis=1)
    return df


def add_noise(series, noise_level):
    """
    target encoding smooth
    """
    return series * (1 + noise_level * np.random.randn(len(series)))


def add_smooth(series, p, a = 1):
    """
    target encoding smooth
    """
    return (series.sum() + p / series.count() + a)


def target(df, col, target_name='label'):
    """
    target encoding  using 5 k-fold with smooth

    target_name : surpvised learning task pred target name, y.
    """
    df[col] = df[col].fillna('-9999999')
    mean_of_target = df[target_name].mean()

    kf = KFold(n_splits = 5, shuffle = True, random_state=2019) 
    col_mean_name = "target_{}".format(col)
    X = df[df[target_name].isnull() == False].reset_index(drop=True)
    X_te = df[df[target_name].isnull()].reset_index(drop=True)
    X.loc[:, col_mean_name] = np.nan
    
    for tr_ind, val_ind in kf.split(X):
        X_tr, X_val = X.iloc[tr_ind], X.iloc[val_ind]
        X.loc[df.index[val_ind], col_mean_name] = X_val[col].map(X_tr.groupby(col)[target_name].apply(lambda x: add_smooth(x, 0.5, 1)))

    tr_agg =  X[[col, target_name]].groupby([col])[target_name].apply(lambda x: add_smooth(x, 0.5, 1)).reset_index()
    tr_agg.columns = [col, col_mean_name]

    X_te = X_te.merge(tr_agg, on = [col], how = 'left')
    _s = np.array(pd.concat([X[col_mean_name], X_te[col_mean_name]]).fillna(mean_of_target))
    df[col_mean_name] =  _s
    return df

