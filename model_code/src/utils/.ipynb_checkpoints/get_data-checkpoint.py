import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import numpy as np
import os
from odps import ODPS
from odps.df import DataFrame
from torch.nn.utils.rnn import pad_sequence
from config.dnn_config import config as dnn_config
import torch

def get_data(ak_config):
    o = ODPS(
        ak_config["access_id"],
        ak_config["access_key"],
        ak_config["project"],
        ak_config["endpoint"],
    )

    # 读取数据。
    sql = '''
    SELECT *
    FROM recom_alg_dev.user_pay_sample_feature_join_dnn_seq_shuffle
    ;
    '''
    print(sql)
    query_job = o.execute_sql(sql)
    result = query_job.open_reader(tunnel=True)
    df = result.to_pandas(n_process=10) #n_process配置可参考机器配置，取值大于1时可以开启多线程加速。
    print('read data finish')
    # 删除非特征列
    df = df.drop(columns=['key_all'])

    # 分离特征和标签
    X = df.drop(columns='label')
    y = df['label']

    # 划分训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    feature_col = dnn_config["feature_col"]
    train_feature_numpy = {}
    test_feature_numpy = {}
    for feature in feature_col:
        train_feature_numpy[feature] = X_train[feature].values
        test_feature_numpy[feature] = X_test[feature].values
    train_label = y_train.values
    test_label = y_test.values
    return train_feature_numpy,test_feature_numpy,train_label,test_label

def calculate_top_k_ratio(predictions, labels, top_k_list):
    # 将预测和标签转换为DataFrame
    results_df = pd.DataFrame({'prediction': predictions, 'label': labels})
    
    # 按预测分数降序排序
    results_df = results_df.sort_values(by='prediction', ascending=False)
    
    # 计算总正例数
    total_positives = (results_df['label'] == 1).sum()
    
    # 计算不同top数量下的正例比例
    ratios = {}
    for k in top_k_list:
        top_k_df = results_df.head(k)
        top_k_positives = (top_k_df['label'] == 1).sum()
        ratio = top_k_positives / total_positives
        ratios[k] = ratio
    
    return ratios

def get_data_test(ak_config, brand_id):
    o = ODPS(
        ak_config["access_id"],
        ak_config["access_key"],
        ak_config["project"],
        ak_config["endpoint"],
    )

    # 读取数据。
    sql = '''
    SELECT *
    FROM recom_alg_dev.user_pay_sample_feature_join_eval_dnn_seq
    where keys_all = '{brand_id}'
    ;
    '''.format(brand_id=brand_id)
    print(sql)
    query_job = o.execute_sql(sql)
    result = query_job.open_reader(tunnel=True)
    df = result.to_pandas(n_process=10) #n_process配置可参考机器配置，取值大于1时可以开启多线程加速。
    print('read data finish')
    # 删除非特征列
    df = df.drop(columns=['keys_all'])

    # 分离特征和标签
    X = df.drop(columns='label')
    y = df['label']

    # 划分训练集和测试集
    feature_col = dnn_config["feature_col"]
    test_feature_numpy = {}
    for feature in feature_col:
        test_feature_numpy[feature] = X[feature].values
    test_label = y.values
    return test_feature_numpy,test_label

def get_data_test_moe(ak_config):
    o = ODPS(
        ak_config["access_id"],
        ak_config["access_key"],
        ak_config["project"],
        ak_config["endpoint"],
    )

    # 读取数据。
    sql = '''
    SELECT *
    FROM recom_alg_dev.user_pay_sample_feature_join_dnn_seq_shuffle limit 3000
    ;
    '''
    print(sql)
    query_job = o.execute_sql(sql)
    result = query_job.open_reader(tunnel=True)
    df = result.to_pandas(n_process=10) #n_process配置可参考机器配置，取值大于1时可以开启多线程加速。
    print('read data finish')
    # 删除非特征列
    df = df.drop(columns=['key_all'])

    # 分离特征和标签
    X = df.drop(columns='label')
    y = df['label']

    # 划分训练集和测试集
    feature_col = dnn_config["feature_col"]
    test_feature_numpy = {}
    for feature in feature_col:
        test_feature_numpy[feature] = X[feature].values
    test_label = y.values
    return test_feature_numpy,test_label


def my_collate_fn(batch):
    res_features_tmp = {}
    labels = []
    for ff in dnn_config["feature_col"]:
        res_features_tmp[ff] = []
    for sample in batch:
        for ff in dnn_config["feature_col"]:
            res_features_tmp[ff].append(sample[ff])
        labels.append(sample["labels"])
    res_feature = {}
    for ff in dnn_config["feature_col"]:
        res_feature[ff] = pad_sequence(res_features_tmp[ff], batch_first=True, padding_value=0)
    return res_feature, torch.tensor(labels)

def seq_collate_fn(batch):
    res_features_tmp = {}
    labels = []
    for ff in dnn_config["feature_col"]:
        res_features_tmp[ff] = []
    for sample in batch:
        for ff in dnn_config["feature_col"]:
            res_features_tmp[ff].append(sample[ff])
        labels.append(sample["labels"])
    res_feature = {}
    res_mask = {}
    for ff in dnn_config["feature_col"]:
        res_feature[ff] = pad_sequence(res_features_tmp[ff], batch_first=True, padding_value=0)
        res_mask[ff] = (res_feature[ff] != 0).type(torch.float32)
    return res_feature, res_mask, torch.tensor(labels)