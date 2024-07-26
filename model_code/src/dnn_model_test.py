import pandas as pd
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import numpy as np
import os
from odps import ODPS
from odps.df import DataFrame
from torch.utils.data import Dataset, DataLoader
from torch.nn.utils.rnn import pad_sequence
import torch
import torch.nn as nn 
import torch.nn.functional as F 
from torch.utils.tensorboard import SummaryWriter

from dataset.dnn_dataset import MyDataset,MyPriorDataset
from model.dnn_model import MyModel
from config.ak_config import config as ak_config
from config.dnn_config import config as dnn_config
from utils.get_data import get_data,get_data_test
from utils.get_data import calculate_top_k_ratio
from utils.get_data import my_collate_fn

brands = ['b47686','b56508','b62063','b78739']
for brand_id in brands:
    #brand_id='b56508'
    model_path = './models/focal2/model_epoch_1_27999.pth'
    # 需要计算top数量
    top_k_list = [1000, 3000, 5000, 10000, 50000]

    test_feature_numpy,test_label = get_data_test(ak_config, brand_id)
    dataset_test = MyPriorDataset(test_feature_numpy, test_label, dnn_config)

    dataloader_test = DataLoader(dataset_test, batch_size=dnn_config["batch_size"], shuffle=False, collate_fn=my_collate_fn)

    model = MyModel(dnn_config).to('cpu')
    model.load_state_dict(torch.load(model_path))
    model.to('cpu')
    model.eval()
    test_preds = []
    test_targets = []
    for data, target in dataloader_test:
        output  = model(data)
        test_preds.extend(output.sigmoid().squeeze().tolist())
        test_targets.extend(target.squeeze().tolist())

    # 计算top k的正例比例
    ratios = calculate_top_k_ratio(test_preds, test_targets, top_k_list)
    # 输出结果
    for k, ratio in ratios.items():
        print(f"Top {k} ratio of positive labels: {ratio:.4f}")
    # 如果需要保存结果到文件
    with open(f'models_{brand_id}_top_results_focal.txt', 'w') as f:
        for k, ratio in ratios.items():
            f.write(f"Top {k} ratio of positive labels: {ratio:.4f}\n")

