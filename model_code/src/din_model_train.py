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

from dataset.dnn_dataset import MyDataset
from dataset.dnn_dataset import MyPriorDataset
from model.din_model import DinModel
from config.ak_config import config as ak_config
from config.dnn_config import config as dnn_config
from utils.get_data import get_data, calculate_top_k_ratio, get_data_test
from utils.get_data import my_collate_fn, seq_collate_fn
from loss.focal_loss import FocalLoss

train_feature_numpy,test_feature_numpy,train_label,test_label = get_data(ak_config)
dataset_train = MyPriorDataset(train_feature_numpy, train_label,dnn_config)
print('train dataset finish')
dataloader_train = DataLoader(dataset_train, batch_size=dnn_config["batch_size"], shuffle=False, collate_fn=seq_collate_fn)
print('train dataloader finish')

brands = ['b47686','b56508','b62063','b78739']
dataset_test_dict = {}
dataloader_test_dict = {}
for brand_id in brands: 
    test_feature_numpy,test_label = get_data_test(ak_config, brand_id)
    dataset_test_dict[brand_id] = MyPriorDataset(test_feature_numpy, test_label, dnn_config)
    dataloader_test_dict[brand_id] = DataLoader(dataset_test_dict[brand_id], batch_size=2048, shuffle=False, collate_fn=seq_collate_fn)
print('test data finish')

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = DinModel(dnn_config).to(device) 
criterion = nn.BCEWithLogitsLoss()
#criterion = FocalLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.004)
log_dir = './runs_din4'
writer = SummaryWriter(log_dir)

def train_model(train_loader, test_loader_dict, model, criterion, optimizer, num_epochs=3):
    total_step = 0
    for epoch in range(num_epochs):
        model.train()
        for features,mask,labels in train_loader:
            for ff in dnn_config["feature_col"]:
                features[ff] = features[ff].to(device)
                mask[ff] = mask[ff].to(device)
            labels = labels.to(device) 
            optimizer.zero_grad()
            outputs = model(features,mask)
            labels = torch.unsqueeze(labels,dim=1)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            total_step += 1
            if (total_step+1)%10 == 0:
                writer.add_scalar('Training Loss', loss.item(), total_step)
            if (total_step+1)%100 == 0:
                print(f'Epoch {epoch}, Step {total_step}: Loss={loss.item(): .4f}')
            if (total_step+1)%7000 == 0:
                with torch.no_grad():
                    model.eval()
                    for brand_id in brands:
                        #brand_id='b56508'
                        # 需要计算top数量
                        top_k_list = [1000, 3000, 5000, 10000, 50000]
                        test_preds = []
                        test_targets = []
                        for data,mask, target in test_loader_dict[brand_id]:
                            output  = model(data,mask)
                            test_preds.extend(output.sigmoid().squeeze().tolist())
                            test_targets.extend(target.squeeze().tolist())

                        # 计算top k的正例比例
                        ratios = calculate_top_k_ratio(test_preds, test_targets, top_k_list)
                        # 输出结果
                        for k, ratio in ratios.items():
                            print(f"{brand_id} Top {k} ratio of positive labels: {ratio:.4f}")
                        # 如果需要保存结果到文件
                        with open(f'{log_dir}/models_{brand_id}_top_results_din_{total_step}.txt', 'w') as f:
                            for k, ratio in ratios.items():
                                f.write(f"Top {k} ratio of positive labels: {ratio:.4f}\n")
                torch.save(model.state_dict(), f'models/din4/model_epoch_{epoch}_{total_step}.pth')
                model.train()
        torch.save(model.state_dict(), f'models/din4/model_epoch_{epoch}.pth')
    with torch.no_grad():
        model.eval()
        for brand_id in brands:
            #brand_id='b56508'
            # 需要计算top数量
            top_k_list = [1000, 3000, 5000, 10000, 50000]
            test_preds = []
            test_targets = []
            for data,mask, target in test_loader_dict[brand_id]:
                output  = model(data,mask)
                test_preds.extend(output.sigmoid().squeeze().tolist())
                test_targets.extend(target.squeeze().tolist())

            # 计算top k的正例比例
            ratios = calculate_top_k_ratio(test_preds, test_targets, top_k_list)
            # 输出结果
            for k, ratio in ratios.items():
                print(f"{brand_id} Top {k} ratio of positive labels: {ratio:.4f}")
            # 如果需要保存结果到文件
            with open(f'{log_dir}/models_{brand_id}_top_results_din_{total_step}.txt', 'w') as f:
                for k, ratio in ratios.items():
                    f.write(f"Top {k} ratio of positive labels: {ratio:.4f}\n")
        
train_model(dataloader_train, dataloader_test_dict, model, criterion, optimizer)
writer.close()