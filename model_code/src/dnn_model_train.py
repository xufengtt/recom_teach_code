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
from model.dnn_model import MyModel
from config.ak_config import config as ak_config
from config.dnn_config import config as dnn_config
from utils.get_data import get_data
from utils.get_data import my_collate_fn

train_feature_numpy,test_feature_numpy,train_label,test_label = get_data(ak_config)
dataset_train = MyPriorDataset(train_feature_numpy, train_label,dnn_config)
dataset_test = MyPriorDataset(test_feature_numpy, test_label, dnn_config)

print('dataset finish')
dataloader_train = DataLoader(dataset_train, batch_size=dnn_config["batch_size"], shuffle=False, collate_fn=my_collate_fn)
dataloader_test = DataLoader(dataset_test, batch_size=dnn_config["batch_size"], shuffle=False, collate_fn=my_collate_fn)
print('dataloader finish')

#device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = MyModel(dnn_config)
criterion = nn.BCEWithLogitsLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.004)
writer = SummaryWriter()

def train_model(train_loader, test_loader, model, criterion, optimizer, num_epochs=2):
    total_step = 0
    for epoch in range(num_epochs):
        model.train()
        for features,labels in train_loader:
            labels = labels
            optimizer.zero_grad()
            outputs = model(features)
            labels = torch.unsqueeze(labels,dim=1)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            total_step += 1
            if (total_step+1)%10 == 0:
                writer.add_scalar('Training Loss', loss.item(), total_step)
            if (total_step+1)%100 == 0:
                print(f'Epoch {epoch}, Step {total_step}: Loss={loss.item(): .4f}')
            if (total_step+1)%2000 == 0:
                with torch.no_grad():
                    model.eval()
                    test_preds = []
                    test_targets = []
                    for data, target in test_loader:
                        output  = model(data)
                        test_preds.extend(output.to('cpu').sigmoid().squeeze().tolist())
                        test_targets.extend(target.squeeze().tolist())
                    test_auc = roc_auc_score(test_targets, test_preds)
                    writer.add_scalar('AUC/train', test_auc, total_step)
                torch.save(model.state_dict(), f'models/dnn2/model_epoch_{epoch}_{total_step}.pth')
                model.train()
        torch.save(model.state_dict(), f'models/dnn2/model_epoch_{epoch}.pth')
    with torch.no_grad():
        model.eval()
        test_preds = []
        test_targets = []
        for data, target in test_loader:
            output  = model(data)
            test_preds.extend(output.to('cpu').sigmoid().squeeze().tolist())
            test_targets.extend(target.squeeze().tolist())
        test_auc = roc_auc_score(test_targets, test_preds)
        writer.add_scalar('AUC/train', test_auc, total_step)
        
train_model(dataloader_train, dataloader_test, model, criterion, optimizer)
writer.close()