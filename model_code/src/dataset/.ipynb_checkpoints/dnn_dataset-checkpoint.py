import torch
import torch.nn as nn 
import torch.nn.functional as F 
from torch.utils.data import Dataset, DataLoader, IterableDataset

class MyDataset(Dataset):
    def __init__(self, features, labels, config):
        self.config = config
        self.features = {}
        for ff in self.config["feature_col"]:
            self.features[ff] = [torch.tensor([int(id) for id in seq.split(',')], dtype=torch.long) for seq in features[ff]]
        self.labels = torch.tensor(labels, dtype=torch.float32)
        
    def __len__(self):
        return len(self.labels)
        
    def __getitem__(self, idx):
        res_features = {}
        for ff in self.config["feature_col"]:
            res_features[ff] = self.features[ff][idx]
        res_features['labels'] = self.labels[idx]
        return res_features
    
class MyPriorDataset(Dataset):
    def __init__(self, features, labels, config):
        self.config = config
        self.features = features
        self.labels = labels
        
    def __len__(self):
        return len(self.labels)
        
    def __getitem__(self, idx):
        res_features = {}
        for ff in self.config["feature_col"]:
            res_features[ff] = torch.tensor([int(id) for id in self.features[ff][idx].split(',')], dtype=torch.long)
        res_features['labels'] = torch.tensor(self.labels[idx], dtype=torch.float32)
        return res_features
    
class MyIterDataset(IterableDataset):
    def __init__(self, df):
        super(CustomIterableDataset).__init__()
        self.df = df
        
    def __iter__(self):
        for index, row in self.df.iterrows():
            yield {
                'features': {col: list(map(int, row[col].split(','))) for col in feature_columns if col != 'label'},
                'label': int(row['label'])
            }