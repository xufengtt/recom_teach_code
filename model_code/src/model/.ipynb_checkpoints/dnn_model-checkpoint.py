import torch
import torch.nn as nn 
import torch.nn.functional as F 
from torch.utils.tensorboard import SummaryWriter

class MyModel(nn.Module):
    def __init__(self, config):
        super(MyModel, self).__init__()
        self.config = config
        self.embedding = nn.Embedding(num_embeddings=self.config["num_embedding"], embedding_dim =self.config["embedding_dim"], padding_idx=0)
        self.fc1 = nn.Linear(self.config["embedding_dim"]*len(self.config["feature_col"]), 512)
        self.fc2 = nn.Linear(512,128)
        self.fc3 = nn.Linear(128,1)
        
    def forward(self, features):
        embedding_dict = {}
        for ff in self.config["feature_col"]:
            embedding_dict[ff] = torch.sum(self.embedding(features[ff]), dim=1)
        x = torch.cat([embedding_dict[ff] for ff in self.config["feature_col"]], dim=1)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x
