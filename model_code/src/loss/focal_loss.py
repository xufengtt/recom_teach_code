import torch
import torch.nn as nn 
from torch.nn.functional import binary_cross_entropy_with_logits
class FocalLoss(nn.Module):
    def __init__(self, alpha=0.25, gamma=2.0):
        """
        初始化 Focal Loss
        :param alpha: 平衡正负样本权重
        :param gamma: 调节易分样本的权重，使模型更关注难分样本
        """
        super(FocalLoss, self).__init__()
        self.alpha = alpha
        self.gamma = gamma

    def forward(self, logits, targets):
        probs = torch.sigmoid(logits)

        # 计算交叉熵损失
        ce_loss = binary_cross_entropy_with_logits(logits, targets, reduction='none')

        # 计算Focal Loss的权重项
        pt = torch.where(targets == 1, probs, 1 - probs)  # 预测正确的概率
        focal_weight = (1 - pt) ** self.gamma

        # 结合alpha进行类别平衡
        alpha_t = torch.where(targets == 1, self.alpha, 1 - self.alpha)
        focal_loss_value = alpha_t * focal_weight * ce_loss

        # 如果你的数据集大小不一，可能需要对loss求平均或总和，这里以求平均为例
        return focal_loss_value.mean()    
