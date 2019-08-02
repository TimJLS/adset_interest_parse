#!/usr/bin/env python
# coding: utf-8

# In[3]:


from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# In[ ]:


fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.scatter(df['X'], df['Y'], df['Z'], c='skyblue', s=60)
ax.view_init(30, 185)
plt.show()


# In[2]:


from sklearn.svm import OneClassSVM
import numpy as np
# import plotly.offline as py
# import plotly.graph_objs as go
# py.init_notebook_mode(connected=True)

# 导入数据
data = np.loadtxt('https://raw.githubusercontent.com/ffzs/dataset/master/outlier.txt', delimiter=' ')
data

