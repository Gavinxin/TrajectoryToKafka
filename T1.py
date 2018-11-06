import pandas as pd
import matplotlib.pyplot as plt
import time
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
data = pd.read_table(r"C:\Users\Gavin\Desktop\T-drive Taxi Trajectories\release\taxi_log_2008_by_id\1.txt",encoding='utf-8',sep=",",engine='python',names=['id','时间','经度','纬度'])
df = pd.DataFrame(data)
print(df.shape)
df['时间'] = pd.to_datetime(df['时间'])
print(df.dtypes)
print('---------------------')
print(df.iloc[3])
df=df.set_index('时间')
print(df.iloc[2])
lng = pd.DataFrame(data,columns = ['经度'])
lat = pd.DataFrame(data,columns = ['纬度'])
time = pd.DataFrame(data,columns = ['时间'])
print(time)
# 生成画布、3D图形对象、三维散点图
fig = plt.figure()
ax = Axes3D(fig)
ax.scatter(lng,lat,time)

# 设置坐标轴显示以及旋转角度
ax.set_xlabel('经度')
ax.set_ylabel('纬度')
ax.set_zlabel('时间')

plt.show()
p