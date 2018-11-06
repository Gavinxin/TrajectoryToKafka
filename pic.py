import matplotlib.pyplot as plt
import pandas as pd

data = pd.read_table(r"C:\Users\Gavin\Desktop\test\per1.txt", encoding='utf-8', sep=' ', engine='python',
                     names=['window_time', 'number', 'delay'], index_col=None)
# for i,row in data.iterrows():
#     row['time']=(1+i)*5
lis = pd.DataFrame(data, columns=['number'])
print(lis)
y1 = pd.DataFrame(data, columns=['number'])
x1 = range(0, 70, 5)
x2 = pd.DataFrame(data, columns=['time'])
y2 = pd.DataFrame(data, columns=['number'])
plt.plot(x1, y1, label='number/5s', color='r', marker='o',
         markerfacecolor='blue', markersize=12)

plt.xlabel(' time')
plt.ylabel('number')
plt.title('Flink Performance')
plt.legend()
plt.show()
