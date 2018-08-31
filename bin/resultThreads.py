import pandas as pd
import sys
import matplotlib 
import matplotlib.pyplot as plt

df = pd.read_csv("results/resultThreads.csv")

plt.figure(1)
x = df.loc[df['target'] == 'wgla' & df['numThreads' == '5']]
print x 
y1_t = df['throughput']
y1_l = df['latency']
plt.plot(x1, y1_l, 'b.-')
plt.savefig("figures/df.png")
plt.show()

plt.figure(2)
plt.plot(x1, y1_t, 'r--')
plt.show()
