import pandas as pd
import sys
import matplotlib 
import matplotlib.pyplot as plt

name = sys.argv[1]

df = pd.read_csv("results.csv")

x1 = df[name]
print (x1)

plt.figure(1)
y1_t = df['throughput']
y1_l = df['latency']
plt.plot(x1, y1_l, 'b.-')
plt.savefig("figures/df.png")
plt.show()

plt.figure(2)
plt.plot(x1, y1_t, 'r--')
plt.show()
