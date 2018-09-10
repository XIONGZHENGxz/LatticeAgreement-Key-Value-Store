import pandas as pd
import sys
import matplotlib 
import matplotlib.pyplot as plt

df = pd.read_csv("results/resultsLoads.csv")

plt.figure(1)
x = df.groupby(['readsRatio']).groups.keys() 
x.sort()
y_w = []
y_r = []
y_t = []
for t in x:
	dd = df.loc[df['readsRatio'] == t]
	y_t_mean = dd['throughput'].mean()
	y_wl_mean = dd['writeLatency'].mean()
	y_rl_mean = dd['readLatency'].mean()
	y_w.append(y_wl_mean)
	y_r.append(y_rl_mean)
	y_t.append(y_t_mean)

plt.plot(x, y_t, 'bs--')
plt.ylabel('Requests/sec')
plt.xlabel('Reads Ratio')
plt.savefig("figures/loadsTh.png")
plt.show()

plt.figure(2)
plt.plot(x, y_w, 'bo--', x, y_r, 'bs--')
plt.ylabel('Latency(ms)')
plt.xlabel('Reads Ratio')
plt.legend(('write', 'read'))
plt.savefig("figures/loadsLa")
plt.show()
