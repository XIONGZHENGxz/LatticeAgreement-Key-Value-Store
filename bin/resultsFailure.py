import pandas as pd
import sys
import matplotlib 
import matplotlib.pyplot as plt

df = pd.read_csv("results/resultsFailure.csv")
df_wgla = df.loc[df['target'] == 'wgla']
df_jp = df.loc[df['target'] == 'jpaxos']
plt.figure(1)
x = df_wgla.groupby(['numThreads']).groups.keys() 
print x
x.sort()
y_w = []
y_r = []
y_t = []
y_t_jp = []
y_l_jp = []

for t in x:
	dd = df_wgla.loc[df_wgla['numThreads'] == t]
	dd_jp = df_jp.loc[df_jp['numThreads'] == t]
	y_t_mean = dd['throughput'].mean()
	print y_t_mean
	y_wl_mean = dd['writeLatency'].mean()
	y_rl = list(dd['readLatency'])
	y_rl = map(float, y_rl)
	y_rl_mean = reduce(lambda x, y: x + y, y_rl) / len(y_rl)
	y_w.append(y_wl_mean)
	y_r.append(y_rl_mean)
	y_t.append(y_t_mean)
	y_t_jp_mean = dd['throughput'].mean()
	y_l_jp_mean = dd_jp['writeLatency'].mean()
	y_l_jp.append(y_l_jp_mean)
	y_t_jp.append(y_t_jp_mean)

plt.plot(x, y_t, 'b+-' , x, y_t_jp, 'r*-')
plt.savefig("figures/threadsTh.png")
plt.show()

plt.figure(2)
plt.plot(x, y_w, 'r*-', x, y_r, 'b+-', x, y_l_jp, 'rs-')
plt.savefig("figures/threadsLa")
plt.show()
