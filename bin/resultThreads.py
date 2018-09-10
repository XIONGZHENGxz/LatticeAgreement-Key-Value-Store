import pandas as pd
import sys
import matplotlib 
import matplotlib.pyplot as plt

df = pd.read_csv("results/resultsThreadsWR.csv")
df_wgla = df.loc[df['target'] == 'wgla']
df_jp = df.loc[df['target'] == 'jpaxos']
plt.figure(1)
x = df_wgla.groupby(['numThreads']).groups.keys() 
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
	y_wl_mean = dd['writeLatency'].mean()
	y_rl = list(dd['readLatency'])
	y_rl = map(float, y_rl)
	y_rl_mean = reduce(lambda x, y: x + y, y_rl) / len(y_rl)
	y_w.append(y_wl_mean)
	y_r.append(y_rl_mean)
	y_t.append(y_t_mean)
	y_t_jp_mean = dd_jp['throughput'].mean()
	y_l_jp_mean = dd_jp['writeLatency'].mean()
	y_l_jp.append(y_l_jp_mean)
	y_t_jp.append(y_t_jp_mean)

x = [e * 5 for e in x]
plt.plot(x, y_t, 'bo--' , x, y_t_jp, 'rs--')
plt.ylabel('Requests/sec')
plt.xlabel('#clients')
plt.xlim([0, 2000])
plt.legend(('GLA', 'SPaxos'))
plt.savefig("figures/threadsTh.png")
plt.show()

plt.figure(2)
plt.plot(x, y_w, 'bs--', x, y_r, 'b^--', x, y_l_jp, 'ro--')
plt.ylabel('Latency(ms)')
plt.xlabel('#clients')
plt.xlim([0, 2000])
plt.legend(('GLA write', 'GLA read', 'SPaxos'))
plt.savefig("figures/threadsLa")
plt.show()
