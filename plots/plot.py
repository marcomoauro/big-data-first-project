import numpy as np
import matplotlib.pyplot as plt
from plots.data import DATA
import os

def plot(map_reduce_values, spark_values, hive_values, job, env):
    ind = np.arange(10)
    width = 0.26
    plt.bar(ind, map_reduce_values, width, label='MapReduce')
    plt.bar(ind + width, spark_values, width, label='Spark')
    plt.bar(ind + (width * 2), hive_values, width, label='Hive')

    plt.title(env.title() + ' - ' + job.title())
    plt.ylabel('Time (S)')

    plt.xticks(ind + width, ('25%', '50%', '75%', '100%', '125%', '150%', '175%', '200%', '400%', '800%'))
    plt.legend(loc='best')
    fig = plt.gcf()
    plt.close()
    fig.savefig(filename(job, env), dpi=200)

def filename(job, env):
    tmp_dirpath = os.getcwd() + '/tmp/'
    if not os.path.exists(tmp_dirpath):
        os.makedirs(tmp_dirpath)
    return tmp_dirpath + env + '_' + job + '.png'

for env in DATA.keys():
    for job in DATA['local']['map_reduce'].keys():
        plot(DATA[env]['map_reduce'][job], DATA[env]['spark'][job], DATA[env]['hive'][job], job, env)
