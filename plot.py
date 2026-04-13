import pandas as pd
import matplotlib.pyplot as plt

path = r"D:\rjh\learn\consult/"
path = "/data/check/pnls/"

def load_plt(ids: list):
    for i in ids:
        df = pd.read_pickle(path + i +".pkl")
        x = range(0, df.shape[0])
        y = df[i]
        plt.plot(x, y, label=i)
    plt.legend(loc="lower right", fontsize=10, title="Functions", frameon=True)
    plt.show()
    plt.savefig('/tmp/plot.png')
# load_plt(["0g6K7Kv", "0LmP8bk"])