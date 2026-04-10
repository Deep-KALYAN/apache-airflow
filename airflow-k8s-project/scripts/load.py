import pandas as pd

df = pd.read_csv("/data/transformed.csv")
print(df)

df.to_csv("/data/final.csv", index=False)
print("Load done")