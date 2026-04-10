import pandas as pd

df = pd.read_csv("/data/raw.csv")
df["age_plus_10"] = df["age"] + 10

df.to_csv("/data/transformed.csv", index=False)
print("Transform done")