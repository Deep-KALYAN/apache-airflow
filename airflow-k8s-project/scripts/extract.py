import pandas as pd

df = pd.DataFrame({
    "name": ["Alice", "Bob"],
    "age": [25, 30]
})

df.to_csv("/data/raw.csv", index=False)
print("Extract done")