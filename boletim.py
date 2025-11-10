import os
import pandas as pd

PASTA = "notas"

linhas = []

for arquivo in os.listdir(PASTA):
    if arquivo.endswith(".csv"):
        df = pd.read_csv(os.path.join(PASTA, arquivo))
        linhas.append(df.iloc[0])

boletim = pd.DataFrame(linhas)
boletim = boletim.sort_values("RA")

boletim.to_csv("boletim_final.csv", index=False)

print("✅ Boletim gerado: boletim_final.csv")
