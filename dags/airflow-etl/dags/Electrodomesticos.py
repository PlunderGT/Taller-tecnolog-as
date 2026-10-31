from datetime import datetime
import os, sqlite3
import pandas as pd
from airflow.sdk import dag, task

BASE = "/opt/airflow/dags/Electrodomesticos"

@dag(dag_id="Electrodomesticos", start_date=datetime(2026, 10, 1),
     schedule=None, catchup=False)
def Electrodomesticos():

    @task
    def extract():
        os.makedirs(BASE, exist_ok=True)
        df = pd.DataFrame({
            "producto": ["Licuadora", "Microondas", "Refrigerador", "Horno", "Lavadora", "Televisor"],
            "cantidad": [2, 2, 2, 2, 3, 3],
            "precio": [150, 300, 1500, 900, 2500, 3100],
        })
        df.to_csv(f"{BASE}/ventas_raw.csv", index=False)
        return f"{BASE}/ventas_raw.csv"

    @task
    def transform(path):
        df = pd.read_csv(path)
        df["total"] = df["cantidad"] * df["precio"]
        out = df.groupby("producto", as_index=False)["total"].sum()
        out.to_csv(f"{BASE}/ventas_clean.csv", index=False)
        return f"{BASE}/ventas_clean.csv"

    @task
    def load(path):
        df = pd.read_csv(path)
        con = sqlite3.connect(f"{BASE}/ventas.db")
        df.to_sql("ventas_por_producto", con, if_exists="replace", index=False)
        con.close()

    load(transform(extract()))

Electrodomesticos()