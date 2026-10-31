# Flujo ETL con Apache Airflow

Flujo ETL de ventas: **extrae** datos, los **transforma** (total por producto) y los **carga** en SQLite.

## Requisitos
- Docker Desktop abierto
- VS Code (o cualquier editor)

## Pasos (PowerShell)

1. Crear carpeta y descargar Airflow:
```powershell
mkdir airflow-etl
cd airflow-etl
curl.exe -LfO https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
mkdir dags, logs, plugins, data
"AIRFLOW_UID=50000" | Out-File .env -Encoding ascii
```

2. Inicializar:
```powershell
docker compose up airflow-init
```

3. Levantar Airflow:
```powershell
docker compose up -d
```

4. Abrir http://localhost:8080 (usuario `airflow`, clave `airflow`).

5. Copiar `etl_ventas.py` a la carpeta `dags/`.

6. En Airflow: buscar `etl_ventas` → activar → **Trigger**.

7. Ver `extract`, `transform` y `load` en verde.

## Qué hace cada tarea
- **extract**: crea un CSV con ventas de ejemplo.
- **transform**: calcula `total = cantidad * precio` y agrupa por producto.
- **load**: guarda el resultado en la base SQLite `ventas.db`.

## Apagar
```powershell
docker compose down
```