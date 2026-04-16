import requests
import time
from datetime import datetime
from requests.auth import HTTPBasicAuth
import pandas as pd
import os

# =========================
# CONFIGURACIÓN
# =========================

organization = "Jikkosoft"
project = "SILIN"
pat = os.getenv("AZURE_PAT")  # 🔥 IMPORTANTE: ya NO va quemado

url = f"https://dev.azure.com/{organization}/{project}/_apis/wit/wiql?api-version=7.0"

# =========================
# QUERY
# =========================

query = {
    "query": """
    SELECT 
        [System.Id],
        [System.Title],
        [System.WorkItemType],
        [System.State],
        [System.CreatedDate],
        [Microsoft.VSTS.Common.ActivatedDate],
        [Microsoft.VSTS.Common.ResolvedDate],
        [Microsoft.VSTS.Common.ClosedDate],
        [System.ChangedDate],
        [System.AssignedTo],
        [System.Tags],
        [System.AreaPath],
        [System.IterationPath],
        [Microsoft.VSTS.Scheduling.StoryPoints],
        [Microsoft.VSTS.Scheduling.OriginalEstimate],
        [Microsoft.VSTS.Common.ResolvedReason],
        [Microsoft.VSTS.Common.Priority],
        [Microsoft.VSTS.Common.Severity],
        [System.Parent]
    FROM WorkItems
    WHERE 
        (
            [System.AreaPath] = 'SILIN\\Squad - Migracion'
            OR
            [System.AreaPath] = 'SILIN\\Squad IE - Migracion-Punta a Punta Parte 1'
        )
    ORDER BY [System.CreatedDate] ASC
    """
}

# =========================
# EJECUCIÓN CON REINTENTOS
# =========================

max_retries = 5

for attempt in range(max_retries):
    response = requests.post(url, json=query, auth=HTTPBasicAuth('', pat))

    if response.status_code == 200:
        break

    if response.status_code == 503:
        print(f"Azure DevOps ocupado. Reintentando intento {attempt+1}...")
        time.sleep(5)
    else:
        print("Error en la solicitud:", response.status_code)
        print(response.text)
        exit()

if response.status_code != 200:
    print("No se pudo completar la solicitud.")
    exit()

data = response.json()
ids = [str(item["id"]) for item in data["workItems"]]

if not ids:
    print("No se encontraron items.")
    exit()

print(f"Se encontraron {len(ids)} items.")

# =========================
# DETALLE POR LOTES
# =========================

details_url = f"https://dev.azure.com/{organization}/_apis/wit/workitemsbatch?api-version=7.0"

all_items = []

for i in range(0, len(ids), 200):
    batch_ids = ids[i:i + 200]

    details_body = {
        "ids": batch_ids,
        "fields": [
            "System.Id","System.Title","System.WorkItemType","System.State",
            "System.CreatedDate","Microsoft.VSTS.Common.ActivatedDate",
            "Microsoft.VSTS.Common.ResolvedDate","Microsoft.VSTS.Common.ClosedDate",
            "System.ChangedDate","System.AssignedTo","System.Tags",
            "System.AreaPath","System.IterationPath",
            "Microsoft.VSTS.Scheduling.StoryPoints",
            "Microsoft.VSTS.Scheduling.OriginalEstimate",
            "Microsoft.VSTS.Common.ResolvedReason",
            "Microsoft.VSTS.Common.Priority",
            "Microsoft.VSTS.Common.Severity",
            "System.Parent"
        ]
    }

    response = requests.post(details_url, json=details_body, auth=HTTPBasicAuth('', pat))

    if response.status_code != 200:
        print(f"Error en lote {i}")
        continue

    all_items.extend(response.json()["value"])

# =========================
# FORMATEO
# =========================

def format_date(iso_date):
    if iso_date:
        try:
            return datetime.strptime(iso_date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d/%m/%Y %H:%M:%S")
        except:
            return iso_date
    return ""

rows = []

for item in all_items:
    fields = item['fields']

    rows.append({
        "ID": item['id'],
        "Tipo": fields.get('System.WorkItemType', ''),
        "Título": fields.get('System.Title', ''),
        "Asignado a": fields.get('System.AssignedTo', {}).get('displayName', '') if isinstance(fields.get('System.AssignedTo', {}), dict) else '',
        "Padre": fields.get('System.Parent', ''),
        "Estado": fields.get('System.State', ''),
        "Área": fields.get('System.AreaPath', ''),
        "Tags": fields.get('System.Tags', ''),
        "Iteración": fields.get('System.IterationPath', ''),
        "Última modificación": format_date(fields.get('System.ChangedDate', '')),
        "Fec Creado": format_date(fields.get('System.CreatedDate', '')),
        "Fec Activado": format_date(fields.get('Microsoft.VSTS.Common.ActivatedDate', '')),
        "Fec Resuelto": format_date(fields.get('Microsoft.VSTS.Common.ResolvedDate', '')),
        "Fec Cerrado": format_date(fields.get('Microsoft.VSTS.Common.ClosedDate', '')),
        "Puntos Historia": fields.get('Microsoft.VSTS.Scheduling.StoryPoints', ''),
        "Estimación": fields.get('Microsoft.VSTS.Scheduling.OriginalEstimate', ''),
        "Resolved reason": fields.get('Microsoft.VSTS.Common.ResolvedReason', ''),
        "Prioridad": fields.get('Microsoft.VSTS.Common.Priority', ''),
        "Severidad": fields.get('Microsoft.VSTS.Common.Severity', '')
    })

df = pd.DataFrame(rows)

# =========================
# GUARDAR CSV (PARA DASHBOARD)
# =========================

os.makedirs("src/data", exist_ok=True)

ruta_csv = "src/data/user_stories.csv"

df.to_csv(ruta_csv, index=False, encoding='utf-8-sig', sep=';')

print(f"Datos guardados en {ruta_csv}")