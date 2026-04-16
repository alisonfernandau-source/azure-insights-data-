import pandas as pd
from datetime import datetime
import os

# =========================
# CONFIGURACIÓN
# =========================

archivo_fuente = "src/data/user_stories.csv"
archivo_snapshot = "src/data/feature_snapshot_history.csv"

os.makedirs("src/data", exist_ok=True)

DONE_STATES = ["Closed","Resolved","Removed"]
ACTIVE_STATES = ["Active","Impediment","Paused","Staging"]
TODO_STATES = ["New","Ready","Ready For Refinement","Refinement","Design"]

# =========================
# CARGAR DATA
# =========================

df = pd.read_csv(archivo_fuente, sep=';', encoding='utf-8-sig')

snapshot_date = datetime.now().strftime("%Y-%m-%d")

# =========================
# FEATURES Y HU
# =========================

features = df[df["Tipo"] == "Feature"].copy()

hu = df[df["Tipo"].isin(["User Story","Bug","Task","Spike","Enabler Story"])].copy()

hu = hu.merge(
    features[["ID","Título"]],
    left_on="Padre",
    right_on="ID",
    how="left",
    suffixes=("","_feature")
)

hu.rename(columns={"ID_feature":"feature_id","Título_feature":"feature_title"}, inplace=True)

# =========================
# MÉTRICAS
# =========================

rows = []

for feature_id, group in hu.groupby("feature_id"):

    if pd.isna(feature_id):
        continue

    feature_title = group["feature_title"].iloc[0]

    hu_total = len(group)
    hu_done = group[group["Estado"].isin(DONE_STATES)].shape[0]
    hu_active = group[group["Estado"].isin(ACTIVE_STATES)].shape[0]
    hu_todo = group[group["Estado"].isin(TODO_STATES)].shape[0]
    blocked_hu = group[group["Estado"] == "Impediment"].shape[0]

    pct_done = hu_done / hu_total if hu_total > 0 else 0

    feature_row = features[features["ID"] == feature_id]

    if not feature_row.empty and pd.notna(feature_row["Fec Activado"].iloc[0]):
        activated_date = pd.to_datetime(feature_row["Fec Activado"].iloc[0], dayfirst=True)
        aging_days = (datetime.now() - activated_date).days
    else:
        aging_days = None

    eternal_90_flag = pct_done >= 0.9 and hu_active > 0
    no_active_hu_flag = hu_active == 0 and hu_done < hu_total

    rows.append({
        "snapshot_date": snapshot_date,
        "feature_id": feature_id,
        "feature_title": feature_title,
        "hu_total": hu_total,
        "hu_done": hu_done,
        "hu_active": hu_active,
        "hu_todo": hu_todo,
        "pct_done": round(pct_done,2),
        "wip_hu": hu_active,
        "blocked_hu": blocked_hu,
        "aging_days": aging_days,
        "eternal_90_flag": eternal_90_flag,
        "no_active_hu_flag": no_active_hu_flag
    })

snapshot_df = pd.DataFrame(rows)

# =========================
# HISTÓRICO
# =========================

if os.path.exists(archivo_snapshot):
    history = pd.read_csv(archivo_snapshot)
    snapshot_df = pd.concat([history, snapshot_df], ignore_index=True)

snapshot_df.to_csv(archivo_snapshot, index=False, encoding="utf-8-sig")

print("Snapshot actualizado correctamente")
print(f"Features procesadas: {len(rows)}")