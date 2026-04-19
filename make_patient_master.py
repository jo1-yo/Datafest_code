import pandas as pd
import re
from pathlib import Path

# ========= config =========
# 先调试 smoking 清理时，建议先设成 False
# 等你电脑空间够了、环境稳定了，再改成 True 跑 encounters
INCLUDE_ENCOUNTERS = False

# ========= helper functions =========
def norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s).lower())

def build_col_lookup(columns):
    return {norm(c): c for c in columns}

def find_col(col_lookup, candidates):
    for c in candidates:
        key = norm(c)
        if key in col_lookup:
            return col_lookup[key]
    return None

def clean_missing(series):
    missing_values = {
        "", "na", "n/a", "null", "none",
        "unknown", "*unknown",
        "unspecified", "*unspecified",
        "*not applicable", "not applicable"
    }
    s = series.astype(str).str.strip()
    s = s.replace(r"^\s*$", pd.NA, regex=True)
    s = s.mask(s.str.lower().isin(missing_values), "Missing")
    s = s.fillna("Missing")
    return s

def clean_status_for_flag(series):
    s = series.astype(str).str.strip().str.lower()
    s = s.replace(r"^\s*$", pd.NA, regex=True)
    s = s.fillna("missing")
    return s

def clean_smoking_group(series):
    s = clean_missing(series).astype(str).str.strip()

    mapping = {
        "Never": "Never",
        "Former": "Former",
        "Every Day": "Current Smoker",
        "Some Days": "Current Smoker",
        "Heavy Smoker": "Current Smoker",
        "Light Smoker": "Current Smoker",
        "Smoker, Current Status Unknown": "Current Smoker",
        "Passive Smoke Exposure - Never Smoker": "Passive / Not Assessed",
        "Never Assessed": "Passive / Not Assessed",
        "Missing": "Missing",
    }

    return s.map(lambda x: mapping.get(x, "Other"))

# ========= file paths =========
base = Path(".")
patients_file = base / "patients.csv"
encounters_file = base / "encounters.csv"

# ========= read patients header first =========
print("Reading patients.csv header ...")
patient_header = pd.read_csv(patients_file, nrows=0)
patient_cols_all = list(patient_header.columns)
pcols = build_col_lookup(patient_cols_all)

patient_id_col = find_col(pcols, ["DurableKey", "PatientDurableKey"])
mychart_col = find_col(pcols, ["MyChartStatus", "MyChart Status"])
birth_bin_col = find_col(pcols, ["PatientBirthYearBin", "Patient Birth Year Bin"])
smoking_col = find_col(pcols, ["Smoking Status", "SmokingStatus"])
omb_race_col = find_col(pcols, ["OmbRace", "OMB Race"])
omb_eth_col = find_col(pcols, ["OmbEthnicity", "OMB Ethnicity"])
first_race_col = find_col(pcols, ["FirstRace", "First Race"])
sex_col = find_col(pcols, ["SexAssignedAtBirth", "Sex Assigned At Birth"])

required = {
    "patient_id": patient_id_col,
    "mychart_status": mychart_col,
    "birth_year_bin": birth_bin_col,
    "smoking_status": smoking_col,
    "omb_race": omb_race_col,
    "omb_ethnicity": omb_eth_col,
    "first_race": first_race_col,
    "sex_assigned_at_birth": sex_col,
}

print("\nMatched patient columns:")
for k, v in required.items():
    print(f"{k}: {v}")

if patient_id_col is None or mychart_col is None:
    raise ValueError("Could not find DurableKey and/or MyChartStatus in patients.csv.")

patient_usecols = [c for c in [
    patient_id_col, mychart_col, birth_bin_col, smoking_col,
    omb_race_col, omb_eth_col, first_race_col, sex_col
] if c is not None]

print("\nReading patients.csv data ...")
patients = pd.read_csv(
    patients_file,
    dtype=str,
    low_memory=False,
    usecols=patient_usecols
)

print("patients shape:", patients.shape)

# ========= build patients_clean =========
patients_clean = pd.DataFrame()
patients_clean["patient_id"] = patients[patient_id_col].astype(str).str.strip()
patients_clean["mychart_status"] = clean_missing(patients[mychart_col])

if birth_bin_col:
    patients_clean["birth_year_bin"] = clean_missing(patients[birth_bin_col])
else:
    patients_clean["birth_year_bin"] = "Missing"

if smoking_col:
    patients_clean["smoking_status"] = clean_missing(patients[smoking_col])
    patients_clean["smoking_group"] = clean_smoking_group(patients[smoking_col])
else:
    patients_clean["smoking_status"] = "Missing"
    patients_clean["smoking_group"] = "Missing"

if omb_race_col:
    patients_clean["omb_race"] = clean_missing(patients[omb_race_col])
else:
    patients_clean["omb_race"] = "Missing"

if omb_eth_col:
    patients_clean["omb_ethnicity"] = clean_missing(patients[omb_eth_col])
else:
    patients_clean["omb_ethnicity"] = "Missing"

if first_race_col:
    patients_clean["first_race"] = clean_missing(patients[first_race_col])
else:
    patients_clean["first_race"] = "Missing"

if sex_col:
    patients_clean["sex_assigned_at_birth"] = clean_missing(patients[sex_col])
else:
    patients_clean["sex_assigned_at_birth"] = "Missing"

status_for_flag = clean_status_for_flag(patients[mychart_col])
patients_clean["activated_flag"] = (status_for_flag == "activated").astype(int)

before = len(patients_clean)
patients_clean = patients_clean.drop_duplicates(subset=["patient_id"])
after = len(patients_clean)

print(f"\nDropped duplicate patient_id rows: {before - after}")
print("patients_clean shape:", patients_clean.shape)

# ========= optional encounter summary =========
if INCLUDE_ENCOUNTERS and encounters_file.exists():
    print("\nReading encounters.csv header ...")
    enc_header = pd.read_csv(encounters_file, nrows=0)
    enc_cols_all = list(enc_header.columns)
    ecols = build_col_lookup(enc_cols_all)

    enc_patient_col = find_col(ecols, ["PatientDurableKey", "DurableKey"])
    enc_date_col = find_col(ecols, ["Date", "EncounterDate"])
    enc_key_col = find_col(ecols, ["EncounterKey"])

    print("Matched encounter columns:")
    print("enc_patient_col:", enc_patient_col)
    print("enc_date_col:", enc_date_col)
    print("enc_key_col:", enc_key_col)

    if enc_patient_col is None:
        raise ValueError("Could not find PatientDurableKey in encounters.csv.")

    enc_usecols = [c for c in [enc_patient_col, enc_date_col, enc_key_col] if c is not None]

    print("\nReading encounters.csv data ...")
    enc = pd.read_csv(
        encounters_file,
        dtype=str,
        low_memory=False,
        usecols=enc_usecols
    )

    print("encounters shape:", enc.shape)

    enc_summary = pd.DataFrame()
    enc_summary["patient_id"] = enc[enc_patient_col].astype(str).str.strip()

    if enc_key_col:
        enc_summary["encounter_key"] = enc[enc_key_col]
        enc_summary = (
            enc_summary.groupby("patient_id", as_index=False)
            .agg(encounter_count=("encounter_key", "count"))
        )
    else:
        enc_summary = (
            enc_summary.groupby("patient_id", as_index=False)
            .size()
            .rename(columns={"size": "encounter_count"})
        )

    if enc_date_col:
        enc_dates = pd.DataFrame({
            "patient_id": enc[enc_patient_col].astype(str).str.strip(),
            "enc_date": pd.to_datetime(enc[enc_date_col], errors="coerce")
        })
        last_dates = (
            enc_dates.groupby("patient_id", as_index=False)
            .agg(last_encounter_date=("enc_date", "max"))
        )
        enc_summary = enc_summary.merge(last_dates, on="patient_id", how="left")

    patient_master = patients_clean.merge(enc_summary, on="patient_id", how="left")
else:
    print("\nSkipping encounters.csv. Creating patient_master from patients only.")
    patient_master = patients_clean.copy()

if "encounter_count" in patient_master.columns:
    patient_master["encounter_count"] = patient_master["encounter_count"].fillna(0)

if "encounter_count" in patient_master.columns:
    patient_master["encounter_count_group"] = pd.cut(
        patient_master["encounter_count"].astype(float),
        bins=[-1, 0, 1, 3, 10, float("inf")],
        labels=["0", "1", "2-3", "4-10", "11+"]
    ).astype(str)

# ========= export =========
patients_clean.to_csv("patients_clean.csv", index=False)
patient_master.to_csv("patient_master.csv", index=False)

summary_age = (
    patient_master.groupby("birth_year_bin", dropna=False)
    .agg(
        n=("patient_id", "count"),
        activation_rate=("activated_flag", "mean")
    )
    .reset_index()
    .sort_values("activation_rate", ascending=False)
)
summary_age.to_csv("summary_age.csv", index=False)

summary_race = (
    patient_master.groupby("omb_race", dropna=False)
    .agg(
        n=("patient_id", "count"),
        activation_rate=("activated_flag", "mean")
    )
    .reset_index()
    .sort_values("activation_rate", ascending=False)
)
summary_race.to_csv("summary_race.csv", index=False)

summary_smoking = (
    patient_master.groupby("smoking_group", dropna=False)
    .agg(
        n=("patient_id", "count"),
        activation_rate=("activated_flag", "mean")
    )
    .reset_index()
    .sort_values("activation_rate", ascending=False)
)
summary_smoking.to_csv("summary_smoking.csv", index=False)

summary_smoking_raw = (
    patient_master.groupby("smoking_status", dropna=False)
    .agg(
        n=("patient_id", "count"),
        activation_rate=("activated_flag", "mean")
    )
    .reset_index()
    .sort_values("activation_rate", ascending=False)
)
summary_smoking_raw.to_csv("summary_smoking_raw.csv", index=False)

print("\nDone.")
print("Created files:")
print("- patients_clean.csv")
print("- patient_master.csv")
print("- summary_age.csv")
print("- summary_race.csv")
print("- summary_smoking.csv")
print("- summary_smoking_raw.csv")
