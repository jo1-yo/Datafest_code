import pandas as pd
import re
from pathlib import Path

# ========= helper functions =========
def norm(s):
    return re.sub(r'[^a-z0-9]', '', str(s).lower())

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
    s = s.replace(r'^\s*$', pd.NA, regex=True)
    s = s.mask(s.str.lower().isin(missing_values), "Missing")
    s = s.fillna("Missing")
    return s

def clean_status_for_flag(series):
    s = series.astype(str).str.strip().str.lower()
    s = s.replace(r'^\s*$', pd.NA, regex=True)
    s = s.fillna("missing")
    return s

# ========= file paths =========
base = Path(".")
patients_file = base / "patients.csv"
encounters_file = base / "encounters.csv"

# ========= read patients =========
print("Reading patients.csv ...")
patients = pd.read_csv(patients_file, dtype=str, low_memory=False)

print("patients shape:", patients.shape)
print("patients columns:")
print(list(patients.columns))

# ========= locate important columns =========
pcols = build_col_lookup(patients.columns)

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

print("\nMatched columns:")
for k, v in required.items():
    print(f"{k}: {v}")

if patient_id_col is None or mychart_col is None:
    raise ValueError("Could not find DurableKey and/or MyChartStatus. Check actual column names above.")

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
else:
    patients_clean["smoking_status"] = "Missing"

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

# ActivatedFlag: only exact 'activated' = 1, everything else = 0
status_for_flag = clean_status_for_flag(patients[mychart_col])
patients_clean["activated_flag"] = (status_for_flag == "activated").astype(int)

# drop duplicate patient_id if any
before = len(patients_clean)
patients_clean = patients_clean.drop_duplicates(subset=["patient_id"])
after = len(patients_clean)

print(f"\nDropped duplicate patient_id rows: {before - after}")
print("patients_clean shape:", patients_clean.shape)

# ========= optional encounter summary =========
if encounters_file.exists():
    print("\nReading encounters.csv for optional aggregation ...")
    enc = pd.read_csv(encounters_file, dtype=str, low_memory=False)

    print("encounters shape:", enc.shape)
    print("encounters columns:")
    print(list(enc.columns))

    ecols = build_col_lookup(enc.columns)
    enc_patient_col = find_col(ecols, ["PatientDurableKey", "DurableKey"])
    enc_date_col = find_col(ecols, ["Date", "EncounterDate"])
    enc_key_col = find_col(ecols, ["EncounterKey"])

    if enc_patient_col is None:
        raise ValueError("Could not find PatientDurableKey in encounters.csv.")

    enc_summary = pd.DataFrame()
    enc_summary["patient_id"] = enc[enc_patient_col].astype(str).str.strip()

    # encounter_count
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

    # last_encounter_date
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
    print("\nencounters.csv not found. Creating patient_master from patients only.")
    patient_master = patients_clean.copy()

# fill encounter_count if missing
if "encounter_count" in patient_master.columns:
    patient_master["encounter_count"] = patient_master["encounter_count"].fillna(0)

# optional grouping for tableau
if "encounter_count" in patient_master.columns:
    patient_master["encounter_count_group"] = pd.cut(
        patient_master["encounter_count"].astype(float),
        bins=[-1, 0, 1, 3, 10, float("inf")],
        labels=["0", "1", "2-3", "4-10", "11+"]
    ).astype(str)

# ========= export =========
patients_clean.to_csv("patients_clean.csv", index=False)
patient_master.to_csv("patient_master.csv", index=False)

# also export quick summary tables for convenience
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
    patient_master.groupby("smoking_status", dropna=False)
    .agg(
        n=("patient_id", "count"),
        activation_rate=("activated_flag", "mean")
    )
    .reset_index()
    .sort_values("activation_rate", ascending=False)
)
summary_smoking.to_csv("summary_smoking.csv", index=False)

print("\nDone.")
print("Created files:")
print("- patients_clean.csv")
print("- patient_master.csv")
print("- summary_age.csv")
print("- summary_race.csv")
print("- summary_smoking.csv")