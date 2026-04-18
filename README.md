# MyChart Activation Data Cleaning Script

This repository contains a Python data-cleaning script for preparing patient-level data for a MyChart activation analysis project.

The goal of the project is to examine how **MyChart activation status** relates to patient characteristics such as **age group, race, ethnicity, smoking status, and sex assigned at birth**, and to identify which groups show the highest or lowest digital engagement with MyChart.

## Project Purpose

MyChart is more than a patient app. It is a portal for viewing medical records, messaging providers, checking test results, requesting refills, scheduling appointments, and managing follow-up care. Differences in activation status across demographic groups may point to unequal access to digital healthcare tools, differences in outreach, digital literacy, trust, language access, or broader structural barriers.

This script was written to transform large raw CSV files into cleaner patient-level tables that can be used for summary statistics, visualizations, and presentation-ready findings.

## What This Script Does

The script:

1. Reads a raw `patients.csv` file.
2. Identifies the columns needed for analysis, including:
   - patient ID
   - MyChart status
   - age / birth-year bin
   - smoking status
   - race
   - ethnicity
   - first race
   - sex assigned at birth
3. Keeps only the relevant columns for analysis.
4. Drops duplicate patient IDs so each patient appears once.
5. Optionally reads `encounters.csv` and aggregates encounter-level information to the patient level.
6. Merges the cleaned patient table with encounter summaries.
7. Exports cleaned and summary CSV files for downstream analysis.

## Expected Input Files

Place the following files in the same folder as the script:

- `patients.csv`
- `encounters.csv`

These files are **not included in this repository**.

## Output Files

After running the script, it generates:

- `patients_clean.csv`  
  A cleaned patient-level table with the selected demographic and MyChart-related fields.

- `patient_master.csv`  
  A patient-level master table that combines the cleaned patient data with encounter-level aggregated information.

- `summary_age.csv`  
  Summary table for MyChart activation by age / birth-year bin.

- `summary_race.csv`  
  Summary table for MyChart activation by race.

- `summary_smoking.csv`  
  Summary table for MyChart activation by smoking status.

## Repository Structure

```text
.
├── make_patient_master.py
├── README.md
└── .gitignore
