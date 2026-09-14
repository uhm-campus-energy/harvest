# Harvest notebooks

Notebooks configure paths and flags, then call functions in `modules/`. Function behavior is documented in [modules/README.md](../modules/README.md).

Run notebooks from `notebooks/` so `../data/` paths resolve.

## Pipeline

```text
1. harvest_orig.ipynb
        │
        ├── 2a. harvest_kwh.ipynb          (kWh)
        └── 2b. harvest_kw.ipynb           (kW)
                    │
                    └── 3. harvest_comparison_aurora_kw.ipynb

Optional: harvest_aurora_kwh.ipynb   (kWh steps on Aurora-format files)

Helpers in other/  — not part of the main pipeline
```

Steps 2a and 2b are independent of each other. Both require the CSV from step 1.

| Notebook | Purpose | Input | Output |
|---|---|---|---|
| [harvest_orig.ipynb](harvest_orig.ipynb) | Combine raw meter folders into one table | Directory with one subdirectory per meter | `data/outputs/harvest_orig_YYMMDD-YYMMDD.csv` |
| [harvest_kwh.ipynb](harvest_kwh.ipynb) | Remove spikes and interpolate 15-minute kWh | Orig CSV | `harvest_kwh_…csv` (all rows) and `harvest_kwh_15min_…csv` (clock times only) |
| [harvest_kw.ipynb](harvest_kw.ipynb) | Average power to 15-minute kW | Orig CSV and `harvest_meter_guide.csv` | `harvest_kw_YYMMDD-YYMMDD.csv` |
| [harvest_comparison_aurora_kw.ipynb](harvest_comparison_aurora_kw.ipynb) | Compare Harvest kW to Aurora kW | Both kW CSVs | Plots PDF and comparison-info CSV |
| [harvest_aurora_kwh.ipynb](harvest_aurora_kwh.ipynb) | Apply Harvest kWh processing to Aurora kWh | Aurora long-format kWh CSV | `aurora_kwh_…csv` (Aurora is often already on 15 minutes) |

## Configuration

Each notebook has an **Enter input** cell. Set paths and flags there; processing cells generally do not need edits.

- **Filename date stamps.** Orig output is named from the data’s min/max dates. Downstream notebooks must use that stamp (for example `harvest_orig_250723-260508.csv`).
- **`data_path`.** Orig expects a directory of raw CSVs, not a single file.
- **`info_path`.** kW requires the meter-model guide; without it, EPM7000 conversion can be off by a factor of 1,000.
- Flags such as `time_frame`, `dedup_check`, and `create_merged_csv`.

## Diagnosis

1. Confirm orig ran first and the filename stamp matches.
2. Open the corresponding section in [modules/README.md](../modules/README.md) (notebook markdown names the function).
3. Inspect one meter in the output CSV before re-running the full set.

## `other/`

Ad hoc checks, not part of the main pipeline:

| Notebook | Purpose |
|---|---|
| [other/find_time_skips.ipynb](other/find_time_skips.ipynb) | kWh: meters missing 15-minute slots. kW: monthly completeness via `find_missing_kw_data`. |
| [other/swapping_gilmore_hall_kw.ipynb](other/swapping_gilmore_hall_kw.ipynb) | Plot Harvest vs Aurora Gilmore Hall A/B crossed (A vs B) to check swapped labels. |
