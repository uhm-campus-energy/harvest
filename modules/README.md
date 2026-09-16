# Harvest modules

This directory contains the Python functions invoked by the notebooks in `notebooks/`. Each section documents one module: purpose, parameters, return values, and notes relevant to diagnosis.

## Overview

Meter exports provide two quantities:

- **kW** — instantaneous (or short-interval) real power. Processing averages `3_phase_watt_total` onto a 15-minute grid.
- **kWh** — cumulative energy (`kwh` / `meter_reading`). Processing removes short-lived spikes, then interpolates onto exact 15-minute timestamps.

Raw timestamps are not guaranteed to fall on the quarter hour. Both paths standardize to a 15-minute schedule so meters can be compared.

**Pipeline**

1. `harvest_orig.py` — ingest raw CSVs (one subdirectory per meter) into a combined table.
2. Either:
  - `harvest_kw.py` — 15-minute mean kW. Optional: `find_missing_data.py` (completeness) or `harvest_kw_comp.py` (Harvest vs Aurora).
  - `harvest_kwh.py` — spike removal and 15-minute kWh interpolation.
3. `file_naming.py` — output filenames of the form `name_var_YYMMDD-YYMMDD.ext`.

**Conventions**

- Meter identifiers must match exactly, including case. `harvest_orig` lowercases names derived from folder names; `harvest_kw.load_data` replaces spaces with `_` in the info file but does not lowercase. A mismatch prevents the EPM7000 watt-to-kW conversion and produces values off by a factor of approximately 1,000.
- EPM7000 meters are treated as reporting watts (divided by 1,000). Any `meter_model` that does not contain `EPM7000` (including PQM2) is treated as already in kW.
- Required ingest columns after renaming: `datetime`, `kwh`, `3_phase_watt_total`. `total_watt_hour` and `3_phase_positive_real_energy_used` are treated as kWh; `3_phase_real_power` is treated as power. Unrecognized vendor headers are skipped until a rename is added.

---



## 1. `harvest_orig.py`

Ingests a directory whose subdirectories are meters, each containing one or more CSV files. Normalizes column names and returns per-meter tables for downstream processing.

### `validate_base_path(path)`

**Purpose.** Tests whether `path` exists on the filesystem.

**Parameters.** `path` — directory path (string).

**Returns.** `True` or `False`. Does not verify that CSV files are present.

### `get_csv_paths(base_path)`

**Purpose.** Enumerates CSV files by meter.

**Parameters.** `base_path` — parent directory containing one subdirectory per meter.

**Returns.** Dictionary mapping `meter_name` to a list of CSV paths. Names are lowercased; spaces become `_`; a trailing `_mtr` is removed. Files whose names start with `.` are ignored.

### `load_meter_dfs(basepath)`

**Purpose.** Primary ingest. For each meter, reads CSVs as binary, strips null bytes, decodes UTF-8 with replacement, normalizes headers, applies column aliases, retains required columns, concatenates files, inserts `meter_name`, and sorts by datetime. Empty files and files missing required columns are skipped with a printed message rather than an exception.

**Parameters.** `basepath` — same parent directory as `get_csv_paths`.

**Returns.** List of DataFrames (one per meter). Columns: `datetime`, `meter_name`, `kwh`, `3_phase_watt_total`.

### `concat_meter_dfs(meter_dfs)`

**Purpose.** Concatenates the per-meter list into a single DataFrame.

**Parameters.** `meter_dfs` — list returned by `load_meter_dfs`.

**Returns.** Combined DataFrame.

### `meter_list(csv_path)`

**Purpose.** Prints unique `meter_name` values and writes `*_meter_list.csv` adjacent to the input.

**Parameters.** `csv_path` — combined CSV that includes `meter_name`.

**Returns.** None. Raises `ValueError` if `meter_name` is absent, listing available columns.

---



## 2. `harvest_kw.py`

Computes 15-minute average kW from the combined meter table. Requires a meter-info CSV with at least `meter_name` and `meter_model`.

### `load_data(data_path, info_path)`

**Purpose.** Loads meter data and info. Drops `total_watt_hour` from the data if present. Parses and sorts `datetime` by meter. Drops `header1` and `header2` from the info file (both columns must exist). Replaces spaces with `_` in info `meter_name` values.

**Parameters.** `data_path` — combined meter CSV. `info_path` — meter-info CSV.

**Returns.** `(df, info_df)`.

### `filter_time_frame(df, start, end)`

**Purpose.** Restricts rows to `start` ≤ `datetime` ≤ `end`.

**Parameters.** `df` — meter data. `start`, `end` — datetime bounds (inclusive).

**Returns.** Copy of the filtered DataFrame.

### `process_kw_data(df, info_df)`

**Purpose.** Floors timestamps to 15-minute intervals, averages `3_phase_watt_total` per meter and interval, and divides by 1,000 when `meter_model` contains `EPM7000`.

**Parameters.** Data and info DataFrames from `load_data` (optionally after `filter_time_frame`).

**Returns.** DataFrame with `datetime`, `meter_name`, `mean_kw`. One row per meter per interval that contained at least one raw reading. Missing intervals are not inserted.

---



## 3. `harvest_kwh.py`

Cleans cumulative energy and interpolates onto exact 15-minute timestamps.

### `load_kwh(data_path)`

**Purpose.** Loads the combined CSV. Renames `kwh` to `meter_reading` when needed. Parses datetime with format `%Y-%m-%d %H:%M:%S` (`errors='coerce'`). Coerces `meter_reading` and, if present, `3_phase_watt_total` to numeric.

**Parameters.** `data_path` — combined meter CSV.

**Returns.** DataFrame prepared for cleaning.

### `_typical_positive_step(values)`

**Purpose.** Internal helper for spike detection. Estimates a typical positive increment (median of positive first differences after excluding the upper 10%). Returns `1.0` if no positive differences are available.

**Parameters.** Sequence of cumulative readings for one meter.

**Returns.** Scalar typical step.

### `remove_invalid_power_rows(meter_group, tiny_power_threshold=1e-20)`

**Purpose.** For a single meter, drops rows whose `|3_phase_watt_total|` is strictly between 0 and `tiny_power_threshold` (corrupted near-zero values such as `5.94e-39`). Exact zeros are retained. Unchanged if the power column is absent.

**Parameters.** One meter’s DataFrame. Optional threshold.

**Returns.** DataFrame with those rows removed.

### `remove_kwh_spikes(meter_group, lookback_rows=90, lookback_minutes=60)`

**Purpose.** For a single meter (sorted by time), removes short-lived upward excursions in the cumulative series that subsequently return near the prior baseline. Lookback defaults: 90 rows or 60 minutes. Spike magnitude is derived from `_typical_positive_step`.

**Parameters.** One meter’s DataFrame.

**Returns.** DataFrame with spike rows removed.

### `clean_kwh_spikes(df)`

**Purpose.** Applies `remove_invalid_power_rows` and `remove_kwh_spikes` per `meter_name`.

**Parameters.** DataFrame from `load_kwh`.

**Returns.** Cleaned DataFrame. If all groups are empty, returns an empty frame with the original columns.

### `process_kwh(df)`

**Purpose.** For each meter, constructs a 15-minute grid from `min.floor('15min')` through `max.ceil('15min')`.

- A reading exactly on a grid time is retained (`is_exact=True`, `interpolated=False`).
- Otherwise, if neighboring readings exist within 15 minutes on both sides, the grid value is linearly interpolated (`is_exact=True`, `interpolated=True`).
- Original off-grid rows are retained (`is_exact=False`).

Gaps larger than 15 minutes on either side are not interpolated. Drops `3_phase_watt_total` if present.

**Parameters.** Cleaned DataFrame (typically after `clean_kwh_spikes`).

**Returns.** DataFrame including original and interpolated rows, with flag columns.

### `interval_kwh(df)`

**Purpose.** Keeps rows with `is_exact == True` and drops `is_exact` and `interpolated`.

**Parameters.** Output of `process_kwh`.

**Returns.** 15-minute kWh DataFrame.

### `duplicate_check(df)`

**Purpose.** Prints fully duplicated rows, or reports that none exist. Does not delete rows.

**Parameters.** Any DataFrame.

**Returns.** None (printed output only).

### `meter_list(csv)`

**Purpose.** Prints unique `meter_name` values. Unlike `harvest_orig.meter_list`, does not write a file.

**Parameters.** CSV path.

**Returns.** None.

---



## 4. `harvest_kw_comp.py`

Aligns Harvest 15-minute kW with Aurora (or Blue Pillar) kW, produces per-meter plots, and summarizes agreement.

Harvest kW column: `mean_kw` (or `mean`, renamed). Aurora kW column: `mean` (or `blue_pillar_kw` / `mean_kw`, renamed to `mean`).

### `load_data_for_comparison(harvest_csv, aurora_csv)`

**Purpose.** Loads both files, normalizes column names, and performs an outer join on `meter_name` and `datetime`. Timestamps are not snapped; they must already align. Missing values on one side remain null.

**Parameters.** Paths to Harvest and Aurora processed kW CSVs.

**Returns.** `(merged_df, meters)` where `meters` is the unique meter-name array.

### `create_plots_pdf(merged_df, meters, filename)`

**Purpose.** Writes a PDF with one time-series plot per meter (Harvest `mean_kw` vs Aurora `mean`).

**Parameters.** Merged DataFrame, meter list, output path.

**Returns.** None. Output is the PDF. Large meter sets produce large files.

### `get_comparison_info(merged_df, meters, corr_threshold, pct_threshold)`

**Purpose.** For each meter, labels Harvest and Aurora as `ok`, `zeros` (all zeros), or `missing` (all null). If both are `ok`, computes Pearson correlation and mean percent difference relative to Harvest (`|H − A| / H × 100`; Harvest zeros are treated as NaN in the denominator).

Match column:

- `yes` — correlation greater than `corr_threshold` and mean percent difference less than `pct_threshold`
- `yes (high r=…) but missing data` — high correlation with large mean percent difference (the label may also reflect a scale offset)
- `no` — with reported r and mean percent difference
- `no valid data` or `n/a` — insufficient paired values, or a side labeled zeros/missing

**Parameters.** Merged DataFrame, meter list, correlation threshold, percent-difference threshold.

**Returns.** Summary DataFrame indexed by meter.

---



## 5. `find_missing_data.py`

Estimates monthly completeness of a processed kW file (`mean_kw`). A complete day of 15-minute data comprises 96 intervals.

### `load_kw_data(file_path)`

**Purpose.** Reads the CSV and parses `datetime`.

**Parameters.** Path to a processed kW CSV.

**Returns.** DataFrame.

### `find_missing_kw_data(file_path, start_month, end_month)`

**Purpose.** Restricts rows to calendar months `start_month` through `end_month` (1 = January) across **all years** present. For each meter and month, completeness is non-null `mean_kw` count divided by (days in month × 96), as a percentage to one decimal.

**Parameters.** Processed kW CSV path; integer month bounds (for example 6 and 8 for June–August).

**Returns.** Pivot table: meters as rows, `Mon'YY` as columns, percent present as values.

---



## 6. `file_naming.py`



### `make_filename(df, name, var, ext)`

**Purpose.** Constructs `{name}_{var}_{YYMMDD}-{YYMMDD}.{ext}` from the minimum and maximum of `df['datetime']`. Converts `datetime` in place.

**Parameters.** DataFrame with a `datetime` column; `name`, `var`, and file extension strings.

**Returns.** Filename string.

---



## Diagnostic map for potential issues


| Observation                                       | Module                                                              |
| ------------------------------------------------- | ------------------------------------------------------------------- |
| Missing meters, unexpected columns, skipped files | `harvest_orig.py` (`load_meter_dfs` prints skip reasons)            |
| kW off by a factor of ~1,000                      | `harvest_kw.py` (`process_kw_data`) and meter-info names/models     |
| kWh spikes or missing 15-minute points            | `harvest_kwh.py` (cleaning vs interpolation)                        |
| Harvest and Aurora series do not align            | `harvest_kw_comp.py` (identifiers and timestamps before thresholds) |
| Implausible completeness percentages              | `find_missing_data.py` (file type or interval length)               |
| Incorrect output filename dates                   | `file_naming.py`                                                    |


Most processing is per meter. Isolating a single meter through the relevant function is sufficient to locate most errors. Intended call order and output paths are defined in `notebooks/`.