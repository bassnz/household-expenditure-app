from __future__ import annotations

import csv
import io
from datetime import datetime
from pathlib import Path
from typing import Optional
from zipfile import BadZipFile, ZipFile

import pandas as pd
import streamlit as st

TYPE1_HEADERS = [
    "Date Processed",
    "Date of Transaction",
    "Unique Id",
    "Tran Type",
    "Reference",
    "Description",
    "Amount",
]

TYPE2_HEADERS = [
    "Date",
    "Unique Id",
    "Tran Type",
    "Cheque Number",
    "Payee",
    "Memo",
    "Amount",
]

CATEGORY_COL = "Categorisation"
REFERENCE_DB_PATH = Path("categorisation_reference.csv")
REFERENCE_DB_COLUMNS = [
    CATEGORY_COL,
    "CSVType",
    "Description",
    "Payee",
    "Memo",
    "ModelDesc",
    "ModelMerchant",
    "LastUpdated",
]
REFERENCE_DB_DEDUPE_KEYS = [CATEGORY_COL, "CSVType", "Description", "Payee", "Memo", "ModelDesc", "ModelMerchant"]


st.set_page_config(page_title="Transaction Categorizer", layout="wide")
st.title("Transaction Categorizer")
st.caption("Upload historical .xlsx data + a new .csv file, review predicted categories, approve, then merge.")


def _read_text_bytes(raw_bytes: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return raw_bytes.decode(enc)
        except UnicodeDecodeError:
            continue
    raise ValueError("Could not decode CSV bytes with utf-8 or latin-1")


def _detect_csv_header_row(raw_text: str) -> tuple[int, str]:
    reader = csv.reader(io.StringIO(raw_text))
    for idx, row in enumerate(reader):
        clean = [c.strip() for c in row]
        if clean == TYPE1_HEADERS:
            return idx, "type1"
        if clean == TYPE2_HEADERS:
            return idx, "type2"
    raise ValueError(
        "CSV headers not recognized. Expected one of the two supported bank formats."
    )


def _load_supported_csv(uploaded_file) -> tuple[pd.DataFrame, str]:
    raw_bytes = uploaded_file.getvalue()
    if not raw_bytes:
        raise ValueError("The uploaded CSV is empty.")

    raw_text = _read_text_bytes(raw_bytes)
    header_row_idx, csv_type = _detect_csv_header_row(raw_text)

    df = pd.read_csv(
        io.StringIO(raw_text),
        skiprows=header_row_idx,
        engine="python",
        on_bad_lines="skip",
    )

    expected = TYPE1_HEADERS if csv_type == "type1" else TYPE2_HEADERS
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")

    # Keep only known columns for that native format.
    df = df[expected].copy()
    return df, csv_type


def _load_history_xlsx(uploaded_file) -> pd.DataFrame:
    raw_bytes = uploaded_file.getvalue()
    if not raw_bytes:
        st.error("The uploaded workbook is empty. Upload a non-empty .xlsx file.")
        st.stop()

    try:
        with ZipFile(io.BytesIO(raw_bytes)) as zf:
            names = set(zf.namelist())
    except BadZipFile:
        st.error(
            "This file is not a valid .xlsx workbook package. "
            "Please open it and re-save as Excel Workbook (.xlsx), then upload again."
        )
        st.stop()

    required_entries = {"[Content_Types].xml", "xl/workbook.xml"}
    if not required_entries.issubset(names):
        st.error(
            "The uploaded file is missing required .xlsx workbook parts. "
            "Please re-save it as a standard .xlsx file and try again."
        )
        st.stop()

    openpyxl_exc = None
    try:
        return pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl")
    except Exception as exc:  # noqa: BLE001
        openpyxl_exc = exc

    try:
        return pd.read_excel(io.BytesIO(raw_bytes), engine="calamine")
    except Exception as calamine_exc:  # noqa: BLE001
        st.error(
            "Could not read the historical workbook with either parser. "
            "Please re-save it as a standard .xlsx workbook and try again."
        )
        st.caption(f"openpyxl detail: {openpyxl_exc}")
        st.caption(f"calamine detail: {calamine_exc}")
        st.stop()


def _first_existing(cols: list[str], candidates: list[str]) -> Optional[str]:
    lookup = {c.lower(): c for c in cols}
    for c in candidates:
        found = lookup.get(c.lower())
        if found:
            return found
    return None


def _normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().lower().split())


def _load_reference_db() -> pd.DataFrame:
    if not REFERENCE_DB_PATH.exists():
        return pd.DataFrame(columns=REFERENCE_DB_COLUMNS)
    try:
        df = pd.read_csv(REFERENCE_DB_PATH)
    except Exception:  # noqa: BLE001
        return pd.DataFrame(columns=REFERENCE_DB_COLUMNS)
    for col in REFERENCE_DB_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[REFERENCE_DB_COLUMNS].copy()


def _save_reference_db(df: pd.DataFrame) -> None:
    clean = df.copy()
    for col in REFERENCE_DB_COLUMNS:
        if col not in clean.columns:
            clean[col] = pd.NA
    clean = clean[REFERENCE_DB_COLUMNS]
    clean = clean.sort_values(by=[CATEGORY_COL, "CSVType", "Description", "Payee", "Memo"], na_position="last")
    clean.to_csv(REFERENCE_DB_PATH, index=False)


def _dedupe_reference_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=REFERENCE_DB_COLUMNS)
    out = df.copy()
    for col in REFERENCE_DB_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out = out[REFERENCE_DB_COLUMNS]
    return out.drop_duplicates(subset=REFERENCE_DB_DEDUPE_KEYS, keep="last")


def _extract_reference_rows(df: pd.DataFrame, csv_type: str, category_col: str) -> pd.DataFrame:
    if category_col not in df.columns:
        return pd.DataFrame(columns=REFERENCE_DB_COLUMNS)

    out = pd.DataFrame(columns=REFERENCE_DB_COLUMNS)
    out[CATEGORY_COL] = df[category_col]
    out["CSVType"] = csv_type
    out["LastUpdated"] = datetime.now().isoformat(timespec="seconds")

    if csv_type == "type1":
        out["Description"] = df.get("Description", pd.Series(dtype="object"))
        model_desc = (
            df.get("Description", pd.Series(dtype="object")).fillna("").astype(str)
            + " "
            + df.get("Reference", pd.Series(dtype="object")).fillna("").astype(str)
        ).str.strip()
        out["ModelDesc"] = model_desc
        out["ModelMerchant"] = df.get("Description", pd.Series(dtype="object"))
        out["Payee"] = pd.NA
        out["Memo"] = pd.NA
    else:
        out["Payee"] = df.get("Payee", pd.Series(dtype="object"))
        out["Memo"] = df.get("Memo", pd.Series(dtype="object"))
        model_desc = (
            df.get("Payee", pd.Series(dtype="object")).fillna("").astype(str)
            + " "
            + df.get("Memo", pd.Series(dtype="object")).fillna("").astype(str)
        ).str.strip()
        out["ModelDesc"] = model_desc
        out["ModelMerchant"] = df.get("Payee", pd.Series(dtype="object"))
        out["Description"] = pd.NA

    out[CATEGORY_COL] = out[CATEGORY_COL].astype(str).str.strip()
    out["ModelDesc"] = out["ModelDesc"].astype(str).str.strip()
    out = out[(out[CATEGORY_COL] != "") & (out["ModelDesc"] != "")]
    out = out[REFERENCE_DB_COLUMNS]
    return out.drop_duplicates(
        subset=[CATEGORY_COL, "CSVType", "Description", "Payee", "Memo", "ModelDesc", "ModelMerchant"]
    )


def _build_reference_from_history(history_df: pd.DataFrame, category_col: str) -> pd.DataFrame:
    seeds = []
    if "Description" in history_df.columns:
        seeds.append(_extract_reference_rows(history_df, "type1", category_col))
    if {"Payee", "Memo"}.issubset(history_df.columns):
        seeds.append(_extract_reference_rows(history_df, "type2", category_col))
    if not seeds:
        return pd.DataFrame(columns=REFERENCE_DB_COLUMNS)
    seed_df = pd.concat(seeds, ignore_index=True) if seeds else pd.DataFrame(columns=REFERENCE_DB_COLUMNS)
    return _dedupe_reference_rows(seed_df)


def _majority_category(series: pd.Series) -> str:
    counts = series.dropna().astype(str).str.strip().value_counts()
    if counts.empty:
        return ""
    return str(counts.index[0])


def _build_reference_lookups(reference_df: pd.DataFrame) -> tuple[dict[str, str], dict[tuple[str, str], str]]:
    type1_map: dict[str, str] = {}
    type2_map: dict[tuple[str, str], str] = {}
    if reference_df.empty:
        return type1_map, type2_map

    type1_df = reference_df[reference_df["CSVType"] == "type1"].copy()
    if not type1_df.empty:
        type1_df["_k_desc"] = type1_df["Description"].map(_normalize_text)
        groups = type1_df[type1_df["_k_desc"] != ""].groupby("_k_desc")[CATEGORY_COL]
        for key, cats in groups:
            cat = _majority_category(cats)
            if cat:
                type1_map[key] = cat

    type2_df = reference_df[reference_df["CSVType"] == "type2"].copy()
    if not type2_df.empty:
        type2_df["_k_payee"] = type2_df["Payee"].map(_normalize_text)
        type2_df["_k_memo"] = type2_df["Memo"].map(_normalize_text)
        valid = type2_df[(type2_df["_k_payee"] != "") | (type2_df["_k_memo"] != "")]
        groups = valid.groupby(["_k_payee", "_k_memo"])[CATEGORY_COL]
        for key, cats in groups:
            cat = _majority_category(cats)
            if cat:
                type2_map[(key[0], key[1])] = cat

    return type1_map, type2_map


def _suggest_categories_from_reference(new_df: pd.DataFrame, csv_type: str, reference_df: pd.DataFrame) -> pd.DataFrame:
    out = new_df.copy()
    out["SuggestedCategorisation"] = ""
    out["MatchStatus"] = "No match"

    type1_map, type2_map = _build_reference_lookups(reference_df)
    if csv_type == "type1":
        keys = out["Description"].map(_normalize_text)
        matched = keys.map(type1_map).fillna("")
    else:
        keys = list(zip(out["Payee"].map(_normalize_text), out["Memo"].map(_normalize_text)))
        matched = pd.Series([type2_map.get(k, "") for k in keys], index=out.index)

    out["SuggestedCategorisation"] = matched
    out.loc[out["SuggestedCategorisation"] != "", "MatchStatus"] = "Matched reference"
    out["FinalCategorisation"] = out["SuggestedCategorisation"]
    return out


def _render_reference_view(reference_df: pd.DataFrame) -> None:
    st.subheader("Reference Database")
    st.caption("Mappings used to improve category learning.")
    if reference_df.empty:
        st.info("No reference rows saved yet. Approve a categorized CSV to populate the database.")
        return

    st.write(f"Stored mappings: {len(reference_df)}")
    csv_data = reference_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download reference DB",
        data=csv_data,
        file_name="categorisation_reference.csv",
        mime="text/csv",
    )
    for category in sorted(reference_df[CATEGORY_COL].dropna().astype(str).unique()):
        cat_df = reference_df[reference_df[CATEGORY_COL].astype(str) == category]
        with st.expander(f"{category} ({len(cat_df)} mappings)", expanded=False):
            type1 = cat_df[cat_df["CSVType"] == "type1"][["Description"]].dropna().drop_duplicates()
            if not type1.empty:
                st.markdown("**CSV Type 1 - Description**")
                st.dataframe(type1, use_container_width=True, hide_index=True)
            type2 = cat_df[cat_df["CSVType"] == "type2"][["Payee", "Memo"]].dropna(how="all").drop_duplicates()
            if not type2.empty:
                st.markdown("**CSV Type 2 - Payee + Memo**")
                st.dataframe(type2, use_container_width=True, hide_index=True)


def _prepare_history(history_df: pd.DataFrame) -> str:
    category_col = _first_existing(history_df.columns.tolist(), [CATEGORY_COL, "Category", "category"])
    if not category_col:
        raise ValueError(
            "Historical workbook needs a category column. Add a 'Categorisation' column with past labels."
        )
    return category_col


def _merge_for_export(history_df: pd.DataFrame, edited_df: pd.DataFrame) -> pd.DataFrame:
    history = history_df.copy()
    if CATEGORY_COL not in history.columns:
        history[CATEGORY_COL] = pd.NA

    incoming = edited_df.copy()
    incoming[CATEGORY_COL] = incoming["FinalCategorisation"]
    incoming = incoming.drop(columns=["SuggestedCategorisation", "FinalCategorisation", "MatchStatus"], errors="ignore")

    all_columns = list(dict.fromkeys(history.columns.tolist() + incoming.columns.tolist()))
    return pd.concat([history.reindex(columns=all_columns), incoming.reindex(columns=all_columns)], ignore_index=True)


with st.sidebar:
    st.header("1) Upload Files")
    history_file = st.file_uploader("Historical workbook (.xlsx)", type=["xlsx"], key="history")
    new_csv_file = st.file_uploader("New transactions (.csv)", type=["csv"], key="csv")

if not history_file or not new_csv_file:
    st.info("Upload both files to continue.")
    st.stop()

history_df = _load_history_xlsx(history_file)

try:
    new_df, csv_type = _load_supported_csv(new_csv_file)
except ValueError as exc:
    st.error(str(exc))
    st.stop()

st.subheader("2) Auto-Categorization")
st.caption(f"Detected CSV format: {csv_type}")

try:
    history_category_col = _prepare_history(history_df)
except ValueError as exc:
    st.error(str(exc))
    st.stop()

# Source-of-truth reference rows come from the historical workbook.
history_reference_df = _build_reference_from_history(history_df, history_category_col)
repo_reference_df = _load_reference_db()
reference_df = _dedupe_reference_rows(pd.concat([repo_reference_df, history_reference_df], ignore_index=True))

# Ensure the repository reference file exists and stays in sync with history-derived mappings.
reference_sync_error = None
reference_needs_write = (not REFERENCE_DB_PATH.exists()) or (len(reference_df) != len(repo_reference_df))
if reference_needs_write:
    try:
        _save_reference_db(reference_df)
    except Exception as exc:  # noqa: BLE001
        reference_sync_error = exc

st.caption("Categorisations in the uploaded .xlsx are treated as the initial source of truth.")
if reference_sync_error:
    st.warning("Reference DB could not be written in this runtime. Use the download button to persist it to your repo.")
    st.caption(f"Save detail: {reference_sync_error}")

_render_reference_view(reference_df)

if st.button("Run Auto-Categorization", type="primary"):
    try:
        predicted_df = _suggest_categories_from_reference(new_df, csv_type, reference_df)
    except ValueError as exc:
        st.error(str(exc))
    else:
        st.session_state["predicted_df"] = predicted_df
        st.session_state["csv_type"] = csv_type

if "predicted_df" not in st.session_state:
    st.stop()

st.subheader("3) Review and Approve")
st.write("Edit any category before approval.")

predicted_df = st.session_state["predicted_df"].copy()
review_df = predicted_df.copy()

match_count = int((review_df["SuggestedCategorisation"].astype(str).str.strip() != "").sum())
st.caption(f"Matched from reference: {match_count} of {len(review_df)} transactions")

edited_df = st.data_editor(
    review_df,
    use_container_width=True,
    num_rows="fixed",
    column_config={
        "SuggestedCategorisation": st.column_config.TextColumn(
            "SuggestedCategorisation",
            disabled=True,
        ),
        "FinalCategorisation": st.column_config.TextColumn(
            "FinalCategorisation",
            help="Enter a category manually when no reference match is found.",
        )
    },
)

approve = st.checkbox("I approve these categories and want to merge into the master workbook")
if approve and st.button("Merge into Master Workbook", type="primary"):
    missing_final = edited_df["FinalCategorisation"].fillna("").astype(str).str.strip() == ""
    if bool(missing_final.any()):
        st.error("Some transactions still have blank FinalCategorisation. Please complete them before merging.")
        st.stop()

    merged = _merge_for_export(history_df, edited_df)
    csv_type_for_save = st.session_state.get("csv_type", csv_type)

    new_refs = _extract_reference_rows(edited_df, csv_type_for_save, "FinalCategorisation")
    all_refs = _dedupe_reference_rows(pd.concat([reference_df, new_refs], ignore_index=True))
    ref_save_error = None
    try:
        _save_reference_db(all_refs)
    except Exception as exc:  # noqa: BLE001
        ref_save_error = exc

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        merged.to_excel(writer, index=False, sheet_name="Master")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"master_merged_{timestamp}.xlsx"

    st.success("Approved and merged. Download the updated master workbook.")
    st.download_button(
        label="Download merged workbook",
        data=output.getvalue(),
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    if ref_save_error:
        st.warning("Merged workbook succeeded, but reference database could not be saved on this runtime.")
        st.caption(f"Save detail: {ref_save_error}")
    else:
        st.success(f"Reference database updated: {REFERENCE_DB_PATH}")
