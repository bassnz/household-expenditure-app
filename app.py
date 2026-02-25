from __future__ import annotations

import csv
import io
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
MAIN_WORKBOOK_PATH = Path("Household_Expenses.xlsx")


st.set_page_config(page_title="Transaction Categorizer", layout="wide")
st.title("Transaction Categorizer")
st.caption("Upload a new .csv file. Suggestions are generated from Household_Expenses.xlsx.")


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
    raise ValueError("CSV headers not recognized. Expected one of the two supported bank formats.")


def _load_supported_csv(uploaded_file) -> tuple[pd.DataFrame, str]:
    raw_bytes = uploaded_file.getvalue()
    if not raw_bytes:
        raise ValueError("The uploaded CSV is empty.")

    raw_text = _read_text_bytes(raw_bytes)
    header_row_idx, csv_type = _detect_csv_header_row(raw_text)

    df = pd.read_csv(io.StringIO(raw_text), skiprows=header_row_idx, engine="python", on_bad_lines="skip")
    expected = TYPE1_HEADERS if csv_type == "type1" else TYPE2_HEADERS
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")
    return df[expected].copy(), csv_type


def _load_main_workbook(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise ValueError(
            f"Main workbook not found: {path}. Add Household_Expenses.xlsx to the repository root."
        )

    raw_bytes = path.read_bytes()
    if not raw_bytes:
        raise ValueError("Main workbook is empty.")

    try:
        with ZipFile(io.BytesIO(raw_bytes)) as zf:
            names = set(zf.namelist())
    except BadZipFile as exc:
        raise ValueError("Main workbook is not a valid .xlsx file.") from exc

    required_entries = {"[Content_Types].xml", "xl/workbook.xml"}
    if not required_entries.issubset(names):
        raise ValueError("Main workbook is missing required .xlsx workbook parts.")

    try:
        return pd.read_excel(io.BytesIO(raw_bytes), engine="openpyxl")
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Could not read main workbook: {exc}") from exc


def _first_existing(cols: list[str], candidates: list[str]) -> Optional[str]:
    lookup = {c.lower(): c for c in cols}
    for candidate in candidates:
        found = lookup.get(candidate.lower())
        if found:
            return found
    return None


def _normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).strip().lower().split())


def _normalized_unique_id(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower()


def _parse_dates_to_iso(series: pd.Series) -> pd.Series:
    dayfirst = pd.to_datetime(series, errors="coerce", dayfirst=True)
    monthfirst = pd.to_datetime(series, errors="coerce", dayfirst=False)
    merged = dayfirst.fillna(monthfirst)
    return merged.dt.strftime("%Y-%m-%d").fillna("")


def _series_or_blank(df: pd.DataFrame, col: str) -> pd.Series:
    if col in df.columns:
        return df[col]
    return pd.Series("", index=df.index, dtype="object")


def _build_duplicate_key(df: pd.DataFrame) -> pd.Series:
    date_source = _series_or_blank(df, "Date of Transaction")
    if "Date Processed" in df.columns:
        date_source = date_source.where(date_source.notna() & (date_source.astype(str).str.strip() != ""), df["Date Processed"])
    if "Date" in df.columns:
        date_source = date_source.where(date_source.notna() & (date_source.astype(str).str.strip() != ""), df["Date"])

    date_key = _parse_dates_to_iso(date_source)
    amount_key = (
        pd.to_numeric(_series_or_blank(df, "Amount"), errors="coerce")
        .round(2)
        .map(lambda v: f"{v:.2f}" if pd.notna(v) else "")
    )
    tran_key = _series_or_blank(df, "Tran Type").map(_normalize_text)
    desc_key = _series_or_blank(df, "Description").map(_normalize_text)
    ref_key = _series_or_blank(df, "Reference").map(_normalize_text)
    payee_key = _series_or_blank(df, "Payee").map(_normalize_text)
    memo_key = _series_or_blank(df, "Memo").map(_normalize_text)

    return (
        date_key
        + "|"
        + amount_key
        + "|"
        + tran_key
        + "|"
        + desc_key
        + "|"
        + ref_key
        + "|"
        + payee_key
        + "|"
        + memo_key
    )


def _annotate_duplicates(new_df: pd.DataFrame, history_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    annotated = new_df.copy()
    reasons = pd.Series("", index=annotated.index, dtype="object")

    new_uid = _normalized_unique_id(_series_or_blank(annotated, "Unique Id"))
    hist_uid = _normalized_unique_id(_series_or_blank(history_df, "Unique Id"))
    hist_uid_set = set(hist_uid[hist_uid != ""])

    dup_within_new_uid = (new_uid != "") & new_uid.duplicated(keep=False)
    dup_against_hist_uid = (new_uid != "") & new_uid.isin(hist_uid_set)

    new_key = _build_duplicate_key(annotated)
    hist_key = _build_duplicate_key(history_df)
    hist_key_set = set(hist_key[hist_key != "||||||"])
    dup_within_new_key = (new_key != "||||||") & new_key.duplicated(keep=False)
    dup_against_hist_key = (new_key != "||||||") & new_key.isin(hist_key_set)

    reasons = reasons.mask(dup_within_new_uid, "Duplicate Unique Id within uploaded CSV")
    reasons = reasons.mask(dup_against_hist_uid, "Unique Id already exists in Household_Expenses.xlsx")
    reasons = reasons.mask((reasons == "") & dup_within_new_key, "Possible duplicate transaction within uploaded CSV")
    reasons = reasons.mask((reasons == "") & dup_against_hist_key, "Possible duplicate transaction already in Household_Expenses.xlsx")

    annotated["DuplicateFlag"] = reasons != ""
    annotated["DuplicateReason"] = reasons

    report_cols = [c for c in ["Unique Id", "Date Processed", "Date of Transaction", "Date", "Description", "Payee", "Memo", "Amount"] if c in annotated.columns]
    report = annotated[annotated["DuplicateFlag"]][report_cols + ["DuplicateReason"]].copy()
    return annotated, report


def _extract_reference_rows(df: pd.DataFrame, csv_type: str, category_col: str) -> pd.DataFrame:
    if category_col not in df.columns:
        return pd.DataFrame(columns=[CATEGORY_COL, "CSVType", "Description", "Payee", "Memo"])

    out = pd.DataFrame()
    out[CATEGORY_COL] = df[category_col].astype(str).str.strip()
    out["CSVType"] = csv_type

    if csv_type == "type1":
        out["Description"] = df.get("Description", pd.Series(dtype="object"))
        out["Payee"] = pd.NA
        out["Memo"] = pd.NA
    else:
        out["Description"] = pd.NA
        out["Payee"] = df.get("Payee", pd.Series(dtype="object"))
        out["Memo"] = df.get("Memo", pd.Series(dtype="object"))

    out = out[(out[CATEGORY_COL] != "") & out[CATEGORY_COL].notna()]
    return out.drop_duplicates()


def _build_reference_from_history(history_df: pd.DataFrame, category_col: str) -> pd.DataFrame:
    rows = []
    if "Description" in history_df.columns:
        rows.append(_extract_reference_rows(history_df, "type1", category_col))
    if {"Payee", "Memo"}.issubset(history_df.columns):
        rows.append(_extract_reference_rows(history_df, "type2", category_col))
    if not rows:
        return pd.DataFrame(columns=[CATEGORY_COL, "CSVType", "Description", "Payee", "Memo"])
    out = pd.concat(rows, ignore_index=True)
    out = out.drop_duplicates(subset=[CATEGORY_COL, "CSVType", "Description", "Payee", "Memo"], keep="last")
    return out


def _majority_category(series: pd.Series) -> str:
    counts = series.dropna().astype(str).str.strip().value_counts()
    return "" if counts.empty else str(counts.index[0])


def _build_reference_lookups(reference_df: pd.DataFrame) -> tuple[dict[str, str], dict[tuple[str, str], str]]:
    type1_map: dict[str, str] = {}
    type2_map: dict[tuple[str, str], str] = {}

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


def _coerce_date_columns_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["Date", "Date Processed", "Date of Transaction"]:
        if col not in out.columns:
            continue
        parsed_dayfirst = pd.to_datetime(out[col], errors="coerce", dayfirst=True)
        parsed_monthfirst = pd.to_datetime(out[col], errors="coerce", dayfirst=False)
        out[col] = parsed_dayfirst.fillna(parsed_monthfirst)
    return out


def _merge_for_export(history_df: pd.DataFrame, edited_df: pd.DataFrame) -> pd.DataFrame:
    history = history_df.copy()
    if CATEGORY_COL not in history.columns:
        history[CATEGORY_COL] = pd.NA

    incoming = edited_df.copy()
    incoming = incoming[~incoming["DuplicateFlag"].fillna(False)].copy()
    incoming[CATEGORY_COL] = incoming["FinalCategorisation"]
    incoming = incoming.drop(
        columns=["SuggestedCategorisation", "FinalCategorisation", "MatchStatus", "DuplicateFlag", "DuplicateReason"],
        errors="ignore",
    )

    all_columns = list(dict.fromkeys(history.columns.tolist() + incoming.columns.tolist()))
    merged = pd.concat([history.reindex(columns=all_columns), incoming.reindex(columns=all_columns)], ignore_index=True)
    return _coerce_date_columns_for_excel(merged)


def _render_reference_view(reference_df: pd.DataFrame) -> None:
    st.subheader("Reference Set (from Household_Expenses.xlsx)")
    if reference_df.empty:
        st.info("No reference rows found in Household_Expenses.xlsx yet.")
        return

    st.write(f"Reference rows: {len(reference_df)}")
    for category in sorted(reference_df[CATEGORY_COL].dropna().astype(str).unique()):
        cat_df = reference_df[reference_df[CATEGORY_COL].astype(str) == category]
        with st.expander(f"{category} ({len(cat_df)} rows)", expanded=False):
            type1 = cat_df[cat_df["CSVType"] == "type1"][["Description"]].dropna().drop_duplicates()
            if not type1.empty:
                st.markdown("**CSV Type 1 - Description**")
                st.dataframe(type1, use_container_width=True, hide_index=True)
            type2 = cat_df[cat_df["CSVType"] == "type2"][["Payee", "Memo"]].dropna(how="all").drop_duplicates()
            if not type2.empty:
                st.markdown("**CSV Type 2 - Payee + Memo**")
                st.dataframe(type2, use_container_width=True, hide_index=True)


with st.sidebar:
    st.header("1) Upload New CSV")
    new_csv_file = st.file_uploader("New transactions (.csv)", type=["csv"], key="csv")

if not new_csv_file:
    st.info("Upload a CSV to continue.")
    st.stop()

try:
    history_df = _load_main_workbook(MAIN_WORKBOOK_PATH)
except ValueError as exc:
    st.error(str(exc))
    st.caption("Tip: Commit a workbook named Household_Expenses.xlsx at the repo root.")
    st.stop()

history_category_col = _first_existing(history_df.columns.tolist(), [CATEGORY_COL, "Category", "category"])
if not history_category_col:
    st.error("Household_Expenses.xlsx must contain a category column (Categorisation or Category).")
    st.stop()

try:
    new_df, csv_type = _load_supported_csv(new_csv_file)
except ValueError as exc:
    st.error(str(exc))
    st.stop()

new_df, duplicate_report = _annotate_duplicates(new_df, history_df)

st.subheader("2) Auto-Categorization")
st.caption(f"Detected CSV format: {csv_type}")
if not duplicate_report.empty:
    st.warning(f"Duplicate transactions detected: {len(duplicate_report)}. They will be excluded from merge.")
    st.dataframe(duplicate_report, use_container_width=True, hide_index=True)
else:
    st.caption("No duplicates detected in uploaded transactions.")

reference_df = _build_reference_from_history(history_df, history_category_col)
_render_reference_view(reference_df)

if st.button("Run Auto-Categorization", type="primary"):
    predicted_df = _suggest_categories_from_reference(new_df, csv_type, reference_df)
    st.session_state["predicted_df"] = predicted_df

if "predicted_df" not in st.session_state:
    st.stop()

st.subheader("3) Review and Approve")
predicted_df = st.session_state["predicted_df"].copy()
match_count = int((predicted_df["SuggestedCategorisation"].astype(str).str.strip() != "").sum())
st.caption(f"Matched from Household_Expenses.xlsx: {match_count} of {len(predicted_df)} transactions")

edited_df = st.data_editor(
    predicted_df,
    use_container_width=True,
    num_rows="fixed",
    column_config={
        "SuggestedCategorisation": st.column_config.TextColumn("SuggestedCategorisation", disabled=True),
        "FinalCategorisation": st.column_config.TextColumn(
            "FinalCategorisation",
            help="Enter manually when no suggestion is provided.",
        ),
        "DuplicateFlag": st.column_config.CheckboxColumn("DuplicateFlag", disabled=True),
        "DuplicateReason": st.column_config.TextColumn("DuplicateReason", disabled=True),
    },
)

approve = st.checkbox("I approve these categories and want to merge into Household_Expenses.xlsx")
if approve and st.button("Merge and Download Updated Workbook", type="primary"):
    missing = (edited_df["FinalCategorisation"].fillna("").astype(str).str.strip() == "") & (~edited_df["DuplicateFlag"].fillna(False))
    if bool(missing.any()):
        st.error("Some non-duplicate transactions still have blank FinalCategorisation. Please complete them before merging.")
        st.stop()

    merged = _merge_for_export(history_df, edited_df)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        merged.to_excel(writer, index=False, sheet_name="Master")

    file_name = "Household_Expenses.xlsx"

    st.success("Merged successfully. Download the updated workbook.")
    st.download_button(
        label="Download updated workbook",
        data=output.getvalue(),
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
