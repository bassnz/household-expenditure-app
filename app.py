from __future__ import annotations

import csv
import hashlib
import io
import re
from typing import Optional
from zipfile import BadZipFile, ZipFile

import altair as alt
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
LOWER_GROUP_CATEGORIES = {"Mortgage", "Savings", "Tax", "Income", "Uncategorised", "Payments", "Home Visa", "Work Visa", "Dividend"}
KEYWORD_SHEET_NAME = "Keywords"
KEYWORD_RULE_COLUMNS = ["Keyword", CATEGORY_COL, "MatchCount", "TotalCount", "Confidence", "LastUpdated"]
APP_VERSION = "2026-02-27-dashboard-v2"

STOPWORDS = {
    "the",
    "and",
    "for",
    "from",
    "with",
    "to",
    "eftpos",
    "payment",
    "bill",
    "pmt",
    "transfer",
    "card",
    "d",
    "dd",
    "ap",
    "of",
}


st.set_page_config(page_title="Transaction Categorizer", layout="wide")
st.title("Transaction Categorizer")
st.caption(f"App version: {APP_VERSION}")
st.caption("Upload Household_Expenses.xlsx each session. Refresh keyword mappings and run CSV categorization.")


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


def _load_main_workbook_from_bytes(raw_bytes: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
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
        excel = pd.ExcelFile(io.BytesIO(raw_bytes), engine="openpyxl")
        sheet_names = excel.sheet_names
        master_sheet = "Master" if "Master" in sheet_names else sheet_names[0]
        history_df = pd.read_excel(excel, sheet_name=master_sheet)
        if KEYWORD_SHEET_NAME in sheet_names:
            keyword_df = pd.read_excel(excel, sheet_name=KEYWORD_SHEET_NAME)
        else:
            keyword_df = pd.DataFrame(columns=KEYWORD_RULE_COLUMNS)
        for col in KEYWORD_RULE_COLUMNS:
            if col not in keyword_df.columns:
                keyword_df[col] = pd.NA
        keyword_df = keyword_df[KEYWORD_RULE_COLUMNS]
        return history_df, keyword_df
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Could not read main workbook: {exc}") from exc


def _build_workbook_bytes(master_df: pd.DataFrame, keyword_rules_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        master_df.to_excel(writer, index=False, sheet_name="Master")
        keyword_rules_df.to_excel(writer, index=False, sheet_name=KEYWORD_SHEET_NAME)
    return output.getvalue()


def _prediction_signature(df: pd.DataFrame) -> str:
    sig_cols = [c for c in ["Unique Id", "Amount", "Date", "Date Processed", "Date of Transaction", "Description", "Payee", "Memo"] if c in df.columns]
    if not sig_cols:
        return str(len(df))
    payload = df[sig_cols].fillna("").astype(str)
    return str(hash(pd.util.hash_pandas_object(payload, index=False).sum()))


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


def _tokenize_keywords(text: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    return [t for t in tokens if len(t) >= 3 and not t.isdigit() and t not in STOPWORDS]


def _normalized_unique_id(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower()


def _parse_dates_to_iso(series: pd.Series) -> pd.Series:
    dayfirst = pd.to_datetime(series, errors="coerce", dayfirst=True)
    monthfirst = pd.to_datetime(series, errors="coerce", dayfirst=False)
    merged = dayfirst.fillna(monthfirst)
    return merged.dt.strftime("%Y-%m-%d").fillna("")


def _parse_datetime_series(series: pd.Series) -> pd.Series:
    dayfirst = pd.to_datetime(series, errors="coerce", dayfirst=True)
    monthfirst = pd.to_datetime(series, errors="coerce", dayfirst=False)
    return dayfirst.fillna(monthfirst)


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


def _build_keyword_rules_from_history(history_df: pd.DataFrame, category_col: str) -> pd.DataFrame:
    text_cols = [c for c in ["Description", "Payee", "Memo"] if c in history_df.columns]
    if not text_cols:
        return pd.DataFrame(columns=KEYWORD_RULE_COLUMNS)

    base = history_df[[category_col] + text_cols].copy()
    base[category_col] = base[category_col].fillna("").astype(str).str.strip()
    base = base[base[category_col] != ""]
    if base.empty:
        return pd.DataFrame(columns=KEYWORD_RULE_COLUMNS)

    token_records: list[tuple[str, str]] = []
    for _, row in base.iterrows():
        category = str(row[category_col]).strip()
        text = " ".join(str(row[col]) for col in text_cols if pd.notna(row[col]))
        for token in set(_tokenize_keywords(text)):
            token_records.append((token, category))

    if not token_records:
        return pd.DataFrame(columns=KEYWORD_RULE_COLUMNS)

    token_df = pd.DataFrame(token_records, columns=["Keyword", CATEGORY_COL])
    counts = token_df.groupby(["Keyword", CATEGORY_COL], as_index=False).size().rename(columns={"size": "MatchCount"})
    totals = counts.groupby("Keyword", as_index=False)["MatchCount"].sum().rename(columns={"MatchCount": "TotalCount"})
    category_span = counts.groupby("Keyword", as_index=False)[CATEGORY_COL].nunique().rename(columns={CATEGORY_COL: "CategoryCount"})
    merged = counts.merge(totals, on="Keyword", how="left").merge(category_span, on="Keyword", how="left")
    merged["Confidence"] = merged["MatchCount"] / merged["TotalCount"]

    # Keep recurring terms that map to exactly one category (distinct keywords only).
    merged = merged[(merged["CategoryCount"] == 1) & (merged["TotalCount"] >= 2) & (merged["MatchCount"] >= 2)]
    merged = merged.sort_values(["Keyword", "Confidence", "MatchCount"], ascending=[True, False, False])
    rules = merged.drop_duplicates(subset=["Keyword"], keep="first").copy()
    rules["LastUpdated"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    rules = rules[KEYWORD_RULE_COLUMNS]
    return rules.sort_values(["Keyword"])


def _merge_keyword_rules(existing_rules: pd.DataFrame, derived_rules: pd.DataFrame) -> pd.DataFrame:
    def _clean(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for col in KEYWORD_RULE_COLUMNS:
            if col not in out.columns:
                out[col] = pd.NA
        out = out[KEYWORD_RULE_COLUMNS]
        out["Keyword"] = out["Keyword"].fillna("").astype(str).str.strip().str.lower()
        out[CATEGORY_COL] = out[CATEGORY_COL].fillna("").astype(str).str.strip()
        out["MatchCount"] = pd.to_numeric(out["MatchCount"], errors="coerce").fillna(0).astype(int)
        out["TotalCount"] = pd.to_numeric(out["TotalCount"], errors="coerce").fillna(0).astype(int)
        out["Confidence"] = pd.to_numeric(out["Confidence"], errors="coerce").fillna(0.0)
        out = out[(out["Keyword"] != "") & (out[CATEGORY_COL] != "")]
        return out

    existing = _clean(existing_rules).drop_duplicates(subset=["Keyword"], keep="first")
    derived = _clean(derived_rules).drop_duplicates(subset=["Keyword"], keep="first")

    # Preserve existing (manually curated) keyword categorisations when present.
    merged = derived[~derived["Keyword"].isin(set(existing["Keyword"]))].copy()
    merged = pd.concat([existing, merged], ignore_index=True)
    merged = merged.sort_values(["Keyword", "Confidence", "MatchCount"], ascending=[True, False, False])
    return merged.drop_duplicates(subset=["Keyword"], keep="first")


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
    out["MatchType"] = ""

    type1_map, type2_map = _build_reference_lookups(reference_df)
    if csv_type == "type1":
        keys = out["Description"].map(_normalize_text)
        matched = keys.map(type1_map).fillna("")
    else:
        keys = list(zip(out["Payee"].map(_normalize_text), out["Memo"].map(_normalize_text)))
        matched = pd.Series([type2_map.get(k, "") for k in keys], index=out.index)

    out["SuggestedCategorisation"] = matched
    out.loc[out["SuggestedCategorisation"] != "", "MatchType"] = "Exact Match"
    out["FinalCategorisation"] = out["SuggestedCategorisation"]
    return out


def _apply_keyword_fallback_suggestions(
    df: pd.DataFrame, csv_type: str, keyword_rules: pd.DataFrame
) -> pd.DataFrame:
    out = df.copy()
    out["MatchedKeywords"] = ""
    out["KeywordScore"] = 0
    if keyword_rules.empty:
        return out

    keyword_to_category = dict(zip(keyword_rules["Keyword"], keyword_rules[CATEGORY_COL]))
    keyword_to_weight = dict(zip(keyword_rules["Keyword"], keyword_rules["Confidence"]))

    if csv_type == "type1":
        text_series = (
            _series_or_blank(out, "Description").fillna("").astype(str)
            + " "
            + _series_or_blank(out, "Reference").fillna("").astype(str)
        )
    else:
        text_series = (
            _series_or_blank(out, "Payee").fillna("").astype(str)
            + " "
            + _series_or_blank(out, "Memo").fillna("").astype(str)
        )

    for idx, text in text_series.items():
        tokens = set(_tokenize_keywords(str(text)))
        scores: dict[str, float] = {}
        matched: list[str] = []
        for token in tokens:
            cat = keyword_to_category.get(token)
            if not cat:
                continue
            weight = float(keyword_to_weight.get(token, 1.0))
            scores[cat] = scores.get(cat, 0.0) + weight
            matched.append(token)
        if not scores or str(out.at[idx, "SuggestedCategorisation"]).strip() != "":
            continue
        best_cat = max(scores.items(), key=lambda item: item[1])[0]
        out.at[idx, "SuggestedCategorisation"] = best_cat
        out.at[idx, "FinalCategorisation"] = best_cat if str(out.at[idx, "FinalCategorisation"]).strip() == "" else out.at[idx, "FinalCategorisation"]
        out.at[idx, "MatchType"] = "Keyword Match"
        out.at[idx, "MatchedKeywords"] = ", ".join(sorted(matched))
        out.at[idx, "KeywordScore"] = round(float(scores[best_cat]), 3)

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


def _update_true_date(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    source_date = pd.Series(pd.NaT, index=out.index, dtype="datetime64[ns]")

    if "Date" in out.columns:
        parsed_date = _parse_datetime_series(out["Date"])
        source_date = source_date.fillna(parsed_date)

    if "Date Processed" in out.columns:
        parsed_processed = _parse_datetime_series(out["Date Processed"])
        source_date = source_date.fillna(parsed_processed)

    out["True Date"] = source_date
    return out


def _coalesce_dashboard_dates(history_df: pd.DataFrame) -> pd.Series:
    source_date = pd.Series(pd.NaT, index=history_df.index, dtype="datetime64[ns]")
    for col in ["True Date", "Date of Transaction", "Date Processed", "Date"]:
        if col in history_df.columns:
            source_date = source_date.fillna(_parse_datetime_series(history_df[col]))
    return source_date


def _period_label(dt: pd.Timestamp, period_mode: str) -> str:
    if period_mode == "Month":
        return dt.strftime("%Y-%m")
    if period_mode == "Quarter":
        quarter = ((dt.month - 1) // 3) + 1
        return f"{dt.year}-Q{quarter}"
    return dt.strftime("%Y")


def _build_dashboard_frame(history_df: pd.DataFrame, category_col: str, period_mode: str, selected_categories: list[str]) -> pd.DataFrame:
    if "Amount" not in history_df.columns:
        return pd.DataFrame(columns=["Period", "Category", "Amount"])

    work = history_df.copy()
    work["_date"] = _coalesce_dashboard_dates(work)
    work["_amount"] = pd.to_numeric(work["Amount"], errors="coerce")
    work["_category"] = work[category_col].fillna("Uncategorised").astype(str).str.strip()
    work["_category"] = work["_category"].replace("", "Uncategorised")
    work = work.dropna(subset=["_date", "_amount"])
    work = work[work["_category"].isin(selected_categories)]
    if work.empty:
        return pd.DataFrame(columns=["Period", "Category", "Amount"])

    if period_mode == "Month":
        period_start = work["_date"].dt.to_period("M").dt.to_timestamp()
        period_index = pd.date_range(period_start.min(), period_start.max(), freq="MS")
    elif period_mode == "Quarter":
        period_start = work["_date"].dt.to_period("Q").dt.start_time
        period_index = pd.date_range(period_start.min(), period_start.max(), freq="QS")
    else:
        period_start = work["_date"].dt.to_period("Y").dt.start_time
        period_index = pd.date_range(period_start.min(), period_start.max(), freq="YS")

    period_labels = [_period_label(ts, period_mode) for ts in period_index]
    work["_period"] = work["_date"].map(lambda d: _period_label(d, period_mode))
    grouped = (
        work.groupby(["_period", "_category"], as_index=False)["_amount"]
        .sum()
        .rename(columns={"_period": "Period", "_category": "Category", "_amount": "Amount"})
    )

    full_grid = pd.MultiIndex.from_product(
        [period_labels, selected_categories],
        names=["Period", "Category"],
    ).to_frame(index=False)

    merged = full_grid.merge(grouped, on=["Period", "Category"], how="left")
    merged["Amount"] = merged["Amount"].fillna(0.0)
    return merged.sort_values(["Period", "Category"])


def _render_dashboard(history_df: pd.DataFrame, category_col: str) -> None:
    st.subheader("Spending Dashboard")
    st.caption(f"Dashboard build marker: totals+rolling | {APP_VERSION}")
    period_mode = st.radio("Time period", options=["Month", "Quarter", "Year"], horizontal=True, key="period_mode")

    all_categories = (
        history_df[category_col]
        .fillna("Uncategorised")
        .astype(str)
        .str.strip()
        .replace("", "Uncategorised")
        .sort_values()
        .unique()
        .tolist()
    )
    upper_categories = [c for c in all_categories if c not in LOWER_GROUP_CATEGORIES]
    lower_categories = [c for c in all_categories if c in LOWER_GROUP_CATEGORIES]

    st.markdown("**Category Filters - Expenses**")
    filter_cols = st.columns(4)
    selected_categories: list[str] = []
    for idx, category in enumerate(upper_categories):
        key = f"dash_cat_{hashlib.md5(category.encode('utf-8')).hexdigest()[:10]}"
        col = filter_cols[idx % 4]
        with col:
            checked = st.checkbox(category, value=st.session_state.get(key, True), key=key)
        if checked:
            selected_categories.append(category)

    st.markdown("**Category Filters - Others**")
    lower_cols = st.columns(3)
    for idx, category in enumerate(lower_categories):
        key = f"dash_cat_{hashlib.md5(category.encode('utf-8')).hexdigest()[:10]}"
        col = lower_cols[idx % 3]
        with col:
            checked = st.checkbox(category, value=st.session_state.get(key, False), key=key)
        if checked:
            selected_categories.append(category)

    rolling_window = st.number_input(
        "Rolling average periods (x)",
        min_value=1,
        value=3,
        step=1,
        key="dashboard_rolling_window",
        help="Applies to the selected period view (e.g. 3 months, 3 quarters, or 3 years).",
    )
    hide_rolling_line = st.toggle("Hide rolling average line", value=False, key="dashboard_hide_rolling")

    if not selected_categories:
        st.warning("Select at least one category to display the dashboard.")
        return

    dashboard_df = _build_dashboard_frame(history_df, category_col, period_mode, selected_categories)
    if dashboard_df.empty:
        st.warning("Dashboard could not be generated. Ensure Household_Expenses.xlsx has a date column and Amount.")
        return

    pivot = (
        dashboard_df.pivot(index="Category", columns="Period", values="Amount")
        .fillna(0.0)
        .sort_index()
    )
    totals_row = pd.DataFrame([pivot.sum(axis=0)], index=["Total"])
    pivot = pd.concat([pivot, totals_row], axis=0)

    def _fmt_accounting(value: object) -> str:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return ""
        if num < 0:
            return f"({abs(num):,.0f})"
        return f"{num:,.0f}"

    pivot_display = pivot.reset_index().rename(columns={"index": "Category"})
    value_cols = [c for c in pivot_display.columns if c != "Category"]
    st.dataframe(
        pivot_display.style.format({c: _fmt_accounting for c in value_cols}),
        use_container_width=True,
        hide_index=True,
    )

    dashboard_chart_df = dashboard_df.copy()
    dashboard_chart_df["AmountDisplay"] = dashboard_chart_df["Amount"].map(_fmt_accounting)
    totals_df = (
        dashboard_chart_df.groupby("Period", as_index=False)["Amount"]
        .sum()
        .rename(columns={"Amount": "TotalAmount"})
    )
    period_order = dashboard_df["Period"].drop_duplicates().tolist()
    totals_df["TotalDisplay"] = totals_df["TotalAmount"].map(_fmt_accounting)
    totals_df["RollingAverage"] = totals_df["TotalAmount"].rolling(window=int(rolling_window), min_periods=1).mean()

    bars = (
        alt.Chart(dashboard_chart_df)
        .mark_bar()
        .encode(
            x=alt.X("Period:N", title=period_mode, sort=period_order),
            y=alt.Y("Amount:Q", title="Total Amount"),
            color=alt.Color("Category:N", title="Categorisation"),
            tooltip=["Period", "Category", alt.Tooltip("AmountDisplay:N", title="Amount")],
        )
    )

    totals_df = totals_df.copy()
    totals_df["LabelRank"] = range(len(totals_df))
    totals_df["LabelDY"] = totals_df["LabelRank"].map(lambda i: -4 if i % 2 == 0 else -18)

    total_labels = (
        alt.Chart(totals_df)
        .mark_text(fontSize=11, clip=False)
        .encode(
            x=alt.X("Period:N", sort=period_order),
            y=alt.value(396),
            dy=alt.Datum("datum.LabelDY"),
            text=alt.Text("TotalDisplay:N"),
        )
    )

    chart = bars + total_labels
    if not hide_rolling_line:
        rolling_line = (
            alt.Chart(totals_df)
            .mark_line(color="#111827", strokeWidth=2)
            .encode(
                x=alt.X("Period:N", sort=period_order),
                y=alt.Y("RollingAverage:Q", title="Total Amount"),
                tooltip=[
                    "Period",
                    alt.Tooltip("RollingAverage:Q", title=f"Rolling Avg ({int(rolling_window)})", format=",.2f"),
                ],
            )
        )
        chart = chart + rolling_line

    st.altair_chart(chart.properties(height=430), use_container_width=True)


def _merge_for_export(history_df: pd.DataFrame, edited_df: pd.DataFrame) -> pd.DataFrame:
    history = history_df.copy()
    if CATEGORY_COL not in history.columns:
        history[CATEGORY_COL] = pd.NA

    incoming = edited_df.copy()
    incoming = incoming[~incoming["DuplicateFlag"].fillna(False)].copy()
    incoming[CATEGORY_COL] = incoming["FinalCategorisation"]
    incoming = incoming.drop(
        columns=[
            "SuggestedCategorisation",
            "FinalCategorisation",
            "MatchType",
            "MatchedKeywords",
            "KeywordScore",
            "DuplicateFlag",
            "DuplicateReason",
        ],
        errors="ignore",
    )

    all_columns = list(dict.fromkeys(history.columns.tolist() + incoming.columns.tolist()))
    merged = pd.concat([history.reindex(columns=all_columns), incoming.reindex(columns=all_columns)], ignore_index=True)
    merged = _coerce_date_columns_for_excel(merged)
    return _update_true_date(merged)


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
    st.header("1) Upload Workbook")
    main_workbook_file = st.file_uploader("Household_Expenses.xlsx", type=["xlsx"], key="main_workbook")
    st.header("2) Upload New CSV (Optional)")
    new_csv_file = st.file_uploader("New transactions (.csv)", type=["csv"], key="csv")

try:
    if main_workbook_file is None:
        raise ValueError("Upload Household_Expenses.xlsx to continue.")
    workbook_bytes = main_workbook_file.getvalue()
    workbook_sig = hashlib.md5(workbook_bytes).hexdigest()
    if st.session_state.get("loaded_workbook_sig") != workbook_sig:
        parsed_history, parsed_keywords = _load_main_workbook_from_bytes(workbook_bytes)
        st.session_state["loaded_workbook_sig"] = workbook_sig
        st.session_state["loaded_history_df"] = parsed_history
        st.session_state["loaded_keyword_rules"] = parsed_keywords
        st.session_state.pop("predicted_df", None)
        st.session_state.pop("edited_df", None)
        st.session_state.pop("prediction_signature", None)
        st.session_state.pop("keyword_rules_refreshed", None)
    history_df = st.session_state["loaded_history_df"].copy()
    keyword_rules_existing = st.session_state["loaded_keyword_rules"].copy()
except ValueError as exc:
    st.error(str(exc))
    st.stop()

history_category_col = _first_existing(history_df.columns.tolist(), [CATEGORY_COL, "Category", "category"])
if not history_category_col:
    st.error("Household_Expenses.xlsx must contain a category column (Categorisation or Category).")
    st.stop()

_render_dashboard(history_df, history_category_col)
st.divider()

st.subheader("1) Refresh Keyword Categories")
st.caption(
    "Create or update worksheet 'Keywords' from recurring words in Master fields: Description, Payee, and Memo."
)
refreshed_keywords = _build_keyword_rules_from_history(history_df, history_category_col)
keywords_to_show = refreshed_keywords if not refreshed_keywords.empty else keyword_rules_existing
if st.button("Refresh Keyword Categories", type="primary"):
    existing_for_refresh = st.session_state.get("keyword_rules_refreshed", keyword_rules_existing)
    st.session_state["keyword_rules_refreshed"] = _merge_keyword_rules(existing_for_refresh, refreshed_keywords)
    st.success("Keywords refreshed from Master. Review/edit below, then download.")

if "keyword_rules_refreshed" in st.session_state:
    keywords_to_show = st.session_state["keyword_rules_refreshed"]

hide_keyword_table = st.toggle("Hide keyword table", value=True)

if keywords_to_show.empty:
    st.info("No recurring category keywords found yet.")
elif not hide_keyword_table:
    editable_keywords = st.data_editor(
        keywords_to_show[KEYWORD_RULE_COLUMNS].sort_values(["Keyword"]).reset_index(drop=True),
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "Keyword": st.column_config.TextColumn("Keyword", disabled=True),
            "MatchCount": st.column_config.NumberColumn("MatchCount", disabled=True, format="%d"),
            "TotalCount": st.column_config.NumberColumn("TotalCount", disabled=True, format="%d"),
            "Confidence": st.column_config.NumberColumn("Confidence", disabled=True, format="%.3f"),
            CATEGORY_COL: st.column_config.TextColumn(CATEGORY_COL, help="You can recategorise a keyword here."),
        },
        key="keywords_editor",
    )
    st.session_state["keyword_rules_refreshed"] = _merge_keyword_rules(editable_keywords, pd.DataFrame(columns=KEYWORD_RULE_COLUMNS))

rules_to_write = st.session_state.get("keyword_rules_refreshed", keywords_to_show)
workbook_bytes_keywords = _build_workbook_bytes(history_df, rules_to_write)
st.download_button(
    label="Download Household_Expenses.xlsx (with Keywords)",
    data=workbook_bytes_keywords,
    file_name="Household_Expenses.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    key="download_keywords_refresh",
)

st.divider()
st.subheader("2) Auto-Categorization")
reference_df = _build_reference_from_history(history_df, history_category_col)
keyword_rules_derived = _build_keyword_rules_from_history(history_df, history_category_col)
keyword_rules = _merge_keyword_rules(keyword_rules_existing, keyword_rules_derived)
if "keyword_rules_refreshed" in st.session_state:
    keyword_rules = _merge_keyword_rules(st.session_state["keyword_rules_refreshed"], pd.DataFrame(columns=KEYWORD_RULE_COLUMNS))
hide_reference_set = st.toggle("Hide Reference Set (from Household_Expenses.xlsx)", value=True)
if not hide_reference_set:
    _render_reference_view(reference_df)

if not new_csv_file:
    st.info("Upload a CSV to run categorization.")
else:
    new_df = None
    csv_type = ""
    try:
        new_df, csv_type = _load_supported_csv(new_csv_file)
    except ValueError as exc:
        st.error(str(exc))
    if new_df is not None:
        new_df, duplicate_report = _annotate_duplicates(new_df, history_df)
        st.caption(f"Detected CSV format: {csv_type}")
        if not duplicate_report.empty:
            st.warning(f"Duplicate transactions detected: {len(duplicate_report)}. They will be excluded from merge.")
            st.dataframe(duplicate_report, use_container_width=True, hide_index=True)
        else:
            st.caption("No duplicates detected in uploaded transactions.")

        if st.button("Run Auto-Categorization", type="primary"):
            predicted_df = _suggest_categories_from_reference(new_df, csv_type, reference_df)
            predicted_df = _apply_keyword_fallback_suggestions(predicted_df, csv_type, keyword_rules)
            st.session_state["predicted_df"] = predicted_df
            st.session_state["edited_df"] = predicted_df.copy()
            st.session_state["prediction_signature"] = _prediction_signature(predicted_df)

    if "predicted_df" in st.session_state:
        st.subheader("3) Review and Approve")
        predicted_df = st.session_state["predicted_df"].copy()
        exact_count = int((predicted_df["MatchType"] == "Exact Match").sum())
        keyword_count = int((predicted_df["MatchType"] == "Keyword Match").sum())
        st.caption(
            f"Matched from Household_Expenses.xlsx: Exact={exact_count}, Keyword={keyword_count}, Total={len(predicted_df)}"
        )
        current_sig = _prediction_signature(predicted_df)
        if "edited_df" not in st.session_state or st.session_state.get("prediction_signature") != current_sig:
            st.session_state["edited_df"] = predicted_df.copy()
            st.session_state["prediction_signature"] = current_sig
        editor_key = f"primary_editor_{st.session_state['prediction_signature']}"

        edited_df = st.data_editor(
            st.session_state["edited_df"],
            use_container_width=True,
            num_rows="fixed",
            column_config={
                "SuggestedCategorisation": st.column_config.TextColumn("SuggestedCategorisation", disabled=True),
                "MatchType": st.column_config.TextColumn("MatchType", disabled=True),
                "MatchedKeywords": st.column_config.TextColumn("MatchedKeywords", disabled=True),
                "KeywordScore": st.column_config.NumberColumn("KeywordScore", disabled=True, format="%.3f"),
                "FinalCategorisation": st.column_config.TextColumn(
                    "FinalCategorisation",
                    help="Enter manually when no suggestion is provided.",
                ),
                "DuplicateFlag": st.column_config.CheckboxColumn("DuplicateFlag", disabled=True),
                "DuplicateReason": st.column_config.TextColumn("DuplicateReason", disabled=True),
            },
            key=editor_key,
        )
        st.session_state["edited_df"] = edited_df

        edited_df = st.session_state["edited_df"].copy()
        approve = st.checkbox("I approve these categories and want to merge into Household_Expenses.xlsx")
        missing = (edited_df["FinalCategorisation"].fillna("").astype(str).str.strip() == "") & (~edited_df["DuplicateFlag"].fillna(False))
        missing_count = int(missing.sum())
        needs_blank_approval = missing_count > 0

        if approve and needs_blank_approval:
            st.warning(
                f"{missing_count} non-duplicate transactions have blank FinalCategorisation. "
                "If you proceed, blanks will be kept in the merged workbook."
            )
        allow_blank = True
        if needs_blank_approval:
            warning_key = f"allow_blank_categories_{st.session_state['prediction_signature']}"
            allow_blank = st.checkbox(
                "I re-approve merge with blank FinalCategorisation values",
                key=warning_key,
            )

        can_merge = approve and ((not needs_blank_approval) or allow_blank)
        if can_merge and st.button("Merge and Download Updated Workbook", type="primary"):
            merged = _merge_for_export(history_df, edited_df)
            keyword_rules_out = _merge_keyword_rules(
                keyword_rules,
                _build_keyword_rules_from_history(merged, CATEGORY_COL if CATEGORY_COL in merged.columns else history_category_col),
            )

            workbook_bytes = _build_workbook_bytes(merged, keyword_rules_out)
            st.success("Merged successfully. Download the updated workbook.")
            st.download_button(
                label="Download updated workbook",
                data=workbook_bytes,
                file_name="Household_Expenses.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_merged_workbook",
            )
