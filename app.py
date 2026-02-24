from __future__ import annotations

import io
from datetime import datetime
from typing import Optional

import pandas as pd
import streamlit as st

from categorizer import build_historical_model, predict_categories


st.set_page_config(page_title="Transaction Categorizer", layout="wide")
st.title("Transaction Categorizer")
st.caption("Upload historical .xlsx data + a new .csv file, review predicted categories, approve, then merge.")


def _pick_column(label: str, columns: list[str], default_candidates: list[str]) -> Optional[str]:
    default_idx = 0
    lowered = [c.lower() for c in columns]
    for candidate in default_candidates:
        if candidate in lowered:
            default_idx = lowered.index(candidate)
            break
    return st.selectbox(label, options=columns, index=default_idx)


def _prepare_rows_for_master(
    edited_df: pd.DataFrame,
    history_columns: list[str],
    hist_desc_col: str,
    new_desc_col: str,
    hist_category_col: str,
    new_merchant_col: Optional[str],
    hist_merchant_col: Optional[str],
    new_amount_col: Optional[str],
    hist_amount_col: Optional[str],
) -> pd.DataFrame:
    rows = pd.DataFrame(columns=history_columns)

    # Map selected CSV columns into the corresponding master-sheet columns.
    rows[hist_desc_col] = edited_df[new_desc_col]
    rows[hist_category_col] = edited_df["PredictedCategory"]

    if hist_merchant_col and new_merchant_col:
        rows[hist_merchant_col] = edited_df[new_merchant_col]

    if hist_amount_col and new_amount_col:
        rows[hist_amount_col] = edited_df[new_amount_col]

    return rows.reindex(columns=history_columns)


with st.sidebar:
    st.header("1) Upload Files")
    history_file = st.file_uploader("Historical workbook (.xlsx)", type=["xlsx"], key="history")
    new_csv_file = st.file_uploader("New transactions (.csv)", type=["csv"], key="csv")

if not history_file or not new_csv_file:
    st.info("Upload both files to continue.")
    st.stop()

history_df = pd.read_excel(history_file)
new_df = pd.read_csv(new_csv_file)

st.subheader("2) Map Columns")
col_left, col_right = st.columns(2)
with col_left:
    st.markdown("**Historical (.xlsx) columns**")
    hist_desc_col = _pick_column("Description", history_df.columns.tolist(), ["description", "details", "memo"])
    hist_category_col = _pick_column("Category", history_df.columns.tolist(), ["category", "type"])
    hist_merchant_col = st.selectbox("Merchant (optional)", options=["<none>"] + history_df.columns.tolist())
    hist_amount_col = st.selectbox("Amount (optional)", options=["<none>"] + history_df.columns.tolist())

with col_right:
    st.markdown("**New (.csv) columns**")
    new_desc_col = _pick_column("Description", new_df.columns.tolist(), ["description", "details", "memo"])
    new_merchant_col = st.selectbox("Merchant (optional)", options=["<none>"] + new_df.columns.tolist())
    new_amount_col = st.selectbox("Amount (optional)", options=["<none>"] + new_df.columns.tolist())

hist_merchant_col = None if hist_merchant_col == "<none>" else hist_merchant_col
hist_amount_col = None if hist_amount_col == "<none>" else hist_amount_col
new_merchant_col = None if new_merchant_col == "<none>" else new_merchant_col
new_amount_col = None if new_amount_col == "<none>" else new_amount_col

if st.button("Run Auto-Categorization", type="primary"):
    try:
        model = build_historical_model(
            history_df=history_df,
            description_col=hist_desc_col,
            merchant_col=hist_merchant_col,
            amount_col=hist_amount_col,
            category_col=hist_category_col,
        )
        predicted_df, predicted_categories = predict_categories(
            new_df=new_df,
            model=model,
            description_col=new_desc_col,
            merchant_col=new_merchant_col,
            amount_col=new_amount_col,
        )
    except ValueError as exc:
        st.error(str(exc))
    else:
        st.session_state["predicted_df"] = predicted_df
        st.session_state["predicted_categories"] = predicted_categories
        st.session_state["hist_category_col"] = hist_category_col

if "predicted_df" not in st.session_state:
    st.stop()

st.subheader("3) Review and Approve")
st.write("Edit any category before approval.")

predicted_df = st.session_state["predicted_df"].copy()
categories = st.session_state["predicted_categories"]

# Ensure dropdown values include everything currently present
current_values = sorted({str(v) for v in predicted_df["PredictedCategory"].dropna().astype(str)})
category_options = sorted(set(categories + current_values))

edited_df = st.data_editor(
    predicted_df,
    use_container_width=True,
    num_rows="fixed",
    column_config={
        "PredictedCategory": st.column_config.SelectboxColumn(
            "PredictedCategory",
            options=category_options,
            required=True,
        )
    },
)

st.session_state["edited_df"] = edited_df

approve = st.checkbox("I approve these categories and want to merge into the master workbook")
if approve and st.button("Merge into Master Workbook", type="primary"):
    hist_category_col = st.session_state["hist_category_col"]
    rows_to_append = _prepare_rows_for_master(
        edited_df=edited_df,
        history_columns=history_df.columns.tolist(),
        hist_desc_col=hist_desc_col,
        new_desc_col=new_desc_col,
        hist_category_col=hist_category_col,
        new_merchant_col=new_merchant_col,
        hist_merchant_col=hist_merchant_col,
        new_amount_col=new_amount_col,
        hist_amount_col=hist_amount_col,
    )
    merged = pd.concat(
        [
            history_df,
            rows_to_append,
        ],
        ignore_index=True,
    )

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
