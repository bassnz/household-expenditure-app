# Household Expenditure Categorizer

A Streamlit app that:
1. Loads historical categorized transactions from a `.xlsx` workbook.
2. Loads new transactions from one of two supported native bank `.csv` formats.
3. Automatically detects and ignores any rows above the CSV header row.
4. Auto-categorizes using historical patterns.
5. Pauses for manual review/approval.
6. Merges approved rows into an updated master workbook and writes a `Categorisation` column.

## Supported CSV Headers

Format 1:
- `Date Processed`
- `Date of Transaction`
- `Unique Id`
- `Tran Type`
- `Reference`
- `Description`
- `Amount`

Format 2:
- `Date`
- `Unique Id`
- `Tran Type`
- `Cheque Number`
- `Payee`
- `Memo`
- `Amount`

## Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

## Notes

- Historical workbook should contain prior labels in `Categorisation` (or `Category`).
- Prediction order is: description match -> merchant match -> amount similarity -> fallback most-common category.
- Merge output is downloaded as a new `.xlsx` file, leaving source files unchanged.
