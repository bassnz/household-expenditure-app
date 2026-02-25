# Household Expenditure Categorizer

A Streamlit app that:
1. Loads historical categorized transactions from `Household_Expenses.xlsx` in the repository root.
2. Displays a dashboard from `Household_Expenses.xlsx` with month/quarter/year toggle, category summary table, and stacked bar chart.
2. Loads new transactions from one of two supported native bank `.csv` formats.
3. Automatically detects and ignores any rows above the CSV header row.
4. Auto-categorizes by exact reference match from `Household_Expenses.xlsx`.
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

- `Household_Expenses.xlsx` is the source of truth for category suggestions.
- The workbook should contain prior labels in `Categorisation` (or `Category`).
- Auto-categorisation is strict reference lookup only:
  - CSV type 1: exact `Description` match
  - CSV type 2: exact (`Payee`, `Memo`) match
- If no reference match is found, category is left blank for manual entry.
- The app displays reference values grouped by category:
  - CSV type 1: `Description`
  - CSV type 2: `Payee` + `Memo`
- Merge output is downloaded as a new `.xlsx` file, leaving source files unchanged.
