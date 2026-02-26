# Household Expenditure Categorizer

A Streamlit app that:
1. Requires you to upload `Household_Expenses.xlsx` each session (the workbook is not stored in the repository).
2. Displays a dashboard from the uploaded workbook with month/quarter/year toggle, category summary table, stacked bar chart, and persistent category filters.
3. Loads new transactions from one of two supported native bank `.csv` formats.
4. Automatically detects and ignores any rows above the CSV header row.
5. Auto-categorizes with a single suggested category and match type (`Exact Match` or `Keyword Match`) from the uploaded workbook.
6. Shows all workflows on one page in sequence (dashboard -> CSV categorization -> category maintenance).
7. Stores recurring keyword mappings on a second worksheet named `KeywordRules` within `Household_Expenses.xlsx`.
8. Supports direct category maintenance without CSV merge via an "Update Existing Categories" screen.
9. Merges approved rows into an updated workbook and writes a `Categorisation` column.

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

- Upload `Household_Expenses.xlsx` each run; it is the source of truth for category suggestions.
- The workbook should contain prior labels in `Categorisation` (or `Category`).
- Auto-categorisation is strict reference lookup only:
  - CSV type 1: exact `Description` match
  - CSV type 2: exact (`Payee`, `Memo`) match
- If no reference match is found, category is left blank for manual entry.
- The app displays reference values grouped by category:
  - CSV type 1: `Description`
  - CSV type 2: `Payee` + `Memo`
- Merge output is downloaded as a new `.xlsx` file, leaving source files unchanged.
