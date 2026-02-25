# Household Expenditure Categorizer

A Streamlit app that:
1. Loads historical categorized transactions from a `.xlsx` workbook.
2. Loads new transactions from one of two supported native bank `.csv` formats.
3. Automatically detects and ignores any rows above the CSV header row.
4. Auto-categorizes using historical patterns.
5. Pauses for manual review/approval.
6. Merges approved rows into an updated master workbook and writes a `Categorisation` column.
7. Stores approved text-to-category mappings in `categorisation_reference.csv` for future learning.

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
- The uploaded historical `.xlsx` is treated as the source of truth for initial category mappings.
- The app builds/refreshes `categorisation_reference.csv` from that workbook and then appends newly approved mappings after each run.
- Auto-categorisation is strict reference lookup only:
  - CSV type 1: exact `Description` match
  - CSV type 2: exact (`Payee`, `Memo`) match
- If no reference match is found, category is left blank for manual entry.
- The app displays the reference database grouped by category:
  - CSV type 1: `Description`
  - CSV type 2: `Payee` + `Memo`
- Merge output is downloaded as a new `.xlsx` file, leaving source files unchanged.
