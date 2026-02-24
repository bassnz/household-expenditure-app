from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd


@dataclass
class HistoricalModel:
    by_description: Dict[str, str]
    by_merchant: Dict[str, str]
    median_amount_by_category: Dict[str, float]
    fallback_category: Optional[str]


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip().lower()
    return " ".join(text.split())


def _majority_vote(values: pd.Series) -> Optional[str]:
    clean = values.dropna().astype(str)
    if clean.empty:
        return None
    counts = clean.value_counts()
    return str(counts.index[0])


def build_historical_model(
    history_df: pd.DataFrame,
    description_col: str,
    merchant_col: Optional[str],
    amount_col: Optional[str],
    category_col: str,
) -> HistoricalModel:
    df = history_df.copy()
    df = df.dropna(subset=[category_col])
    df["_category"] = df[category_col].astype(str).str.strip()

    if description_col not in df.columns:
        raise ValueError(f"Description column '{description_col}' not found in history file")

    df["_description_norm"] = df[description_col].map(normalize_text)

    by_description: Dict[str, str] = {}
    desc_groups = df[df["_description_norm"] != ""].groupby("_description_norm")["_category"]
    for description, categories in desc_groups:
        vote = _majority_vote(categories)
        if vote:
            by_description[description] = vote

    by_merchant: Dict[str, str] = {}
    if merchant_col and merchant_col in df.columns:
        df["_merchant_norm"] = df[merchant_col].map(normalize_text)
        merchant_groups = df[df["_merchant_norm"] != ""].groupby("_merchant_norm")["_category"]
        for merchant, categories in merchant_groups:
            vote = _majority_vote(categories)
            if vote:
                by_merchant[merchant] = vote

    median_amount_by_category: Dict[str, float] = {}
    if amount_col and amount_col in df.columns:
        amt_df = df.copy()
        amt_df["_amount"] = pd.to_numeric(amt_df[amount_col], errors="coerce")
        grouped = amt_df.dropna(subset=["_amount"]).groupby("_category")["_amount"].median()
        median_amount_by_category = grouped.to_dict()

    fallback_category = _majority_vote(df["_category"])

    return HistoricalModel(
        by_description=by_description,
        by_merchant=by_merchant,
        median_amount_by_category=median_amount_by_category,
        fallback_category=fallback_category,
    )


def _closest_category_by_amount(amount: float, medians: Dict[str, float]) -> Optional[str]:
    if not medians:
        return None
    best_category = None
    best_distance = None
    for category, median in medians.items():
        distance = abs(amount - median)
        if best_distance is None or distance < best_distance:
            best_distance = distance
            best_category = category
    return best_category


def predict_categories(
    new_df: pd.DataFrame,
    model: HistoricalModel,
    description_col: str,
    merchant_col: Optional[str],
    amount_col: Optional[str],
) -> Tuple[pd.DataFrame, List[str]]:
    if description_col not in new_df.columns:
        raise ValueError(f"Description column '{description_col}' not found in CSV file")

    df = new_df.copy()
    df["_description_norm"] = df[description_col].map(normalize_text)

    if merchant_col and merchant_col in df.columns:
        df["_merchant_norm"] = df[merchant_col].map(normalize_text)
    else:
        df["_merchant_norm"] = ""

    if amount_col and amount_col in df.columns:
        df["_amount"] = pd.to_numeric(df[amount_col], errors="coerce")
    else:
        df["_amount"] = pd.NA

    predictions: List[str] = []
    confidence_reasons: List[str] = []

    for _, row in df.iterrows():
        description_key = row["_description_norm"]
        merchant_key = row["_merchant_norm"]

        if description_key and description_key in model.by_description:
            predictions.append(model.by_description[description_key])
            confidence_reasons.append("matched exact historical description")
            continue

        if merchant_key and merchant_key in model.by_merchant:
            predictions.append(model.by_merchant[merchant_key])
            confidence_reasons.append("matched historical merchant")
            continue

        amount = row["_amount"]
        if pd.notna(amount):
            amount_guess = _closest_category_by_amount(float(amount), model.median_amount_by_category)
            if amount_guess:
                predictions.append(amount_guess)
                confidence_reasons.append("nearest historical median amount")
                continue

        predictions.append(model.fallback_category or "Uncategorized")
        confidence_reasons.append("fallback to most common category")

    output_df = new_df.copy()
    output_df["PredictedCategory"] = predictions
    output_df["PredictionReason"] = confidence_reasons
    return output_df, sorted(set(predictions))
