"""CSV read / write utilities for the pipeline."""

import json
import logging

import pandas as pd

from models import ProspectInput

logger = logging.getLogger("custom_messaging")


def _find_column(df: pd.DataFrame, target: str) -> str:
    """Case-insensitive column lookup; raises ValueError if missing."""
    for col in df.columns:
        if col.strip().lower() == target:
            return col
    raise ValueError(
        f"Missing required column: '{target}'. "
        f"Found columns: {list(df.columns)}"
    )


def read_input_csv(path: str) -> tuple[list[ProspectInput], pd.DataFrame]:
    """Read the input CSV.

    Returns:
        (list of ProspectInput aligned with DataFrame rows, original DataFrame)
    """
    df = pd.read_csv(path)

    # Locate columns (case-insensitive)
    name_col = _find_column(df, "company_name")
    web_col = _find_column(df, "company_website")
    li_col = _find_column(df, "company_linkedin_url")

    prospects: list[ProspectInput] = []
    for _, row in df.iterrows():
        prospects.append(
            ProspectInput(
                company_name=str(row[name_col]).strip(),
                company_website=str(row[web_col]).strip(),
                company_linkedin_url=str(row[li_col]).strip(),
            )
        )

    return prospects, df


def write_output_csv(
    df: pd.DataFrame,
    results: list[dict],
    output_path: str,
) -> None:
    """Write the output CSV with prospect_brief and custom_messaging columns.

    ``results`` must be in the same order as the rows in ``df``
    (may be shorter if dry-run was used; extra rows get empty values).
    """
    df_out = df.copy()

    brief_col = [""] * len(df_out)
    msg_col = [""] * len(df_out)
    out1_col = [""] * len(df_out)
    out2_col = [""] * len(df_out)
    out3_col = [""] * len(df_out)

    for i, r in enumerate(results):
        if r.get("brief"):
            brief_col[i] = json.dumps(r["brief"])
        if r.get("messaging"):
            msg_col[i] = r["messaging"]
        out1_col[i] = r.get("custom_message_output_1", "")
        out2_col[i] = r.get("custom_message_output_2", "")
        out3_col[i] = r.get("custom_message_output_3", "")

    df_out["prospect_brief"] = brief_col
    df_out["custom_messaging"] = msg_col
    df_out["custom_message_output_1"] = out1_col
    df_out["custom_message_output_2"] = out2_col
    df_out["custom_message_output_3"] = out3_col

    df_out.to_csv(output_path, index=False)
    logger.info(f"Output written to {output_path}")


def write_errors_csv(errors: list[dict], output_path: str) -> None:
    """Write a companion _errors.csv alongside the output."""
    if not errors:
        return
    error_path = output_path.replace(".csv", "_errors.csv")
    pd.DataFrame(errors).to_csv(error_path, index=False)
    logger.info(f"Errors written to {error_path}")
