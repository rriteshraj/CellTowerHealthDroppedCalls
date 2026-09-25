# Cell Tower Health & Dropped Calls

Deployment-ready Databricks + Snowflake + Streamlit capstone for Topic 31.

## Pipeline
Raw generator → Bronze → Silver → Gold → CSV → Snowflake → Streamlit dashboard.

Expected checks from the supplied brief:
- Bronze towers: 120
- Bronze calls: 376,200
- Silver calls: 372,150
- Gold rows: 43,142
- Overall drop rate: about 1.82%

## Run
1. Import the `databricks/` scripts into Databricks.
2. Change `MY_ID` in `00_setup.py`.
3. Run 00 → 01 → 02 → 03 → 04 → 05.
4. Upload the exported Gold CSV to Snowflake and run `snowflake/01_setup_and_queries.sql`.
5. Deploy `streamlit/app.py` to Streamlit Cloud using `streamlit/requirements.txt`.
6. Add the Snowflake credentials from `secrets.toml.example` to Streamlit Secrets.

The supplied brief requires a scheduled Databricks Job, so `databricks/job.json` is included.
