import pandas as pd
import streamlit as st
import snowflake.connector

st.set_page_config(
    page_title="TowerPulse | Network Intelligence",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------- Premium UI ----------
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
}

.stApp {
    background: #f5f7fb;
}

.block-container {
    max-width: 1450px;
    padding-top: 2rem;
    padding-bottom: 3rem;
}

/* Hide Streamlit chrome */
#MainMenu, footer {visibility: hidden;}

.hero {
    background: linear-gradient(135deg, #111827 0%, #1e293b 55%, #312e81 100%);
    padding: 30px 34px;
    border-radius: 24px;
    color: white;
    margin-bottom: 24px;
    box-shadow: 0 12px 35px rgba(15,23,42,.14);
}

.hero .eyebrow {
    color: #a5b4fc;
    font-size: 12px;
    font-weight: 800;
    letter-spacing: 1.8px;
    text-transform: uppercase;
    margin-bottom: 8px;
}

.hero h1 {
    font-size: 34px;
    line-height: 1.15;
    margin: 0;
    font-weight: 800;
}

.hero p {
    margin: 10px 0 0;
    color: #cbd5e1;
    font-size: 14px;
}

.status {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    margin-top: 18px;
    background: rgba(255,255,255,.09);
    border: 1px solid rgba(255,255,255,.12);
    border-radius: 999px;
    padding: 7px 12px;
    font-size: 12px;
    color: #e2e8f0;
}

.dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: #34d399;
    box-shadow: 0 0 0 4px rgba(52,211,153,.12);
}

/* KPI cards */
.kpi {
    background: white;
    border: 1px solid #e5e7eb;
    border-radius: 18px;
    padding: 20px;
    min-height: 125px;
    box-shadow: 0 5px 18px rgba(15,23,42,.05);
}

.kpi-label {
    color: #64748b;
    font-size: 12px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: .7px;
}

.kpi-value {
    color: #0f172a;
    font-size: 29px;
    font-weight: 800;
    margin-top: 8px;
}

.kpi-sub {
    color: #94a3b8;
    font-size: 12px;
    margin-top: 5px;
}

/* Section heading */
.section {
    margin: 28px 0 12px;
}

.section h2 {
    color: #0f172a;
    font-size: 19px;
    font-weight: 800;
    margin: 0;
}

.section p {
    color: #64748b;
    font-size: 12px;
    margin: 4px 0 0;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #111827;
}

[data-testid="stSidebar"] * {
    color: #e5e7eb !important;
}

[data-testid="stSidebar"] .stMultiSelect div[data-baseweb="select"] {
    background: #1f2937;
    border-color: #374151;
}

.sidebar-brand {
    padding: 6px 0 22px;
}

.sidebar-brand .logo {
    font-size: 26px;
    font-weight: 800;
    color: white;
}

.sidebar-brand .small {
    color: #94a3b8;
    font-size: 11px;
    margin-top: 3px;
}

/* Cards around charts */
.chart-card {
    background: white;
    border: 1px solid #e5e7eb;
    border-radius: 18px;
    padding: 6px 10px 10px;
    box-shadow: 0 5px 18px rgba(15,23,42,.04);
}

/* Dataframe */
[data-testid="stDataFrame"] {
    border-radius: 14px;
    overflow: hidden;
}

/* Buttons */
.stButton button {
    border-radius: 10px;
    font-weight: 700;
}

/* Footer */
.footer {
    margin-top: 35px;
    padding-top: 18px;
    border-top: 1px solid #e2e8f0;
    color: #94a3b8;
    font-size: 11px;
    text-align: center;
}
</style>
""", encoding="utf-8")

# ---------- Connection ----------
@st.cache_resource
def get_connection():
    s = st.secrets["snowflake"]
    kwargs = dict(
        account=s["account"],
        user=s["user"],
        password=s["password"],
        warehouse=s["warehouse"],
        database=s["database"],
        schema=s["schema"],
    )
    if s.get("role"):
        kwargs["role"] = s["role"]
    return snowflake.connector.connect(**kwargs)

@st.cache_data(ttl=300)
def load_data():
    return pd.read_sql("""
        SELECT
            TOWER_ID, SITE_NAME, CITY, DISTRICT, CAPACITY_CHANNELS,
            CALL_DATE, HOUR_OF_DAY, CALLS, DROPPED_CALLS, DROP_RATE,
            PEAK_CONCURRENT, UTILISATION, AVG_DURATION_SECONDS
        FROM GOLD_TOWER_HOUR
    """, get_connection())

# ---------- Load ----------
try:
    df = load_data()
except Exception as e:
    st.error("Unable to connect to Snowflake.")
    st.code(str(e))
    st.stop()

df["CALL_DATE"] = pd.to_datetime(df["CALL_DATE"])

# ---------- Sidebar ----------
with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
        <div class="logo">📡 TowerPulse</div>
        <div class="small">Network health intelligence</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### Filters")

    cities = sorted(df["CITY"].dropna().unique())
    selected_cities = st.multiselect(
        "City",
        cities,
        default=cities,
    )

    tower_pool = sorted(df[df["CITY"].isin(selected_cities)]["TOWER_ID"].unique())
    selected_towers = st.multiselect(
        "Tower",
        tower_pool,
        placeholder="All towers",
    )

    st.markdown("---")
    st.markdown("**Data period**")
    st.caption(
        f"{df['CALL_DATE'].min().strftime('%d %b %Y')} — "
        f"{df['CALL_DATE'].max().strftime('%d %b %Y')}"
    )
    st.caption(f"{df['TOWER_ID'].nunique():,} towers • {len(df):,} Gold rows")

    if st.button("↻ Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ---------- Filter ----------
view = df[df["CITY"].isin(selected_cities)].copy()
if selected_towers:
    view = view[view["TOWER_ID"].isin(selected_towers)]

total_calls = int(view["CALLS"].sum())
total_drops = int(view["DROPPED_CALLS"].sum())
drop_rate = (100 * total_drops / total_calls) if total_calls else 0
tower_count = view["TOWER_ID"].nunique()
avg_util = 100 * view["UTILISATION"].mean() if len(view) else 0

# ---------- Hero ----------
st.markdown("""
<div class="hero">
    <div class="eyebrow">Network operations dashboard</div>
    <h1>Cell Tower Health & Dropped Calls</h1>
    <p>Monitor tower reliability, traffic pressure and drop-rate patterns from the Gold analytics layer.</p>
    <div class="status"><span class="dot"></span> Snowflake analytics layer connected</div>
</div>
""", unsafe_allow_html=True)

# ---------- KPIs ----------
k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(f"""
    <div class="kpi">
        <div class="kpi-label">Total calls</div>
        <div class="kpi-value">{total_calls:,}</div>
        <div class="kpi-sub">Selected scope</div>
    </div>
    """, unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="kpi">
        <div class="kpi-label">Dropped calls</div>
        <div class="kpi-value">{total_drops:,}</div>
        <div class="kpi-sub">Calls marked DROPPED</div>
    </div>
    """, unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="kpi">
        <div class="kpi-label">Drop rate</div>
        <div class="kpi-value">{drop_rate:.2f}%</div>
        <div class="kpi-sub">SUM(drops) / SUM(calls)</div>
    </div>
    """, unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class="kpi">
        <div class="kpi-label">Avg utilisation</div>
        <div class="kpi-value">{avg_util:.1f}%</div>
        <div class="kpi-sub">{tower_count:,} active towers</div>
    </div>
    """, unsafe_allow_html=True)

# ---------- Top analytics ----------
st.markdown("""
<div class="section">
    <h2>Network performance</h2>
    <p>Identify problem towers and the hours where network quality deteriorates.</p>
</div>
""", unsafe_allow_html=True)

tower = (
    view.groupby(["TOWER_ID", "CITY"], as_index=False)
        .agg(CALLS=("CALLS", "sum"), DROPS=("DROPPED_CALLS", "sum"))
)
tower["DROP_RATE_PCT"] = 100 * tower["DROPS"] / tower["CALLS"].replace(0, pd.NA)
tower = tower.sort_values("DROP_RATE_PCT", ascending=False)

hour = (
    view.groupby("HOUR_OF_DAY", as_index=False)
        .agg(CALLS=("CALLS", "sum"), DROPS=("DROPPED_CALLS", "sum"))
)
hour["DROP_RATE_PCT"] = 100 * hour["DROPS"] / hour["CALLS"].replace(0, pd.NA)

left, right = st.columns([1, 1.25])

with left:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown("**Highest drop-rate towers**")
    display_tower = tower.head(12).copy()
    display_tower["DROP_RATE_PCT"] = display_tower["DROP_RATE_PCT"].round(2)
    display_tower.columns = ["Tower", "City", "Calls", "Drops", "Drop rate %"]
    st.dataframe(display_tower, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)

with right:
    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown("**Drop rate by hour of day**")
    chart_hour = hour.set_index("HOUR_OF_DAY")[["DROP_RATE_PCT"]]
    chart_hour.columns = ["Drop rate %"]
    st.line_chart(chart_hour, height=300)
    st.markdown("</div>", unsafe_allow_html=True)

# ---------- Concurrency ----------
st.markdown("""
<div class="section">
    <h2>Traffic pressure</h2>
    <p>Explore how peak concurrent calls relate to dropped-call behaviour.</p>
</div>
""", unsafe_allow_html=True)

con = (
    view.groupby("PEAK_CONCURRENT", as_index=False)
        .agg(CALLS=("CALLS", "sum"), DROPS=("DROPPED_CALLS", "sum"))
)
con["DROP_RATE_PCT"] = 100 * con["DROPS"] / con["CALLS"].replace(0, pd.NA)

st.markdown('<div class="chart-card">', unsafe_allow_html=True)
chart_con = con.set_index("PEAK_CONCURRENT")[["DROP_RATE_PCT"]]
chart_con.columns = ["Drop rate %"]
st.line_chart(chart_con, height=300)
st.markdown("</div>", unsafe_allow_html=True)

# ---------- Daily threshold ----------
st.markdown("""
<div class="section">
    <h2>Reliability watchlist</h2>
    <p>Towers with more than 10 days above the 5% drop-rate threshold.</p>
</div>
""", unsafe_allow_html=True)

daily = (
    view.groupby(["TOWER_ID", "CALL_DATE"], as_index=False)
        .agg(CALLS=("CALLS", "sum"), DROPS=("DROPPED_CALLS", "sum"))
)
daily["DROP_RATE"] = daily["DROPS"] / daily["CALLS"].replace(0, pd.NA)

above = (
    daily.assign(ABOVE_5=daily["DROP_RATE"] > 0.05)
         .groupby("TOWER_ID", as_index=False)["ABOVE_5"]
         .sum()
         .rename(columns={"ABOVE_5": "DAYS_ABOVE_5_PCT"})
)
above["DAYS_ABOVE_5_PCT"] = above["DAYS_ABOVE_5_PCT"].astype(int)
above = above[above["DAYS_ABOVE_5_PCT"] > 10].sort_values(
    "DAYS_ABOVE_5_PCT", ascending=False
)

if len(above):
    st.dataframe(above, use_container_width=True, hide_index=True)
else:
    st.info("No towers in the selected scope exceed the 5% threshold for more than 10 days.")

# ---------- Data explorer ----------
st.markdown("""
<div class="section">
    <h2>Data explorer</h2>
    <p>Inspect the Gold tower-hour dataset behind the dashboard.</p>
</div>
""", unsafe_allow_html=True)

with st.expander("Open Gold data preview"):
    st.dataframe(view.head(500), use_container_width=True, hide_index=True)

st.markdown("""
<div class="footer">
    TowerPulse • Databricks → Gold → Snowflake → Streamlit
    <br>Cell Tower Health & Dropped Calls | Capstone Project
</div>
""", unsafe_allow_html=True)
