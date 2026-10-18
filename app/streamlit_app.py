import streamlit as st

import gold_data

st.set_page_config(page_title="Détection de fraude bancaire", page_icon=":material/shield:", layout="wide")


@st.cache_data(ttl="10m", max_entries=8)
def load(name: str, folder: str):
    return gold_data.load_table(name, folder)


st.title("Détection de fraude bancaire")
folder = str(gold_data.gold_dir())
st.caption(f"Tables gold lues dans `{folder}`")

try:
    fraud_by_hour = load("fraud_by_hour", folder)
    alerts = load("alerts", folder)
    metrics = load("model_metrics", folder)
except (FileNotFoundError, ValueError) as err:
    st.error(str(err), title="Tables gold introuvables ou invalides")
    st.stop()

all_countries = sorted(fraud_by_hour["country"].unique())
with st.sidebar:
    countries = st.multiselect("Pays", all_countries, default=all_countries, key="countries")

if not countries:
    st.warning("Sélectionne au moins un pays.")
    st.stop()

filtered = gold_data.filter_countries(fraud_by_hour, countries)
k = gold_data.kpis(filtered)

with st.container(horizontal=True):
    st.metric("Transactions", f"{k['transactions']:,}", border=True)
    st.metric("Fraudes", f"{k['frauds']:,}", border=True)
    st.metric("Taux de fraude", f"{k['fraud_rate']:.2%}", border=True)

left, right = st.columns(2)
with left:
    with st.container(border=True):
        st.subheader("Taux de fraude par pays")
        st.bar_chart(gold_data.rate_by_country(filtered), x="country", y="fraud_rate", alt="Taux de fraude par pays")
with right:
    with st.container(border=True):
        st.subheader("Taux de fraude par heure de la journée")
        st.bar_chart(
            gold_data.rate_by_hour_of_day(filtered),
            x="hour_of_day",
            y="fraud_rate",
            alt="Taux de fraude par heure de la journée",
        )

with st.container(border=True):
    st.subheader("Alertes les plus fortes")
    st.dataframe(alerts.sort_values("score", ascending=False).head(200), hide_index=True, alt="Alertes triées par score")

with st.container(border=True):
    st.subheader("Qualité de la détection")
    st.dataframe(metrics, hide_index=True, alt="Précision et rappel par jeu de données")
