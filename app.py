import streamlit as st
import pandas as pd
import re
import warnings
import io
import requests
import pymongo
import pdfplumber
from datetime import datetime, timezone

# 1. Warnungen unterdrücken
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 2. Konfiguration & Design
st.set_page_config(page_title="Raten-Finder Pro (40'HC)", layout="wide")

st.markdown("""
    <style>
    .all-in-box { background-color: #e6f4ea; border: 2px solid #28a745; padding: 15px; border-radius: 10px; text-align: center; }
    .basis-box { background-color: #e8f0fe; border: 1px solid #1a73e8; padding: 15px; border-radius: 10px; text-align: center; }
    .fremd-waehrung { color: #d9534f; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# --- MONGODB ANBINDUNG (SICHER ÜBER SECRETS) ---
MONGO_URI = st.secrets["MONGO_URI"]

@st.cache_resource
def init_db():
    client = pymongo.MongoClient(MONGO_URI)
    db = client["SpeditionsDB"]
    collection = db["Raten"]
    collection.create_index("createdAt", expireAfterSeconds=15552000)
    return collection

collection = init_db()

# --- LIVE WECHSELKURS ---
@st.cache_data(ttl=3600)
def hole_live_wechselkurs():
    try:
        response = requests.get("https://api.frankfurter.app/latest?from=USD&to=EUR", timeout=5)
        return round(response.json()['rates']['EUR'], 3)
    except Exception: return 0.92

aktueller_kurs = hole_live_wechselkurs()
st.sidebar.header("💱 Einstellungen")
usd_to_eur = st.sidebar.number_input("Wechselkurs: 1 USD in EUR", value=aktueller_kurs, step=0.01)

# --- HILFSFUNKTIONEN ---
def berechne_gebuehren(zuschlaege_str):
    if not isinstance(zuschlaege_str, str) or not zuschlaege_str: return []
    treffer = re.findall(r'([A-Za-z0-9\s\-\']+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})', zuschlaege_str)
    return [{"name": t[0].strip(), "betrag": float(t[1].replace(',', '.')), "waehrung": t[2].upper()} for t in treffer]

def berechne_total_eur_dynamic(row, price_col, prep_col):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    if pd.isna(basis) or basis <= 0: return 999999
    basis_eur = basis * usd_to_eur if str(row.get('Currency', 'USD')).upper() == 'USD' else basis
    summe = 0
    for g in berechne_gebuehren(str(row.get(prep_col, ''))):
        summe += (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag']
    return basis_eur + summe

def anzeige_container_daten(row, size_label, price_col, prep_col):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    curr = str(row.get('Currency', 'USD')).upper()
    basis_eur = basis * usd_to_eur if curr == 'USD' else basis
    
    col_basis, col_prep, col_total = st.columns([1, 2, 1])
    with col_basis:
        st.markdown(f'<div class="basis-box"><b>{size_label}</b><br>{basis:,.2f} {curr}</div>', unsafe_allow_html=True)
    with col_prep:
        st.write("**Zuschläge:**")
        summe_prep = 0
        for g in berechne_gebuehren(str(row.get(prep_col, ''))):
            umg = (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag']
            summe_prep += umg
            st.write(f"➕ {g['name']}: {g['betrag']:.2f} {g['waehrung']}")
    with col_total:
        st.markdown(f'<div class="all-in-box"><b>Gesamt All-In</b><br>{(basis_eur + summe_prep):.2f} EUR</div>', unsafe_allow_html=True)

# --- DATEI-VERARBEITUNG ---
def lade_und_uebersetze_cached(file_name, file_bytes):
    datei = io.BytesIO(file_bytes)
    datei.name = file_name
    
    if datei.name.lower().endswith('.pdf'):
        try:
            with pdfplumber.open(datei) as pdf:
                full_text = "\n".join([p.extract_text() for p in pdf.pages if p.extract_text()])
            
            raten = []
            prepaid = []
            
            # Surcharge Detection
            patterns = [(r'(PSS|Peak Season).*?(\d{2,4})[,.]\d{2}\s*(EUR|USD)', "PSS"), (r'(ETS|Emissions).*?(\d{1,3})[,.]\d{2}\s*(EUR|USD)', "ETS")]
            for p, code in patterns:
                m = re.search(p, full_text, re.I)
                if m: prepaid.append(f"{code} = {m.group(2)}.00 {m.group(3).upper()}")

            current_pod, global_pol = "Unbekannt", "Hamburg"
            for line in full_text.split('\n'):
                if "Port of Discharge" in line: current_pod = line.split("Discharge")[-1].replace(":", "").strip().title()
                rate_m = re.search(r'([\d\.]{3,5})[,.]\d{2}\s*(USD|EUR)', line)
                if rate_m:
                    pol = global_pol
                    if "via POL" in line: pol = line.split("via POL")[-1].split()[0].upper()
                    pod = current_pod if current_pod != "Unbekannt" else line.split()[0].title()
                    raten.append({
                        'Carrier': 'MSC', 'Contract Number': 'PDF Import',
                        'Port of Loading': pol, 'Port of Destination': pod,
                        'Valid from': 'Siehe PDF', 'Valid to': 'Siehe PDF',
                        '40HC': float(rate_m.group(1).replace('.', '')), 'Currency': rate_m.group(2).upper(),
                        'Included Prepaid Surcharges 40HC': ", ".join(prepaid),
                        'Valid from dt': str(datetime.now().date()), 'Valid to dt': '2026-12-31'
                    })
            return pd.DataFrame(raten), "PDF"
        except Exception as e: return pd.DataFrame(), f"PDF Fehler: {e}"
    else:
        try:
            df = pd.read_excel(datei) if datei.name.lower().endswith('.xlsx') else pd.read_csv(datei)
            df.columns = [str(c).strip() for c in df.columns]
            # Maersk Tender Check
            if '40HDRY' in df.columns and 'Charge' in df.columns:
                res = []
                for n, g in df.dropna(subset=['40HDRY']).groupby(['POL', 'POD', 'Effective Date']):
                    bas = g[g['Charge'] == 'BAS']
                    if not bas.empty:
                        val = str(bas['40HDRY'].values[0]).split()
                        res.append({'Carrier': 'Maersk', 'Port of Loading': n[0], 'Port of Destination': n[1], '40HC': float(val[1].replace(',', '')), 'Currency': val[0], 'Valid from dt': str(n[2]), 'Valid to dt': '2026-06-30'})
                return pd.DataFrame(res), "Tender"
            return df, "Standard"
        except Exception as e: return pd.DataFrame(), f"Excel Fehler: {e}"

# --- UI LOGIK ---
tab1, tab2 = st.tabs(["🔍 Suche", "⚙️ Admin"])
with tab1:
    data = list(collection.find({}))
    if data:
        df = pd.DataFrame(data)
        pol_q = st.text_input("📍 Ladehafen")
        pod_q = st.text_input("🏁 Zielhafen")
        mask = pd.Series([True]*len(df))
        if pol_q: mask &= df['Port of Loading'].str.contains(pol_q, case=False, na=False)
        if pod_q: mask &= df['Port of Destination'].str.contains(pod_q, case=False, na=False)
        for _, r in df[mask].iterrows():
            with st.expander(f"{r['Port of Loading']} ➡️ {r['Port of Destination']} ({r['Carrier']})"):
                anzeige_container_daten(r, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC')
    else: st.info("Keine Daten vorhanden.")

with tab2:
    f = st.file_uploader("Upload", accept_multiple_files=True)
    if f and st.button("Speichern"):
        for u in f:
            d, m = lade_und_uebersetze_cached(u.name, u.getvalue())
            if not d.empty:
                d['createdAt'] = datetime.now(timezone.utc)
                collection.insert_many(d.to_dict('records'))
                st.success(f"{u.name} hochgeladen.")
        st.rerun()
    if st.button("🗑️ Alles löschen"):
        collection.delete_many({})
        st.rerun()
