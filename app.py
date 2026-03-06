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

# --- MONGODB ANBINDUNG ---
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

def ist_doc_gebuehr(name):
    n = str(name).lower()
    return any(x in n for x in ['b/l', 'bl', 'doc', 'documentation', 'bill of lading'])

def berechne_gebuehren(zuschlaege_str):
    if not isinstance(zuschlaege_str, str) or not zuschlaege_str: return []
    treffer = re.findall(r'([A-Za-z0-9\s\-\']+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})', zuschlaege_str)
    return [{"name": t[0].strip(), "betrag": float(t[1].replace(',', '.')), "waehrung": t[2].upper()} for t in treffer]

def berechne_total_eur_dynamic(row, price_col, prep_col, coll_col, row_index):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    if pd.isna(basis) or basis <= 0: return 999999
    basis_eur = basis * usd_to_eur if str(row.get('Currency', 'USD')).upper() == 'USD' else basis
    summe = 0
    for g in berechne_gebuehren(str(row.get(prep_col, ''))):
        if not ist_doc_gebuehr(g['name']):
            summe += (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag']
    return basis_eur + summe

def anzeige_container_daten(row, size_label, price_col, prep_col, coll_col, row_index):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    curr = str(row.get('Currency', 'USD')).upper()
    basis_eur = basis * usd_to_eur if curr == 'USD' else basis
    
    col_basis, col_prep, col_total = st.columns([1, 2, 1])
    with col_basis:
        st.markdown(f'<div class="basis-box"><b>{size_label}</b><br>{basis:,.2f} {curr}</div>', unsafe_allow_html=True)
    with col_prep:
        st.write("**Prepaid:**")
        summe_prep = 0
        for g in berechne_gebuehren(str(row.get(prep_col, ''))):
            umg = (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag']
            summe_prep += umg
            st.write(f"➕ {g['name']}: {g['betrag']:.2f} {g['waehrung']}")
    with col_total:
        st.markdown(f'<div class="all-in-box"><b>All-In</b><br>{(basis_eur + summe_prep):.2f} EUR</div>', unsafe_allow_html=True)

# --- PDF & EXCEL PARSER ---
def lade_und_uebersetze_cached(file_name, file_bytes):
    datei = io.BytesIO(file_bytes)
    datei.name = file_name
    
    if datei.name.lower().endswith('.pdf'):
        try:
            with pdfplumber.open(datei) as pdf:
                full_text = "\n".join([p.extract_text() for p in pdf.pages if p.extract_text()])
            
            raten = []
            prepaid, collect = [], []
            
            # Metadaten
            contract = re.search(r'(?:Reference|Contract)[\s:]*([A-Z0-9]{10,16})', full_text, re.I)
            contract_no = contract.group(1) if contract else "Unbekannt"
            valid = re.findall(r'(\d{2}\.\d{2}\.\d{4})', full_text)
            v_from = valid[0] if len(valid) > 0 else "Unbekannt"
            v_to = valid[1] if len(valid) > 1 else "Unbekannt"
            
            # Surcharges (PSS, ETS, ERC etc.)
            patterns = [
                (r'(PSS|Peak Season).*?(\d{2,4})[,.]\d{2}\s*(EUR|USD)', "PSS"),
                (r'(ETS|Emissions).*?(\d{1,3})[,.]\d{2}\s*(EUR|USD)', "ETS"),
                (r'(ERC|Logistic).*?(\d{1,3})[,.]\d{2}\s*(EUR|USD)', "ERC"),
                (r'(FEU|Fuel).*?(\d{1,3})[,.]\d{2}\s*(EUR|USD)', "FEU")
            ]
            for p, code in patterns:
                m = re.search(p, full_text, re.I)
                if m: prepaid.append(f"{code} = {m.group(2)}.00 {m.group(3).upper()}")

            # Raten-Extraktion (Hybrid)
            current_pod = "Unbekannt"
            global_pol = "Hamburg" if "Hamburg" in full_text else "Unbekannt"
            
            for line in full_text.split('\n'):
                line = line.strip()
                if "Port of Discharge" in line and len(line) > 20:
                    current_pod = line.split("Discharge")[-1].replace(":", "").strip().title()
                
                # Suche nach Raten-Zeilen (z.B. "Jeddah 400,00 USD" oder "via POL 350 EUR")
                rate_m = re.search(r'([\d\.]{3,5})[,.]\d{2}\s*(USD|EUR)', line)
                if rate_m:
                    pol = global_pol
                    pod = current_pod
                    if "via POL" in line:
                        pol = line.split("via POL")[-1].split()[0].upper()
                    elif current_pod == "Unbekannt":
                        pod = line.split()[0].title()
                    
                    if pod != "Unbekannt":
                        raten.append({
                            'Carrier': 'MSC', 'Contract Number': contract_no,
                            'Port of Loading': pol, 'Port of Destination': pod,
                            'Valid from': v_from, 'Valid to': v_to,
                            '40HC': float(rate_m.group(1).replace('.', '')), 'Currency': rate_m.group(2).upper(),
                            'Included Prepaid Surcharges 40HC': ", ".join(prepaid),
                            'Included Collect Surcharges 40HC': "", 'Remark': 'PDF Import'
                        })

            df = pd.DataFrame(raten).drop_duplicates(subset=['Port of Destination', '40HC'])
            if not df.empty:
                df['Valid from dt'] = pd.to_datetime(df['Valid from'], dayfirst=True, errors='coerce').astype(str)
                df['Valid to dt'] = pd.to_datetime(df['Valid to'], dayfirst=True, errors='coerce').astype(str)
            return df, "PDF"
        except Exception as e: return pd.DataFrame(), f"PDF Fehler: {e}"

    else: # Excel/CSV
        try:
            df_raw = pd.read_excel(datei) if datei.name.lower().endswith('.xlsx') else pd.read_csv(datei)
            # Einfache Spaltenerkennung für Maersk/Standard-Exporte
            df_raw.columns = [str(c).strip() for c in df_raw.columns]
            if '40HC' in df_raw.columns:
                df_raw['Valid from dt'] = pd.to_datetime(df_raw.get('Valid from', datetime.now()), errors='coerce').astype(str)
                df_raw['Valid to dt'] = pd.to_datetime(df_raw.get('Valid to', datetime.now()), errors='coerce').astype(str)
                return df_raw, "Excel/CSV"
            return pd.DataFrame(), "Keine 40HC Spalte gefunden"
        except Exception as e: return pd.DataFrame(), f"Excel Fehler: {e}"

# --- UI TABS ---
tab1, tab2 = st.tabs(["🔍 Suchen", "⚙️ Upload"])
with tab1:
    cursor = list(collection.find({}))
    if cursor:
        df = pd.DataFrame(cursor)
        df['Valid from dt'] = pd.to_datetime(df['Valid from dt'], errors='coerce')
        df['Valid to dt'] = pd.to_datetime(df['Valid to dt'], errors='coerce')
        
        c1, c2 = st.columns(2)
        pol_in = c1.text_input("📍 POL")
        pod_in = c2.text_input("🏁 POD")
        
        mask = pd.Series([True]*len(df))
        if pol_in: mask &= df['Port of Loading'].str.contains(pol_in, case=False, na=False)
        if pod_in: mask &= df['Port of Destination'].str.contains(pod_in, case=False, na=False)
        
        for _, row in df[mask].sort_values(by='Port of Destination').iterrows():
            with st.expander(f"🚢 {row['Carrier']} | {row['Port of Loading']} ➡️ {row['Port of Destination']}"):
                anzeige_container_daten(row, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC', '', row.name)
    else: st.info("Datenbank leer.")

with tab2:
    ups = st.file_uploader("Dateien auswählen", accept_multiple_files=True)
    if ups and st.button("🚀 Speichern"):
        for u in ups:
            d, m = lade_und_uebersetze_cached(u.name, u.getvalue())
            if not d.empty:
                d['createdAt'] = datetime.now(timezone.utc)
                collection.insert_many(d.to_dict('records'))
                st.success(f"{u.name} geladen!")
        st.rerun()
    if st.button("🗑️ Alles löschen"):
        collection.delete_many({})
        st.rerun()
