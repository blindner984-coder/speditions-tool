import streamlit as st
import pandas as pd
import re
import PyPDF2
import warnings
import io
import requests
import pymongo
from datetime import datetime, timezone

# 1. Warnungen unterdrücken
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 2. Konfiguration & Design
st.set_page_config(page_title="Raten-Finder Pro (40'HC)", layout="wide")

st.markdown("""
    <style>
    /* Standard-Boxen Styling */
    .all-in-box { background-color: #e6f4ea; border: 2px solid #28a745; padding: 15px; border-radius: 10px; text-align: center; }
    .basis-box { background-color: #e8f0fe; border: 1px solid #1a73e8; padding: 15px; border-radius: 10px; text-align: center; }
    .fremd-waehrung { color: #d9534f; font-weight: bold; }
    
    /* 🔴 NEU: Spezielles Styling für "Papier"-Dokumente */
    .papier-row { 
        background-color: #fff5f5; /* Leichter Rotton */
        border: 2px solid #feb2b2; 
        padding: 10px; 
        border-radius: 10px; 
        margin-bottom: 5px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- LOGO IN DER SIDEBAR ---
try:
    st.sidebar.image("logo_farbig.png", use_container_width=True) 
except FileNotFoundError:
    pass 

# --- HAUPT-ÜBERSCHRIFT ---
st.title("🚢 Speditions-Raten-Finder (Cloud-Datenbank)")

# --- MONGODB ANBINDUNG ---
MONGO_URI = "mongodb+srv://blindner984_db_user:GtCR5qnPJeGKGpbe@cluster0.yc0llqz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

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
    except Exception:
        return 0.92

aktueller_kurs = hole_live_wechselkurs()
st.sidebar.header("💱 Einstellungen")
usd_to_eur = st.sidebar.number_input("Wechselkurs: 1 USD in EUR", value=aktueller_kurs, step=0.01)

# --- HILFSFUNKTIONEN ---
def ist_doc_gebuehr(name):
    n = str(name).lower()
    return 'b/l' in n or any(w in n for w in ['bl', 'doc', 'docs', 'documentation', 'bill of lading'])

def berechne_gebuehren(zuschlaege_str):
    if not isinstance(zuschlaege_str, str) or zuschlaege_str.lower() in ['nan', 'none', '']: return []
    treffer = re.findall(r'([A-Za-z0-9\s\(\)\-]+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})', zuschlaege_str)
    liste = []
    for t in treffer:
        try: liste.append({"name": t[0].strip().lstrip(','), "betrag": float(t[1].replace('.', '').replace(',', '.')), "waehrung": t[2].upper()})
        except: pass
    return liste

def berechne_total_eur_dynamic(row, price_col, prep_surcharge_col, coll_surcharge_col, row_index):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    if pd.isna(basis) or basis <= 0: return 99999999 
    basis_eur = basis * usd_to_eur if str(row.get('Currency', 'USD')).upper() == 'USD' else basis
    summe_gebuehren_eur = 0
    for g in berechne_gebuehren(str(row.get(prep_surcharge_col, ''))):
        if not ist_doc_gebuehr(g['name']):
            summe_gebuehren_eur += (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag'] if g['waehrung'] == 'EUR' else 0
    for i, g in enumerate(berechne_gebuehren(str(row.get(coll_surcharge_col, '')))):
        if not ist_doc_gebuehr(g['name']) and st.session_state.get(f"chk_{row_index}_{i}_{g['name']}", False):
            summe_gebuehren_eur += (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag'] if g['waehrung'] == 'EUR' else 0
    return basis_eur + summe_gebuehren_eur

def anzeige_container_daten(row, size_label, price_col, prep_surcharge_col, coll_surcharge_col, row_index):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    curr_basis = str(row.get('Currency', 'USD')).upper()
    basis_eur = basis * usd_to_eur if curr_basis == 'USD' else basis
    prep_gebuehren = berechne_gebuehren(str(row.get(prep_surcharge_col, '')))
    coll_gebuehren = berechne_gebuehren(str(row.get(coll_surcharge_col, '')))
    summe_gebuehren_eur, fremd_gebuehren, doc_gebuehren = 0, [], [] 
    
    col_basis, col_prep, col_coll, col_doc, col_total = st.columns([1, 1.1, 1.1, 1.1, 1.2])
    with col_basis: st.markdown(f'<div class="basis-box"><b>Basisfracht {size_label}</b><br><span style="font-size:20px;">{basis:,.2f} {curr_basis}</span><br><small>≈ {basis_eur:.2f} EUR</small></div>', unsafe_allow_html=True)
    with col_prep:
        st.write("**Prepaid:**")
        for g in prep_gebuehren:
            if ist_doc_gebuehr(g['name']): doc_gebuehren.append(g); continue
            if g['waehrung'] == 'USD':
                umg = g['betrag'] * usd_to_eur
                summe_gebuehren_eur += umg
                st.write(f"➕ {g['name']}: {g['betrag']:.2f} USD")
            elif g['waehrung'] == 'EUR':
                summe_gebuehren_eur += g['betrag']
                st.write(f"➕ {g['name']}: {g['betrag']:.2f} EUR")
    with col_coll:
        st.write("**🏢 Collect:**")
        for i, g in enumerate(coll_gebuehren):
            if ist_doc_gebuehr(g['name']): doc_gebuehren.append(g); continue
            if st.checkbox(f"{g['name']} ({g['betrag']:.2f} {g['waehrung']})", key=f"chk_{row_index}_{i}_{g['name']}"):
                summe_gebuehren_eur += (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag'] if g['waehrung'] == 'EUR' else 0
    with col_doc:
        st.write("**📄 BL & Docs:**")
        for g in doc_gebuehren: st.write(f"<small>{g['name']}: {g['betrag']:.2f} {g['waehrung']}</small>", unsafe_allow_html=True)
    with col_total:
        total_eur = basis_eur + summe_gebuehren_eur
        st.markdown(f'<div class="all-in-box"><b>All-In Preis</b><br><span style="font-size:24px; font-weight:bold;">{total_eur:.2f} EUR</span></div>', unsafe_allow_html=True)

# --- DATEI READER ---
def lade_und_uebersetze_cached(file_name, file_bytes):
    datei = io.BytesIO(file_bytes)
    datei.name = file_name
    is_papier = "papier" in file_name.lower() # 🔴 Check auf Dateinamen
    
    # Vereinfachte Logik für PDF/Excel (wie bisher)
    # [Hier würde dein bestehender Reader-Code stehen, ergänzt um:]
    # ...
    # df_return['is_papier'] = is_papier
    # return df_return

    # Dummy für dieses Beispiel (ersetze dies durch deine volle Reader-Funktion):
    if datei.name.endswith('.csv'): df = pd.read_csv(datei)
    else: df = pd.read_excel(datei)
    
    # Spaltenmapping (Minimalbeispiel passend zu deinem Format)
    df_return = pd.DataFrame()
    if 'POL' in df.columns:
        df_return['Carrier'] = ['Maersk'] * len(df)
        df_return['Port of Loading'] = df['POL']
        df_return['Port of Destination'] = df['POD']
        df_return['40HC'] = df['40HDRY'].str.extract('(\d+)').astype(float) if '40HDRY' in df.columns else 0
        df_return['Currency'] = 'USD'
        df_return['Contract Number'] = 'Papier-Rate'
        df_return['is_papier'] = is_papier
    return df_return, "Format"

# --- TABS ---
tab_suche, tab_upload = st.tabs(["🔍 Raten suchen", "⚙️ Daten hochladen (Admin)"])

with tab_suche:
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    if not df.empty:
        # Suche/Filter (wie bisher)
        # ...
        for idx, row in df.iterrows():
            # 🔴 Check auf Papier-Markierung
            ist_papier = row.get('is_papier', False)
            label = f"{'📄 [PAPIER] ' if ist_papier else ''}🚢 {row.get('Carrier')} | {row.get('Port of Loading')} ➡️ {row.get('Port of Destination')}"
            
            # Expander mit rotem Hintergrund wenn Papier
            if ist_papier:
                st.markdown('<div class="papier-row">', unsafe_allow_html=True)
                with st.expander(label):
                    anzeige_container_daten(row, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', idx)
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                with st.expander(label):
                    anzeige_container_daten(row, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', idx)

with tab_upload:
    uploaded_files = st.file_uploader("Dateien auswählen", accept_multiple_files=True)
    if uploaded_files and st.button("Speichern"):
        for f in uploaded_files:
            df_up, _ = lade_und_uebersetze_cached(f.name, f.getvalue())
            if not df_up.empty:
                df_up['createdAt'] = datetime.now(timezone.utc)
                collection.insert_many(df_up.to_dict('records'))
        st.success("Daten gespeichert!")
