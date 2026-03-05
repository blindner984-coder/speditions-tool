import streamlit as st
import pandas as pd
import re
import PyPDF2
import warnings
import io
import requests
import pymongo
from datetime import datetime, timezone

# --- KONFIGURATION & DESIGN ---
st.set_page_config(page_title="Raten-Finder Pro (40'HC)", layout="wide")
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

st.markdown("""
    <style>
    .all-in-box { background-color: #e6f4ea; border: 2px solid #28a745; padding: 15px; border-radius: 10px; text-align: center; }
    .basis-box { background-color: #e8f0fe; border: 1px solid #1a73e8; padding: 15px; border-radius: 10px; text-align: center; }
    .fremd-waehrung { color: #d9534f; font-weight: bold; font-size: 0.85em; }
    .stCheckbox { margin-bottom: 0px !important; }
    </style>
    """, unsafe_allow_html=True)

# --- MONGODB ANBINDUNG ---
# Nutze st.secrets für Sicherheit in der Cloud!
MONGO_URI = "mongodb+srv://blindner984_db_user:GtCR5qnPJeGKGpbe@cluster0.yc0llqz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

@st.cache_resource
def init_db():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client["SpeditionsDB"]
        collection = db["Raten"]
        collection.create_index("createdAt", expireAfterSeconds=15552000) # 6 Monate
        return collection
    except Exception as e:
        st.error(f"Datenbank-Verbindung fehlgeschlagen: {e}")
        return None

collection = init_db()

# --- ERWEITERTER WÄHRUNGS-CHECK ---
@st.cache_data(ttl=3600)
def hole_wechselkurse():
    """Holt Kurse von der Frankfurter API (EZB Daten)"""
    kurse = {"USD": 1.08, "CNY": 7.80, "GBP": 0.85, "HKD": 8.50, "EUR": 1.0}
    try:
        # Basis EUR holen
        response = requests.get("https://api.frankfurter.app/latest?from=EUR", timeout=5)
        if response.status_code == 200:
            api_rates = response.json()['rates']
            for w in kurse.keys():
                if w in api_rates:
                    kurse[w] = api_rates[w]
        return kurse
    except:
        return kurse # Fallback-Werte

kurse_dict = hole_wechselkurse()

def konvertiere_zu_eur(betrag, von_waehrung):
    if von_waehrung == "EUR" or not von_waehrung:
        return betrag
    # Frankfurter API gibt Kurse als 1 EUR = X USD an -> Betrag / Kurs
    kurs = kurse_dict.get(von_waehrung.upper())
    if kurs:
        return betrag / kurs
    return 0

# --- HILFSFUNKTIONEN ---
def extrahiere_gebuehren(text):
    if not isinstance(text, str) or text.lower() in ['nan', 'none', '']: return []
    # Findet Muster wie "THC = 250 EUR" oder "CAF = 10.50 USD"
    matches = re.findall(r'([A-Z\s\(\)\-\/]+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})', text)
    res = []
    for m in matches:
        try:
            res.append({
                "name": m[0].strip(),
                "betrag": float(m[1].replace('.', '').replace(',', '.')),
                "waehrung": m[2].upper()
            })
        except: continue
    return res

def ist_dokumentation(name):
    return any(x in name.lower() for x in ['bl', 'doc', 'documentation', 'bill of lading', 'fee'])

# --- UI LOGIK: ANZEIGE ---
def render_rate_card(row, index):
    basis = pd.to_numeric(row.get('40HC'), errors='coerce') or 0
    waehrung_basis = str(row.get('Currency', 'USD')).upper()
    basis_eur = konvertiere_zu_eur(basis, waehrung_basis)

    prep_list = extrahiere_gebuehren(str(row.get('Included Prepaid Surcharges 40HC', '')))
    coll_list = extrahiere_gebuehren(str(row.get('Included Collect Surcharges 40HC', '')))

    col1, col2, col3, col4 = st.columns([1, 1.2, 1.2, 1.2])

    with col1:
        st.markdown(f"""<div class="basis-box">
            <small>BASISFRACHT</small><br>
            <b style="font-size:1.4em;">{basis:,.2f} {waehrung_basis}</b><br>
            <small>({basis_eur:.2f} EUR)</small>
        </div>""", unsafe_allow_html=True)

    summe_zuschlaege_eur = 0
    fremdwahrungen_warnung = []

    with col2:
        st.write("**Prepaid (Abgang):**")
        for g in prep_list:
            if ist_dokumentation(g['name']): continue
            eur_wert = konvertiere_zu_eur(g['betrag'], g['waehrung'])
            if eur_wert > 0:
                summe_zuschlaege_eur += eur_wert
                st.write(f"✅ <small>{g['name']}: {g['betrag']} {g['waehrung']}</small>", unsafe_allow_html=True)
            else:
                fremdwahrungen_warnung.append(f"{g['name']} ({g['waehrung']})")

    with col3:
        st.write("**Collect (Import):**")
        for i, g in enumerate(coll_list):
            if ist_dokumentation(g['name']): continue
            if st.checkbox(f"{g['name']} ({g['betrag']} {g['waehrung']})", key=f"c_{index}_{i}"):
                eur_wert = konvertiere_zu_eur(g['betrag'], g['waehrung'])
                if eur_wert > 0:
                    summe_zuschlaege_eur += eur_wert
                else:
                    fremdwahrungen_warnung.append(f"{g['name']} ({g['waehrung']})")

    with col4:
        total = basis_eur + summe_zuschlaege_eur
        warn_html = f"<div class='fremd-waehrung'>⚠️ Unbekannte Währung: {', '.join(fremdwahrungen_warnung)}</div>" if fremdwahrungen_warnung else ""
        st.markdown(f"""<div class="all-in-box">
            <small>GESAMT (ALL-IN)</small><br>
            <b style="font-size:1.6em; color:#1e7e34;">{total:.2f} EUR</b>
            {warn_html}
        </div>""", unsafe_allow_html=True)

# --- DATEN-IMPORT NORMALISIERUNG ---
def verarbeite_datei(datei):
    if datei.name.endswith('.pdf'):
        # PDF Logik (vereinfacht)
        return pd.DataFrame(), "PDF" 
    
    df = pd.read_excel(datei) if datei.name.endswith('.xlsx') else pd.read_csv(datei)
    
    # Spalten-Mapping (Normalisierung)
    df.columns = [c.strip() for c in df.columns]
    mapping = {
        '40HDRY': '40HC', '40HC All In': '40HC', 'Rate': '40HC',
        'POL': 'Port of Loading', 'Port of Loading': 'Port of Loading',
        'POD': 'Port of Destination', 'Port of Destination': 'Port of Destination',
        'Effective Date': 'Valid from', 'Expiry Date': 'Valid to'
    }
    df = df.rename(columns=mapping)
    
    # Datum konvertieren
    for col in ['Valid from', 'Valid to']:
        if col in df.columns:
            df[f'{col} dt'] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            
    return df, "Tabellarisch"

# --- MAIN UI ---
st.title("🚢 Speditions-Raten-Finder (Multi-Währung)")

t_suche, t_upload = st.tabs(["🔍 Raten-Suche", "⚙️ Admin-Backend"])

with t_suche:
    if collection is not None:
        raw_data = list(collection.find())
        if not raw_data:
            st.info("Datenbank leer. Bitte im Admin-Tab Daten hochladen.")
        else:
            df = pd.DataFrame(raw_data)
            
            # Filterleiste
            c1, c2, c3 = st.columns(3)
            pol = c1.text_input("Ladehafen (POL)")
            pod = c2.text_input("Zielhafen (POD)")
            stichtag = c3.date_input("Gültig am", datetime.now())
            
            # Filterung
            mask = pd.Series([True] * len(df))
            if pol: mask &= df['Port of Loading'].str.contains(pol, case=False, na=False)
            if pod: mask &= df['Port of Destination'].str.contains(pod, case=False, na=False)
            
            # Datumsfilter (MongoDB speichert oft als String oder Datetime)
            if 'Valid from dt' in df.columns:
                df['Valid from dt'] = pd.to_datetime(df['Valid from dt'])
                df['Valid to dt'] = pd.to_datetime(df['Valid to dt'])
                mask &= (df['Valid from dt'].dt.date <= stichtag) & (df['Valid to dt'].dt.date >= stichtag)
            
            treffer = df[mask]
            
            if not treffer.empty:
                st.success(f"{len(treffer)} Raten gefunden.")
                for idx, row in treffer.iterrows():
                    with st.expander(f"🚢 {row.get('Carrier', 'Carrier')} | {row.get('Port of Loading')} ➡️ {row.get('Port of Destination')} | {row.get('Contract Number', 'N/A')}"):
                        render_rate_card(row, idx)
            else:
                st.warning("Keine passenden Raten gefunden.")

with t_upload:
    st.write("### Daten-Upload")
    files = st.file_uploader("Excel/CSV Dateien hochladen", accept_multiple_files=True)
    if st.button("Daten in Cloud speichern") and files:
        for f in files:
            df_new, _ = verarbeite_datei(f)
            if not df_new.empty:
                df_new['createdAt'] = datetime.now(timezone.utc)
                # MongoDB braucht Dicts, Datetimes müssen ggf. konvertiert werden
                for col in df_new.select_dtypes(include=['datetime']).columns:
                    df_new[col] = df_new[col].astype(str)
                collection.insert_many(df_new.to_dict('records'))
        st.success("Erfolgreich importiert!")
        st.cache_data.clear()

    if st.button("🗑️ Gesamte Datenbank löschen"):
        collection.delete_many({})
        st.rerun()
