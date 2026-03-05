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
    /* Info-Boxen Styling */
    .all-in-box { background-color: #e6f4ea; border: 2px solid #28a745; padding: 15px; border-radius: 10px; text-align: center; }
    .basis-box { background-color: #e8f0fe; border: 1px solid #1a73e8; padding: 15px; border-radius: 10px; text-align: center; }
    .collect-box { background-color: #fff3cd; border: 1px solid #ffeeba; padding: 15px; border-radius: 10px; margin-bottom: 15px; }
    .fremd-waehrung { color: #d9534f; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# --- LOGO ---
try:
    st.sidebar.image("logo_farbig.png", use_container_width=True) 
except:
    pass 

st.title("🚢 Speditions-Raten-Finder (Cloud-Datenbank)")

# --- MONGODB ---
MONGO_URI = "mongodb+srv://blindner984_db_user:GtCR5qnPJeGKGpbe@cluster0.yc0llqz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

@st.cache_resource
def init_db():
    client = pymongo.MongoClient(MONGO_URI)
    db = client["SpeditionsDB"]
    collection = db["Raten"]
    collection.create_index("createdAt", expireAfterSeconds=15552000)
    return collection

collection = init_db()

# --- WECHSELKURS ---
@st.cache_data(ttl=3600)
def hole_live_wechselkurs():
    try:
        response = requests.get("https://api.frankfurter.app/latest?from=USD&to=EUR", timeout=5)
        return round(response.json()['rates']['EUR'], 3)
    except:
        return 0.92

aktueller_kurs = hole_live_wechselkurs()
st.sidebar.header("💱 Einstellungen")
usd_to_eur = st.sidebar.number_input("Wechselkurs: 1 USD in EUR", value=aktueller_kurs, step=0.01)

# --- DOC FILTER ---
def ist_doc_gebuehr(name):
    n = str(name).lower()
    return any(x in n for x in ['b/l', 'bl', 'doc', 'documentation'])

# --- BERECHNUNGSLOGIK ---
def berechne_gebuehren(zuschlaege_str):
    if not isinstance(zuschlaege_str, str) or zuschlaege_str.lower() in ['nan', 'none', '']: return []
    treffer = re.findall(r'([A-Za-z0-9\s\(\)\-]+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})', zuschlaege_str)
    return [{"name": t[0].strip(), "betrag": float(t[1].replace(',', '.')), "waehrung": t[2].upper()} for t in treffer]

def berechne_total_eur_dynamic(row, price_col, prep_col, coll_col, row_index):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    if pd.isna(basis) or basis <= 0: return 99999999 
    basis_eur = basis * usd_to_eur if str(row.get('Currency', 'USD')).upper() == 'USD' else basis
    
    total_gebuehren = 0
    for g in berechne_gebuehren(str(row.get(prep_col, ''))):
        if not ist_doc_gebuehr(g['name']):
            total_gebuehren += (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag']
    return basis_eur + total_gebuehren

def anzeige_container_daten(row, size_label, price_col, prep_col, coll_col, row_index):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    curr = str(row.get('Currency', 'USD')).upper()
    basis_eur = basis * usd_to_eur if curr == 'USD' else basis
    
    col_basis, col_prep, col_coll, col_doc, col_total = st.columns([1, 1.1, 1.1, 1.1, 1.2])
    with col_basis:
        st.markdown(f'<div class="basis-box"><b>Basis {size_label}</b><br>{basis:,.2f} {curr}<br><small>≈ {basis_eur:.2f} EUR</small></div>', unsafe_allow_html=True)
    
    prep_geb = berechne_gebuehren(str(row.get(prep_col, '')))
    total_prep_eur = 0
    with col_prep:
        st.write("**Prepaid:**")
        for g in prep_geb:
            if ist_doc_gebuehr(g['name']): continue
            umg = g['betrag'] * usd_to_eur if g['waehrung'] == 'USD' else g['betrag']
            total_prep_eur += umg
            st.write(f"➕ {g['name']}: {g['betrag']:.2f} {g['waehrung']}")

    with col_total:
        total = basis_eur + total_prep_eur
        st.markdown(f'<div class="all-in-box"><b>All-In Preis</b><br><span style="font-size:24px; font-weight:bold;">{total:.2f} EUR</span></div>', unsafe_allow_html=True)

# --- PDF PARSER ---
def lade_und_uebersetze_cached(file_name, file_bytes):
    datei = io.BytesIO(file_bytes)
    datei.name = file_name
    if not datei.name.lower().endswith('.pdf'): return pd.DataFrame(), "Kein PDF"
    
    try:
        reader = PyPDF2.PdfReader(datei)
        text = " ".join([p.extract_text() for p in reader.pages])
        
        # Basisdaten
        date_matches = re.findall(r'(\d{2,4}[.\-/]\d{2}[.\-/]\d{2,4})', text)
        v_from = date_matches[0] if len(date_matches) > 0 else "Unbekannt"
        v_to = date_matches[-1] if len(date_matches) > 1 else "Unbekannt"
        
        contract = re.search(r'(?:Contract|Quote)[\s\S]{1,300}?\b([A-Z0-9]{5,20})\b', text, re.I)
        contract_no = contract.group(1) if contract else "Unbekannt"
        
        # Zuschlags-Scanner
        prepaid_list = []
        def find_surcharge(name, patterns, force_teu=False, prevent_teu=False):
            regex = r'(?:' + '|'.join(patterns) + r')'
            for m in re.finditer(regex, text, re.I):
                block = text[m.end():m.end()+120]
                # Inkludiert-Check
                if re.search(r'\b(?:not subject to|incl\.?|included|n/a|ind\.?)\b', block[:60], re.I): continue
                # Preis-Check (muss EUR/USD dahinter haben)
                price_match = re.search(r'(?<!\d)(\d{1,4}(?:[.,]\d{1,2})?)\s*(EUR|USD)', block, re.I)
                if price_match:
                    val = float(price_match.group(1).replace(',', '.'))
                    curr = price_match.group(2).upper()
                    # TEU Logik
                    if force_teu or (not prevent_teu and 'teu' in block.lower()): val *= 2
                    return f"{name} = {val:.2f} {curr}"
            return None

        # ECA, ETS, FEU, PSS, BRC/BAC, ERC Scannen
        for s in [("ERC", ["Logistic Fee"]), ("ETS", ["Emissions Trading System", "ET'S"], True), 
                  ("FEU", ["Fuel EU", "FEU"], True), ("PSS", ["Peak Season Surcharge", "PSS"], False, True),
                  ("ECA", ["Emission Control Area", "ECA"], True), ("BRC", ["Bunker Recovery", "BRC", "BAC"], True)]:
            res = find_surcharge(s[0], s[1], *s[2:])
            if res: prepaid_list.append(res)

        # Routen-Erkennung
        raten_liste = []
        if "via pol" in text.lower():
            # Matrix-Logik
            for block in re.split(r'Port\s+of\s+Discharge', text, flags=re.I)[1:]:
                pod = re.search(r'^\s*([A-Za-z\s\-]+)', block)
                pod_str = re.sub(r'[^A-Za-z\s]', '', pod.group(1)).split()[0].title() if pod else "Unbekannt"
                for route in re.finditer(r'via\s+POL\s+([A-Za-z/]+)\s+[\d.,]+\s*[A-Z]{3}\s+([\d.,]+)\s*([A-Z]{3})', block, re.I):
                    raten_liste.append({'Carrier': 'MSC', 'Contract Number': contract_no, 'Port of Loading': route.group(1), 
                                        'Port of Destination': pod_str, 'Valid from': v_from, 'Valid to': v_to, 
                                        '40HC': float(route.group(2).replace(',', '.')), 'Currency': route.group(3).upper(),
                                        'Included Prepaid Surcharges 40HC': ", ".join(prepaid_list)})
        else:
            # Single-Rate Logik
            pod = re.search(r'Port\s+of\s+Discharge[\s\n]*([A-Za-z\s\-]+)', text, re.I)
            pod_str = re.sub(r'(?i)Volume|DV|HC|Freetime|Remarks', '', pod.group(1)).strip().split()[0].title() if pod else "Unbekannt"
            rate = re.search(r'(\d{3,4}(?:[.,]\d{1,2})?)\s*(USD|EUR)', text[text.find("40'"):text.find("40'")+100])
            if rate:
                raten_liste.append({'Carrier': 'MSC', 'Contract Number': contract_no, 'Port of Loading': 'Hamburg', 
                                    'Port of Destination': pod_str, 'Valid from': v_from, 'Valid to': v_to, 
                                    '40HC': float(rate.group(1).replace(',', '.')), 'Currency': rate.group(2).upper(),
                                    'Included Prepaid Surcharges 40HC': ", ".join(prepaid_list)})

        df = pd.DataFrame(raten_liste)
        for col in ['Valid from', 'Valid to']:
            df[col + ' dt'] = pd.to_datetime(df[col], dayfirst=True, errors='coerce').astype(str)
        return df, "PDF"
    except Exception as e: return pd.DataFrame(), f"Fehler: {e}"

# --- UI TABS ---
tab_suche, tab_upload = st.tabs(["🔍 Raten suchen", "⚙️ Daten hochladen (Admin)"])

with tab_suche:
    cursor = list(collection.find({}))
    if not cursor:
        st.info("Datenbank leer.")
    else:
        df = pd.DataFrame(cursor)
        df['Valid from dt'] = pd.to_datetime(df['Valid from dt'], errors='coerce')
        df['Valid to dt'] = pd.to_datetime(df['Valid to dt'], errors='coerce')
        
        c1, c2, c3 = st.columns(3)
        pol = c1.text_input("POL:")
        pod = c2.text_input("POD:")
        datum = c3.date_input("Gültig am:")
        
        mask = (df['Valid from dt'] <= pd.to_datetime(datum)) & (df['Valid to dt'] >= pd.to_datetime(datum))
        if pol: mask &= df['Port of Loading'].str.contains(pol, case=False, na=False)
        if pod: mask &= df['Port of Destination'].str.contains(pod, case=False, na=False)
        
        treffer = df[mask].copy()
        if not treffer.empty:
            treffer['Sort'] = treffer.apply(lambda r: berechne_total_eur_dynamic(r, '40HC', 'Included Prepaid Surcharges 40HC', '', r.name), axis=1)
            for _, r in treffer.sort_values('Sort').iterrows():
                with st.expander(f"🚢 {r['Carrier']} | {r['Port of Loading']} ➡️ {r['Port of Destination']}"):
                    anzeige_container_daten(r, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC', '', r.name)

with tab_upload:
    files = st.file_uploader("Upload PDFs", type="pdf", accept_multiple_files=True)
    if files and st.button("Speichern"):
        for f in files:
            df_teil, _ = lade_und_uebersetze_cached(f.name, f.getvalue())
            if not df_teil.empty:
                for rec in df_teil.to_dict('records'):
                    rec['createdAt'] = datetime.now(timezone.utc)
                    collection.update_one({"Contract Number": rec["Contract Number"], "Port of Loading": rec["Port of Loading"], "Port of Destination": rec["Port of Destination"]}, {"$set": rec}, upsert=True)
        st.success("Erfolgreich hochgeladen!")
    if st.button("🗑️ Datenbank leeren"):
        collection.delete_many({})
        st.rerun()
