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
    .all-in-box { background-color: #e6f4ea; border: 2px solid #28a745; padding: 15px; border-radius: 10px; text-align: center; }
    .basis-box { background-color: #e8f0fe; border: 1px solid #1a73e8; padding: 15px; border-radius: 10px; text-align: center; }
    .fremd-waehrung { color: #d9534f; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

# --- LOGO ---
try:
    st.sidebar.image("logo_farbig.png", use_container_width=True) 
except:
    pass 

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
    except:
        return 0.92

aktueller_kurs = hole_live_wechselkurs()
st.sidebar.header("💱 Einstellungen")
usd_to_eur = st.sidebar.number_input("Wechselkurs: 1 USD in EUR", value=aktueller_kurs, step=0.01)

# --- HILFSFUNKTIONEN ---
def ist_doc_gebuehr(name):
    n = str(name).lower()
    return any(x in n for x in ['b/l', 'bl', 'doc', 'documentation'])

def parse_price(val_str):
    """Wandelt Strings mit Kommas oder Punkten sicher in Floats um."""
    if not val_str: return 0.0
    s = str(val_str).replace(' ', '').replace('EUR', '').replace('USD', '').replace('LISD', '')
    # Entferne Tausenderpunkte, ersetze Dezimalkomma durch Punkt
    if ',' in s and '.' in s:
        if s.find('.') < s.find(','): s = s.replace('.', '')
    s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0

def berechne_total_eur_dynamic(row, price_col, prep_col):
    """Berechnet den Gesamtpreis und fängt Fehler bei der Konvertierung ab."""
    basis = parse_price(row.get(price_col))
    if basis <= 0: return 99999999 
    
    basis_eur = basis * usd_to_eur if str(row.get('Currency', 'USD')).upper() == 'USD' else basis
    
    sum_geb = 0
    raw_geb = str(row.get(prep_col, ''))
    # Suche Muster: NAME = BETRAG WÄHRUNG
    matches = re.findall(r'([A-Z0-9\s]+?)\s*=\s*([\d,\.]+)\s*([A-Z]{3})', raw_geb)
    
    for m in matches:
        if not ist_doc_gebuehr(m[0]):
            val = parse_price(m[1])
            sum_geb += (val * usd_to_eur) if m[2] == 'USD' else val
            
    return basis_eur + sum_geb

def anzeige_container_daten(row, size_label, price_col, prep_col):
    basis = parse_price(row.get(price_col))
    curr = str(row.get('Currency', 'USD')).upper()
    basis_eur = basis * usd_to_eur if curr == 'USD' else basis
    
    col1, col2, col3 = st.columns([1, 1.5, 1])
    with col1:
        st.markdown(f'<div class="basis-box"><b>Basis {size_label}</b><br>{basis:,.2f} {curr}<br><small>≈ {basis_eur:.2f} EUR</small></div>', unsafe_allow_html=True)
    
    prep_raw = str(row.get(prep_col, ''))
    geb_list = re.findall(r'([A-Z0-9\s]+?)\s*=\s*([\d,\.]+)\s*([A-Z]{3})', prep_raw)
    total_prep_eur = 0
    with col2:
        st.write("**Zusammensetzung (Prepaid):**")
        if not geb_list:
            st.write("<small>Keine extra Gebühren</small>", unsafe_allow_html=True)
        for g in geb_list:
            if ist_doc_gebuehr(g[0]): continue
            val = parse_price(g[1])
            umg = (val * usd_to_eur) if g[2] == 'USD' else val
            total_prep_eur += umg
            st.write(f"➕ {g[0]}: {val:.2f} {g[2]}")

    with col3:
        total = basis_eur + total_prep_eur
        st.markdown(f'<div class="all-in-box"><b>Echter All-In Preis</b><br><span style="font-size:24px; font-weight:bold;">{total:.2f} EUR</span></div>', unsafe_allow_html=True)

# --- PDF READER ---
def lade_und_uebersetze_cached(file_name, file_bytes):
    datei = io.BytesIO(file_bytes)
    try:
        reader = PyPDF2.PdfReader(datei)
        text = " ".join([p.extract_text() for p in reader.pages])
        
        dates = re.findall(r'(\d{2,4}[.\-/]\d{2}[.\-/]\d{2,4})', text)
        v_from = dates[0] if dates else "Unbekannt"
        v_to = dates[-1] if len(dates) > 1 else "Unbekannt"
        
        cont = re.search(r'(?:Contract|Quote|Reference)[\s#:]*([A-Z0-9]{5,20})', text, re.I)
        contract_no = cont.group(1) if cont else "Unbekannt"
        
        prepaid_list = []
        def get_sur(name, keys, f_teu=False, p_teu=False):
            regex = r'(?:' + '|'.join(keys) + r')'
            for m in re.finditer(regex, text, re.I):
                block = text[m.end():m.end()+130]
                if re.search(r'\b(?:not subject to|incl|ind|included|n/a)\b', block[:60], re.I): continue
                p_match = re.search(r'(?<!\d)(\d{1,4}(?:[.,]\d{1,3})?)\s*(EUR|USD|LISD|SEUR)', block, re.I)
                if p_match:
                    val = parse_price(p_match[1])
                    curr = p_match[2].upper().replace('LISD', 'USD').replace('SEUR', 'EUR')
                    if f_teu or (not p_teu and 'teu' in block.lower()): val *= 2
                    return f"{name} = {val:.2f} {curr}"
            return None

        # Zuschläge scannen
        for s in [("ERC", ["Logistic Fee"]), ("ETS", ["ETS", "Emissions Trading"], True), 
                  ("FEU", ["Fuel EU", "FEU"], True), ("PSS", ["PSS", "Peak Season"], False, True),
                  ("ECA", ["ECA", "Emission Control Area"], True), ("BRC", ["BRC", "BAC", "Bunker Recovery"], True)]:
            res = get_sur(s[0], s[1], *s[2:])
            if res: prepaid_list.append(res)
        
        prep_str = ", ".join(prepaid_list)
        res_list = []

        if "via pol" in text.lower():
            for block in re.split(r'Port\s+of\s+Discharge', text, flags=re.I)[1:]:
                pod_m = re.search(r'^\s*([A-Za-z\s\-]+)', block)
                pod = pod_m.group(1).split()[0].title() if pod_m else "Unbekannt"
                for r in re.finditer(r'via\s+POL\s+([A-Za-z/]+)\s+[\d.,]+\s*[A-Z]{3}\s+([\d.,]+)\s*([A-Z]{3})', block, re.I):
                    res_list.append({'Carrier': 'MSC', 'Contract Number': contract_no, 'Port of Loading': r.group(1), 'Port of Destination': pod, 'Valid from': v_from, 'Valid to': v_to, '40HC': parse_price(r.group(2)), 'Currency': r.group(3).upper(), 'Included Prepaid Surcharges 40HC': prep_str})
        else:
            pod_m = re.search(r'Port\s+of\s+Discharge[\s\n]*([A-Za-z\s\-]+)', text, re.I)
            pod = re.sub(r'(?i)Volume|DV|HC|Freetime|at|POD|Origin|Remarks', '', pod_m.group(1)).strip().split()[0].title() if pod_m else "Unbekannt"
            rate_m = re.search(r'(\d{3,4}(?:[.,]\d{1,2})?)\s*(USD|EUR)', text[text.find("40'"):text.find("40'")+120])
            if rate_m:
                res_list.append({'Carrier': 'MSC', 'Contract Number': contract_no, 'Port of Loading': 'Hamburg', 'Port of Destination': pod, 'Valid from': v_from, 'Valid to': v_to, '40HC': parse_price(rate_m.group(1)), 'Currency': rate_m.group(2).upper(), 'Included Prepaid Surcharges 40HC': prep_str})

        df = pd.DataFrame(res_list)
        for c in ['Valid from', 'Valid to']:
            df[c + ' dt'] = pd.to_datetime(df[c], dayfirst=True, errors='coerce').astype(str)
        return df, "PDF"
    except Exception as e: return pd.DataFrame(), f"Error: {e}"

# --- UI ---
tab_suche, tab_upload = st.tabs(["🔍 Raten suchen", "⚙️ Daten hochladen (Admin)"])

with tab_suche:
    data = list(collection.find({}))
    if not data:
        st.info("Datenbank leer.")
    else:
        df = pd.DataFrame(data)
        c1, c2, c3 = st.columns(3)
        pol = c1.text_input("POL:")
        pod = c2.text_input("POD:")
        datum = pd.to_datetime(c3.date_input("Gültig am:"))
        
        # Sicherstellen, dass Datumsvergleiche funktionieren
        df['Valid from dt'] = pd.to_datetime(df['Valid from dt'], errors='coerce')
        df['Valid to dt'] = pd.to_datetime(df['Valid to dt'], errors='coerce')
        
        mask = (df['Valid from dt'] <= datum) & (df['Valid to dt'] >= datum)
        if pol: mask &= df['Port of Loading'].str.contains(pol, case=False, na=False)
        if pod: mask &= df['Port of Destination'].str.contains(pod, case=False, na=False)
        
        treffer = df[mask].copy()
        if not treffer.empty:
            treffer['Sort'] = treffer.apply(lambda r: berechne_total_eur_dynamic(r, '40HC', 'Included Prepaid Surcharges 40HC'), axis=1)
            for _, r in treffer.sort_values('Sort').iterrows():
                with st.expander(f"🚢 {r['Carrier']} | {r['Port of Loading']} ➡️ {r['Port of Destination']} | All-In: {r['Sort']:.2f} EUR"):
                    anzeige_container_daten(r, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC')

with tab_upload:
    uploaded = st.file_uploader("Upload PDFs", type="pdf", accept_multiple_files=True)
    if uploaded and st.button("Speichern"):
        for f in uploaded:
            df_new, _ = lade_und_uebersetze_cached(f.name, f.getvalue())
            if not df_new.empty:
                for rec in df_new.to_dict('records'):
                    rec['createdAt'] = datetime.now(timezone.utc)
                    collection.update_one({"Contract Number": rec["Contract Number"], "Port of Loading": rec["Port of Loading"], "Port of Destination": rec["Port of Destination"]}, {"$set": rec}, upsert=True)
        st.success("Erfolgreich hochgeladen!")
    if st.button("🗑️ Datenbank leeren"):
        collection.delete_many({})
        st.rerun()
