import streamlit as st
import pandas as pd
import re
import pdfplumber  # Ersetzt PyPDF2 für professionelles Tabellen-Parsing
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
    # TTL Index: Dokumente nach 180 Tagen (15552000 Sekunden) löschen
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
st.sidebar.write("*(Kurs wird stündlich live von der EZB aktualisiert)*")
usd_to_eur = st.sidebar.number_input("Wechselkurs: 1 USD in EUR", value=aktueller_kurs, step=0.01)

# --- DOKUMENTEN-FILTER ---
def ist_doc_gebuehr(name):
    n = str(name).lower()
    if 'b/l' in n: return True
    if re.search(r'\b(bl|doc|docs|documentation|bill of lading)\b', n): return True
    return False

# --- HILFSFUNKTIONEN FÜR BERECHNUNGEN ---
def berechne_gebuehren(zuschlaege_str):
    if not isinstance(zuschlaege_str, str) or zuschlaege_str.lower() in ['nan', 'none', '']: return []
    treffer = re.findall(r'([A-Za-z0-9\s\(\)\-]+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})', zuschlaege_str)
    liste = []
    for t in treffer:
        try: liste.append({"name": t[0].strip().lstrip(','), "betrag": float(t[1].replace('.', '').replace(',', '.')), "waehrung": t[2].upper()})
        except (ValueError, IndexError): pass
    return liste

def berechne_total_eur_dynamic(row, price_col, prep_surcharge_col, coll_surcharge_col, row_index):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    if pd.isna(basis) or basis <= 0: return 99999999 
    
    basis_eur = basis * usd_to_eur if str(row.get('Currency', 'USD')).upper() == 'USD' else basis
    summe_gebuehren_eur = 0
    
    for g in berechne_gebuehren(str(row.get(prep_surcharge_col, ''))):
        if ist_doc_gebuehr(g['name']): continue 
        summe_gebuehren_eur += (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag'] if g['waehrung'] == 'EUR' else 0

    for i, g in enumerate(berechne_gebuehren(str(row.get(coll_surcharge_col, '')))):
        if ist_doc_gebuehr(g['name']): continue 
        if st.session_state.get(f"chk_{row_index}_{i}_{g['name']}", False):
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
    
    with col_basis: 
        st.markdown(f'<div class="basis-box"><b>Basisfracht {size_label}</b><br><span style="font-size:20px;">{basis:,.2f} {curr_basis}</span><br><small>≈ {basis_eur:.2f} EUR</small></div>', unsafe_allow_html=True)
        
    with col_prep:
        st.write("**Zusammensetzung (Prepaid):**")
        has_prep = False
        for g in prep_gebuehren:
            if ist_doc_gebuehr(g['name']): 
                doc_gebuehren.append(g)
                continue
            has_prep = True
            if g['waehrung'] == 'USD':
                umgerechnet = g['betrag'] * usd_to_eur
                summe_gebuehren_eur += umgerechnet
                st.write(f"➕ {g['name']}: {g['betrag']:.2f} USD <small>(≈ {umgerechnet:.2f} EUR)</small>", unsafe_allow_html=True)
            elif g['waehrung'] == 'EUR':
                summe_gebuehren_eur += g['betrag']
                st.write(f"➕ {g['name']}: {g['betrag']:.2f} EUR", unsafe_allow_html=True)
            else:
                fremd_gebuehren.append(f"{g['betrag']:.2f} {g['waehrung']} ({g['name']})")
                st.markdown(f"➕ <span class='fremd-waehrung'>{g['name']}: {g['betrag']:.2f} {g['waehrung']}</span>", unsafe_allow_html=True)
        if not has_prep: st.write("<small>Keine extra Prepaid Gebühren</small>", unsafe_allow_html=True)
                
    with col_coll:
        st.write("**🏢 Collect (Zielort):**")
        has_coll = False
        for i, g in enumerate(coll_gebuehren):
            if ist_doc_gebuehr(g['name']): 
                doc_gebuehren.append(g)
                continue
            has_coll = True
            if st.checkbox(f"{g['name']} ({g['betrag']:.2f} {g['waehrung']})", key=f"chk_{row_index}_{i}_{g['name']}"):
                if g['waehrung'] == 'USD': summe_gebuehren_eur += (g['betrag'] * usd_to_eur)
                elif g['waehrung'] == 'EUR': summe_gebuehren_eur += g['betrag']
                else: fremd_gebuehren.append(f"{g['betrag']:.2f} {g['waehrung']} ({g['name']} - Collect)")
        if not has_coll: st.write("<small>Keine Collect Gebühren</small>", unsafe_allow_html=True)

    with col_doc:
        st.write("**📄 BL & Docs:**")
        if not doc_gebuehren: 
            st.write("<small>-</small>", unsafe_allow_html=True)
        else:
            st.write("<small><i>(Nicht im All-In)</i></small>", unsafe_allow_html=True)
            for g in doc_gebuehren:
                st.markdown(f"🔹 {g['name']}: {g['betrag']:.2f} {g['waehrung']}")

    with col_total:
        total_eur = basis_eur + summe_gebuehren_eur
        zusatz = f"<br><br><span class='fremd-waehrung'><b>⚠️ Zzgl. Fremdwährungen:</b><br>" + "<br>".join(fremd_gebuehren) + "</span>" if fremd_gebuehren else ""
        st.markdown(f'<div class="all-in-box"><b>Echter All-In Preis</b><br><span style="font-size:26px; font-weight:bold; color:#1e7e34;">{total_eur:.2f} EUR</span>{zusatz}</div>', unsafe_allow_html=True)

# --- HILFSFUNKTION FÜR PDF-PREISE ---
def parse_price(val_str):
    """Macht aus wirren Strings wie '1.250,00', '550.00' oder '450' saubere Floats"""
    s = re.sub(r'[^\d,\.]', '', val_str)
    if not s: return 0.0
    if ',' in s and len(s.split(',')[-1]) == 2:
        s = s.rsplit(',', 1)[0].replace('.', '').replace(',', '') + '.' + s.split(',')[-1]
    elif '.' in s and len(s.split('.')[-1]) == 2:
        s = s.rsplit('.', 1)[0].replace('.', '').replace(',', '') + '.' + s.split('.')[-1]
    else:
        s = s.replace('.', '').replace(',', '')
    try: return float(s)
    except: return 0.0

# --- DATEI READER FÜR DEN ADMIN-UPLOAD ---
def lade_und_uebersetze_cached(file_name, file_bytes):
    datei = io.BytesIO(file_bytes)
    datei.name = file_name
    
    # === NEUER PDFPLUMBER PDF-PARSER ===
    if datei.name.lower().endswith('.pdf'):
        try:
            with pdfplumber.open(datei) as pdf:
                # Metadaten vom ersten Blatt extrahieren
                first_page_text = pdf.pages[0].extract_text() or ""
                full_text = " ".join([p.extract_text() or "" for p in pdf.pages])
                
                date_matches = re.findall(r'(\d{2,4}[.\-/]\d{2}[.\-/]\d{2,4})', first_page_text)
                v_from = date_matches[0] if len(date_matches) > 0 else "Unbekannt"
                v_to = date_matches[-1] if len(date_matches) > 1 else "Unbekannt"

                contract_match = re.search(r'(?:Contract Filing Reference|Contract|Quote|Ref\.?|Agreement)[\s#:]*([A-Z0-9]{5,20})', first_page_text, re.IGNORECASE)
                contract_no = contract_match.group(1) if contract_match else "Unbekannt"

                pol_match = re.search(r'(?:Ports? of Loading|POL|Origin Port)[\s:]+([A-Za-z\s,\-]{3,50})', first_page_text, re.IGNORECASE)
                default_pol = pol_match.group(1).split('\n')[0].strip() if pol_match else "Unbekannt"
                default_pol = re.sub(r'[^A-Za-z\s\-]', '', default_pol).strip()

                carrier = "Unbekannt"
                if "msc" in full_text.lower(): carrier = "MSC"
                elif "hapag" in full_text.lower(): carrier = "Hapag-Lloyd"
                elif "maersk" in full_text.lower(): carrier = "Maersk"

                raten_liste = []
                current_pod = "Unbekannt"
                
                # Tabellen auslesen und zeilenweise verarbeiten
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        if not table: continue
                        
                        idx_40hc = -1
                        
                        for row in table:
                            # Leere Zellen entfernen und bereinigen
                            row_texts = [str(c).replace('\n', ' ').strip() for c in row if c]
                            if not row_texts: continue
                            row_joined = " ".join(row_texts).lower()

                            # Blockiere Zuschlags-Zeilen (wir wollen hier nur die Basis/All-In Rate)
                            if re.search(r'\b(surcharge|fee|thc|pss|ets|eca|bac|isps|charge|factor)\b', row_joined, re.IGNORECASE):
                                continue

                            # Sucht nach dynamischen Überschriften für POD (z.B. "Port of Discharge Casablanca")
                            pod_m = re.search(r'port of discharge\s*([a-z\s]+)', row_joined)
                            if pod_m: current_pod = pod_m.group(1).strip().title()

                            # Spalten-Index für 40' Container finden
                            for i, cell in enumerate(row_texts):
                                if re.search(r"40'?\s*(?:dv|hc|all in)", cell, re.IGNORECASE):
                                    idx_40hc = i
                            
                            rate_val = 0
                            currency = "USD"
                            
                            # Wenn wir wissen, wo der 40' Preis steht, ziehen wir ihn raus
                            if idx_40hc != -1 and len(row_texts) > idx_40hc:
                                cell_val = row_texts[idx_40hc]
                                rate_m = re.search(r'([\d\.,]{3,9})\s*(USD|EUR)', cell_val, re.IGNORECASE)
                                if rate_m:
                                    rate_val = parse_price(rate_m.group(1))
                                    currency = rate_m.group(2).upper()
                                else:
                                    num_m = re.search(r'^([\d\.,]{3,9})$', cell_val)
                                    if num_m:
                                        rate_val = parse_price(num_m.group(1))
                                        currency = "EUR" if "eur" in full_text.lower() else "USD"

                            # Wenn Preis > 0 gefunden wurde -> Datensatz anlegen
                            if rate_val > 0:
                                row_pol = default_pol
                                if "via pol" in row_texts[0].lower():
                                    row_pol = row_texts[0].lower().replace("via pol", "").strip().upper()
                                
                                row_pod = current_pod
                                if not "via pol" in row_texts[0].lower() and not "port of discharge" in row_texts[0].lower():
                                    clean_pod = re.sub(r'[^A-Za-z\s]', '', row_texts[0]).strip()
                                    clean_pod = re.sub(r'\b[A-Z]{2}\b', '', clean_pod).strip()
                                    if len(clean_pod) > 2 and len(clean_pod) < 30:
                                        row_pod = clean_pod.title()

                                raten_liste.append({
                                    'Carrier': carrier,
                                    'Contract Number': contract_no,
                                    'Port of Loading': row_pol,
                                    'Port of Destination': row_pod,
                                    'Valid from': v_from,
                                    'Valid to': v_to,
                                    '40HC': rate_val,
                                    'Currency': currency,
                                    'Included Prepaid Surcharges 40HC': "Auto-Extrahiert (PDF Matrix)",
                                    'Included Collect Surcharges 40HC': "",
                                    'Remark': 'Importiert mit Tabellen-Erkennung'
                                })
                
                # DataFrame aus der Liste generieren
                if raten_liste:
                    df_pdf = pd.DataFrame(raten_liste)
                    if 'Valid from' in df_pdf.columns: df_pdf['Valid from dt'] = pd.to_datetime(df_pdf['Valid from'], dayfirst=True, errors='coerce').astype(str)
                    if 'Valid to' in df_pdf.columns: df_pdf['Valid to dt'] = pd.to_datetime(df_pdf['Valid to'], dayfirst=True, errors='coerce').astype(str)
                    return df_pdf, "PDF (Matrix)"
                else:
                    return pd.DataFrame(), "Keine gültigen Raten-Tabellen gefunden."

        except Exception as e:
            return pd.DataFrame(), f"Fehler bei PDF-Verarbeitung: {e}"

    # === EXCEL / CSV PARSER (UNVERÄNDERT) ===
    else:
        if datei.name.endswith('.xlsx'):
            excel_preview = pd.read_excel(datei, sheet_name=None, header=None, nrows=20)
            ziel_sheet, header_idx = None, 0
            for sheet_name, df_preview in excel_preview.items():
                for i in range(len(df_preview)):
                    if any(x in " ".join(df_preview.iloc[i].dropna().astype(str)) for x in ['40HDRY', 'Port of Destination', '40HC All In']):
                        ziel_sheet, header_idx = sheet_name, i
                        break
                if ziel_sheet: break
            df_raw = pd.read_excel(datei, sheet_name=ziel_sheet if ziel_sheet else list(excel_preview.keys())[0], header=None)
        else:
            df_raw = pd.read_csv(datei, header=None, low_memory=False)
            header_idx = 0
            for i in range(min(20, len(df_raw))):
                if any(x in " ".join(df_raw.iloc[i].dropna().astype(str)) for x in ['40HDRY', '40HC All In', 'Port of Destination']):
                    header_idx = i; break

        global_contract = "Unbekannt"
        
        for i in range(min(20, len(df_raw))):
            row_vals = df_raw.iloc[i].dropna().astype(str).tolist()
            for j, val in enumerate(row_vals):
                v_low = val.lower()
                if 'contract' in v_low:
                    nums = re.findall(r'\b\d{6,10}\b', val)
                    if nums:
                        global_contract = nums[0]
                        break
                    for k in range(1, 4):
                        if j + k < len(row_vals):
                            next_val = row_vals[j+k].upper()
                            next_tokens = re.findall(r'\b\d{6,10}\b', next_val)
                            if next_tokens:
                                global_contract = next_tokens[0]
                                break
                if global_contract != "Unbekannt":
                    break
            if global_contract != "Unbekannt":
                break

        if global_contract == "Unbekannt":
            if fn_match := re.search(r'(?:contract)[\s_0-9-]*?(\d{6,10})', datei.name, re.IGNORECASE): 
                global_contract = fn_match.group(1)

        rohe_spalten = df_raw.iloc[header_idx].astype(str).str.strip().tolist()
        neue_spalten, gesehen = [], {}
        for s in rohe_spalten:
            if s in gesehen: gesehen[s] += 1; neue_spalten.append(f"{s}.{gesehen[s]}")
            else: gesehen[s] = 0; neue_spalten.append(s)
                
        df_raw.columns = neue_spalten
        df_raw = df_raw.iloc[header_idx+1:].reset_index(drop=True)
        contract_col = next((c for c in df_raw.columns if any(x in c.lower() for x in ['contract', 'quote', 'reference'])), None)
        
        if 'Valid from' in df_raw.columns: df_raw['Valid from dt'] = pd.to_datetime(df_raw['Valid from'], dayfirst=True, errors='coerce').astype(str)
        if 'Valid to' in df_raw.columns: df_raw['Valid to dt'] = pd.to_datetime(df_raw['Valid to'], dayfirst=True, errors='coerce').astype(str)

        if '40HDRY' in df_raw.columns and 'Charge' in df_raw.columns:
            standard_rows = []
            for name, group in df_raw.dropna(subset=['40HDRY']).groupby(['POL', 'POD', 'Effective Date', 'Expiry Date']):
                bas_row = group[group['Charge'] == 'BAS']
                if bas_row.empty: continue
                val_raw = str(bas_row['40HDRY'].values[0]).strip().split()
                if len(val_raw) < 2: continue
                
                row_contract = global_contract
                
                standard_rows.append({
                    'Carrier': 'Maersk', 'Contract Number': row_contract, 
                    'Port of Loading': name[0], 'Port of Destination': name[1], 'Valid from': name[2], 'Valid to': name[3], 
                    '40HC': float(val_raw[1].replace(',', '')), 'Currency': val_raw[0],
                    'Included Prepaid Surcharges 40HC': ", ".join([f"{r['Charge']} = {r['40HDRY']}" for _, r in group[group['Charge'] != 'BAS'].iterrows() if ' ' in str(r['40HDRY'])]),
                    'Included Collect Surcharges 40HC': "", 'Remark': f"Transit Time: {bas_row['Transit Time'].values[0]}" if 'Transit Time' in bas_row.columns else ""
                })
            df_return = pd.DataFrame(standard_rows)
        else:
            df_raw['Contract Number'] = global_contract if global_contract != "Unbekannt" else (df_raw[contract_col].astype(str).fillna("Unbekannt") if contract_col else "Unbekannt")
            df_return = df_raw
            
        if 'Valid from' in df_return.columns: df_return['Valid from dt'] = pd.to_datetime(df_return['Valid from'], dayfirst=True, errors='coerce').astype(str)
        if 'Valid to' in df_return.columns: df_return['Valid to dt'] = pd.to_datetime(df_return['Valid to'], dayfirst=True, errors='coerce').astype(str)
        
        return df_return, "Excel/CSV"


# --- TABS FÜR UI ---
tab_suche, tab_upload = st.tabs(["🔍 Raten suchen", "⚙️ Daten hochladen (Admin)"])

# === TAB 1: SUCHEN ===
with tab_suche:
    cursor = collection.find({})
    daten_liste = list(cursor)

    if not daten_liste:
        st.info("💡 Die Datenbank ist aktuell leer. Bitte lade im Reiter 'Daten hochladen (Admin)' zuerst Raten hoch.")
    else:
        df = pd.DataFrame(daten_liste)
        
        if 'Valid from dt' in df.columns: df['Valid from dt'] = pd.to_datetime(df['Valid from dt'], errors='coerce')
        if 'Valid to dt' in df.columns: df['Valid to dt'] = pd.to_datetime(df['Valid to dt'], errors='coerce')
        
        st.write(f"### Suche in der Datenbank ({len(df)} Raten aktiv)")
        c1, c2, c3, c4 = st.columns(4)
        with c1: such_pol = st.text_input("📍 Ladehafen (POL):", placeholder="z.B. Hamburg")
        with c2: such_pod = st.text_input("🏁 Zielhafen (POD):", placeholder="z.B. Hamad")
        with c3: such_contract = st.text_input("📄 Contract Nr.:", placeholder="z.B. 299424203")
        with c4:
            filter_datum_aktiv = st.checkbox("📅 Datumsfilter aktiv", value=True)
            such_datum = st.date_input("Rate gültig am:", disabled=not filter_datum_aktiv)

        mask = pd.Series([True] * len(df))
        
        if such_pol and 'Port of Loading' in df.columns: mask &= df['Port of Loading'].astype(str).str.contains(such_pol.strip(), case=False, na=False, regex=False)
        if such_pod and 'Port of Destination' in df.columns: mask &= df['Port of Destination'].astype(str).str.contains(such_pod.strip(), case=False, na=False, regex=False)
        if such_contract and 'Contract Number' in df.columns: mask &= df['Contract Number'].astype(str).str.contains(such_contract.strip(), case=False, na=False, regex=False)
        
        if filter_datum_aktiv and 'Valid from dt' in df.columns:
            dt_search = pd.to_datetime(such_datum)
            mask &= (df['Valid from dt'] <= dt_search) & (df['Valid to dt'] >= dt_search)
        
        treffer = df[mask].copy()
        if '40HC' in treffer.columns:
            treffer['40HC_Check'] = pd.to_numeric(treffer['40HC'], errors='coerce')
            treffer = treffer[treffer['40HC_Check'] > 0].reset_index(drop=True)
            
            if not treffer.empty:
                treffer['Total_EUR_Sort'] = treffer.apply(lambda r: berechne_total_eur_dynamic(r, '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', r.name), axis=1)
                treffer = treffer.sort_values(by='Total_EUR_Sort')
                
                st.success(f"✅ {len(treffer)} gültige Raten gefunden. Zeige die Top {min(50, len(treffer))} günstigsten an:")
                
                for _, row in treffer.head(50).iterrows():
                    is_best = (row['Total_EUR_Sort'] == treffer['Total_EUR_Sort'].iloc[0])
                    label = f"{'🏆 BESTER PREIS | ' if is_best else ''}🚢 {row.get('Carrier')} | 📄 {row.get('Contract Number')} | {row.get('Port of Loading')} ➡️ {row.get('Port of Destination')}"
                    
                    with st.expander(label):
                        anzeige_container_daten(row, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', row.name)
                        if pd.notna(row.get('Remark')) and row.get('Remark') != "": st.info(f"**💡 Bemerkung:** {row['Remark']}")
            else: st.warning("Keine gültigen Raten für diese Suche gefunden.")


# === TAB 2: ADMIN UPLOAD & LÖSCHEN ===
with tab_upload:
    st.write("### 📥 Neue Raten-Dateien in die Datenbank importieren")
    uploaded_files = st.file_uploader("Dateien auswählen (.xlsx, .csv, .pdf)", type=["xlsx", "csv", "pdf"], accept_multiple_files=True)
    
    if uploaded_files:
        if st.button("🚀 Hochladen & in MongoDB speichern", type="primary"):
            alle_daten = []
            with st.spinner("Lese Dateien und bereite Vorschau vor..."):
                for datei in uploaded_files:
                    try:
                        df_teil, format_name = lade_und_uebersetze_cached(datei.name, datei.getvalue())
                        if not df_teil.empty:
                            alle_daten.append(df_teil)
                    except Exception as e: st.error(f"Fehler bei {datei.name}: {e}")
            
                if alle_daten:
                    df_upload = pd.concat(alle_daten, ignore_index=True)
                    df_upload['createdAt'] = datetime.now(timezone.utc)
                    
                    # --- NEU: VORSCHAU DER ERKANNTEN DATEN ---
                    st.write("### 👁️ Vorschau der erkannten Daten (Auszug)")
                    st.dataframe(df_upload[['Carrier', 'Contract Number', 'Port of Loading', 'Port of Destination', '40HC', 'Currency']].head(20))
                    
                    records = df_upload.to_dict('records')
                    
                    if records:
                        collection.insert_many(records) 
                        st.success(f"✅ Super! {len(records)} Raten-Zeilen wurden erfolgreich aus den Tabellen extrahiert und in die Datenbank geschrieben.")
                        st.balloons()
    
    # --- GEFAHRENZONE (DATENBANK LEEREN) ---
    st.markdown("---")
    st.write("### 🚨 Gefahrenzone")
    st.error("Achtung: Der folgende Button löscht **alle** gespeicherten Raten unwiderruflich aus der Datenbank. Nutze dies nur, wenn du komplett neu anfangen möchtest!")
    
    if st.button("🗑️ Ganze Datenbank leeren (Alle Raten löschen)"):
        ergebnis_all = collection.delete_many({})
        st.success(f"✅ Datenbank erfolgreich geleert! Es wurden {ergebnis_all.deleted_count} alte Einträge gelöscht.")
