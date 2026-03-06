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
    treffer = re.findall(r'([A-Za-z0-9\s\(\)\-\']+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})', zuschlaege_str)
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


# --- DATEI READER FÜR DEN ADMIN-UPLOAD ---
def lade_und_uebersetze_cached(file_name, file_bytes):
    datei = io.BytesIO(file_bytes)
    datei.name = file_name
    
    # PDF VERARBEITUNG
    if datei.name.lower().endswith('.pdf'):
        try:
            with pdfplumber.open(datei) as pdf:
                full_text = "\n".join([page.extract_text() for page in pdf.pages if page.extract_text()])
                try:
                    layout_text = "\n".join([page.extract_text(layout=True) for page in pdf.pages if page.extract_text(layout=True)])
                except:
                    layout_text = full_text
                    
            raten_zeilen = []
            prep_surcharges = []
            coll_surcharges = []
            
            c_match = re.search(r'\b([A-Z0-9]{14,16})\b', full_text)
            contract_no = c_match.group(1) if c_match else "Unbekannt"
            
            v_match = re.search(r'Valid as from (\d{2}\.\d{2}\.\d{4}).*?not beyond (\d{2}\.\d{2}\.\d{4})', full_text, re.IGNORECASE)
            valid_from = v_match.group(1) if v_match else "Unbekannt"
            valid_to = v_match.group(2) if v_match else "Unbekannt"
            
            pol_match = re.search(r'Ports?\s+of\s+Loading\s{2,}([A-Za-z\s,]+)', layout_text, re.IGNORECASE)
            if pol_match and len(pol_match.group(1).strip()) > 2:
                global_pol = pol_match.group(1).strip()
            elif "Hamburg" in full_text:
                global_pol = "Hamburg"
            else:
                global_pol = "Unbekannt"
                
            current_pod = "Unbekannt"
            
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        clean_row = [str(c).replace('\n', ' ').strip() if c else "" for c in row]
                        if not any(clean_row): continue
                        
                        col0 = clean_row[0].lower()
                        row_text = " ".join(clean_row).lower()
                        
                        is_surcharge = any(x in row_text for x in ["surcharge", "fee", "factor", "thc", "emission", "bunker", "recovery"])
                        if is_surcharge and not "freetime" in row_text and not "via pol" in row_text:
                            amt_match = re.search(r'(\d+(?:[,.]\d{2})?)\s*(EUR|USD)', row_text, re.IGNORECASE)
                            if amt_match:
                                code = "FEE"
                                if "thc" in row_text or "terminal" in row_text: code = "THC"
                                elif "pss" in row_text or "peak season" in row_text: code = "PSS"
                                elif "ets" in row_text or "emissions trading" in row_text: code = "ETS"
                                elif "feu" in row_text or "fuel eu" in row_text: code = "FEU"
                                elif "erc" in row_text or "logistic" in row_text or "equipment" in row_text: code = "ERC"
                                elif "occ" in row_text or "operation" in row_text: code = "OCC"
                                
                                amt = amt_match.group(1).replace(',', '.')
                                curr = amt_match.group(2).upper()
                                entry = f"{code} = {amt} {curr}"
                                
                                if code == "OCC" or "collect" in row_text:
                                    if entry not in coll_surcharges: coll_surcharges.append(entry)
                                else:
                                    if entry not in prep_surcharges: prep_surcharges.append(entry)
                            continue

                        if "port of discharge" in col0 and len(col0) > 17:
                            pod_cand = col0.replace("port of discharge", "").replace(":", "").strip()
                            if pod_cand: current_pod = pod_cand.title()
                            continue
                            
                        rate_matches = re.findall(r'(\d{3,4}(?:[,.]\d{2})?)\s*(USD|EUR)', " ".join(clean_row), re.IGNORECASE)
                        if rate_matches:
                            target_match = rate_matches[-1] 
                            betrag = float(target_match[0].replace(',', '.'))
                            waehrung = target_match[1].upper()
                            
                            row_pol = global_pol
                            row_pod = current_pod
                            
                            if "via pol" in col0: row_pol = clean_row[0].lower().replace("via pol", "").strip().upper()
                            elif current_pod == "Unbekannt":
                                potential_pod = clean_row[0]
                                potential_pod = re.sub(r'\b[A-Z]{2}\b$', '', potential_pod).strip()
                                if 2 < len(potential_pod) < 25 and not any(char.isdigit() for char in potential_pod):
                                    row_pod = potential_pod.title()
                                    
                            if row_pod != "Unbekannt" and betrag > 0:
                                raten_zeilen.append({
                                    'Carrier': 'MSC',
                                    'Contract Number': contract_no,
                                    'Port of Loading': row_pol,
                                    'Port of Destination': row_pod,
                                    'Valid from': valid_from,
                                    'Valid to': valid_to,
                                    '40HC': betrag,
                                    'Currency': waehrung
                                })
            
            prep_str = ", ".join(prep_surcharges)
            coll_str = ", ".join(coll_surcharges)
            for r in raten_zeilen:
                r['Included Prepaid Surcharges 40HC'] = prep_str
                r['Included Collect Surcharges 40HC'] = coll_str
                r['Remark'] = 'PDF Import'
                
            return pd.DataFrame(raten_zeilen).drop_duplicates(), "PDF"
        except Exception as e: 
            return pd.DataFrame(), f"PDF Fehler: {e}"

    # EXCEL / CSV VERARBEITUNG
    else:
        try:
            # Endung prüfen (ignoriert Großschreibung)
            is_excel = datei.name.lower().endswith(('.xlsx', '.xls'))
            if is_excel:
                excel_preview = pd.read_excel(datei, sheet_name=None, header=None, nrows=20)
                ziel_sheet, header_idx = None, 0
                for sheet_name, df_preview in excel_preview.items():
                    for i in range(len(df_preview)):
                        row_str = " ".join([str(x) for x in df_preview.iloc[i].dropna()])
                        if any(x in row_str for x in ['40HDRY', '40HC All In', '40HC']):
                            ziel_sheet, header_idx = sheet_name, i
                            break
                    if ziel_sheet: break
                df_raw = pd.read_excel(datei, sheet_name=ziel_sheet if ziel_sheet else list(excel_preview.keys())[0], header=None)
            else:
                df_raw = pd.read_csv(datei, header=None, low_memory=False)
                header_idx = 0
                for i in range(min(20, len(df_raw))):
                    row_str = " ".join([str(x) for x in df_raw.iloc[i].dropna()])
                    if any(x in row_str for x in ['40HDRY', '40HC All In', '40HC']):
                        header_idx = i; break

            # Contract Suche
            global_contract = "Unbekannt"
            # Spaltenköpfe bereinigen
            rohe_spalten = df_raw.iloc[header_idx].astype(str).str.strip().tolist()
            neue_spalten, gesehen = [], {}
            for s in rohe_spalten:
                if s in gesehen: 
                    gesehen[s] += 1
                    neue_spalten.append(f"{s}.{gesehen[s]}")
                else: 
                    gesehen[s] = 0
                    neue_spalten.append(s)
            df_raw.columns = neue_spalten
            df_raw = df_raw.iloc[header_idx+1:].reset_index(drop=True)

            # Maersk Tender Format
            if '40HDRY' in df_raw.columns and 'Charge' in df_raw.columns:
                standard_rows = []
                for name, group in df_raw.dropna(subset=['40HDRY']).groupby(['POL', 'POD', 'Effective Date', 'Expiry Date']):
                    bas_row = group[group['Charge'] == 'BAS']
                    if bas_row.empty: continue
                    val_raw = str(bas_row['40HDRY'].values[0]).strip().split()
                    if len(val_raw) < 2: continue
                    standard_rows.append({
                        'Carrier': 'Maersk', 'Contract Number': 'Tender', 
                        'Port of Loading': name[0], 'Port of Destination': name[1], 'Valid from': name[2], 'Valid to': name[3], 
                        '40HC': float(val_raw[1].replace(',', '')), 'Currency': val_raw[0],
                        'Included Prepaid Surcharges 40HC': ", ".join([f"{r['Charge']} = {r['40HDRY']}" for _, r in group[group['Charge'] != 'BAS'].iterrows()]),
                        'Included Collect Surcharges 40HC': "", 'Remark': 'Maersk Tender'
                    })
                df_return = pd.DataFrame(standard_rows)
            else:
                # Standard CSV/Excel
                if '40HC' in neue_spalten:
                    idx_40hc = neue_spalten.index('40HC')
                    if idx_40hc + 1 < len(neue_spalten):
                        df_raw['Currency'] = df_raw[neue_spalten[idx_40hc + 1]]
                df_return = df_raw
            
            # Datumsfilter vorbereiten
            for col in ['Valid from', 'Effective Date']:
                if col in df_return.columns: df_return['Valid from dt'] = pd.to_datetime(df_return[col], errors='coerce').astype(str)
            for col in ['Valid to', 'Expiry Date']:
                if col in df_return.columns: df_return['Valid to dt'] = pd.to_datetime(df_return[col], errors='coerce').astype(str)
            
            return df_return, "Excel/CSV"
        except Exception as e:
            return pd.DataFrame(), f"Excel Fehler: {e}"


# --- TABS ---
tab_suche, tab_upload = st.tabs(["🔍 Raten suchen", "⚙️ Daten hochladen (Admin)"])

with tab_suche:
    cursor = collection.find({})
    daten_liste = list(cursor)

    if not daten_liste:
        st.info("💡 Datenbank ist leer.")
    else:
        df = pd.DataFrame(daten_liste)
        if 'Valid from dt' in df.columns: df['Valid from dt'] = pd.to_datetime(df['Valid from dt'], errors='coerce')
        if 'Valid to dt' in df.columns: df['Valid to dt'] = pd.to_datetime(df['Valid to dt'], errors='coerce')
        
        st.write(f"### Suche ({len(df)} Raten)")
        c1, c2, c3, c4 = st.columns(4)
        with c1: such_pol = st.text_input("📍 POL:")
        with c2: such_pod = st.text_input("🏁 POD:")
        with c3: such_contract = st.text_input("📄 Contract:")
        with c4:
            filter_datum_aktiv = st.checkbox("📅 Datumsfilter", value=True)
            such_datum = st.date_input("Gültig am:", disabled=not filter_datum_aktiv)

        mask = pd.Series([True] * len(df))
        if such_pol: mask &= df['Port of Loading'].astype(str).str.contains(such_pol, case=False, na=False)
        if such_pod: mask &= df['Port of Destination'].astype(str).str.contains(such_pod, case=False, na=False)
        if such_contract: mask &= df['Contract Number'].astype(str).str.contains(such_contract, case=False, na=False)
        if filter_datum_aktiv and 'Valid from dt' in df.columns:
            dt_s = pd.to_datetime(such_datum)
            mask &= (df['Valid from dt'] <= dt_s) & (df['Valid to dt'] >= dt_s)
        
        treffer = df[mask].copy()
        if not treffer.empty and '40HC' in treffer.columns:
            treffer['Total_EUR_Sort'] = treffer.apply(lambda r: berechne_total_eur_dynamic(r, '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', r.name), axis=1)
            treffer = treffer.sort_values(by='Total_EUR_Sort')
            for _, row in treffer.head(50).iterrows():
                with st.expander(f"🚢 {row.get('Carrier')} | {row.get('Port of Loading')} ➡️ {row.get('Port of Destination')} | {row.get('Total_EUR_Sort', 0):.2f} EUR"):
                    anzeige_container_daten(row, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', row.name)

with tab_upload:
    st.write("### 📥 Neue Raten importieren")
    # GEÄNDERTE ZEILE HIER:
    uploaded_files = st.file_uploader("Dateien auswählen", accept_multiple_files=True)
    
    if uploaded_files:
        if st.button("🚀 In Datenbank speichern", type="primary"):
            alle_daten = []
            for datei in uploaded_files:
                df_teil, msg = lade_und_uebersetze_cached(datei.name, datei.getvalue())
                if not df_teil.empty:
                    alle_daten.append(df_teil)
                else:
                    st.error(f"Fehler in {datei.name}: {msg}")
            
            if alle_daten:
                df_u = pd.concat(alle_daten, ignore_index=True)
                df_u['createdAt'] = datetime.now(timezone.utc)
                collection.insert_many(df_u.to_dict('records'))
                st.success(f"{len(df_u)} Raten gespeichert!")
                st.balloons()
    
    st.markdown("---")
    if st.button("🗑️ Datenbank leeren"):
        collection.delete_many({})
        st.success("Geleert!")
