import streamlit as st
import pandas as pd
import re
import PyPDF2
import warnings
import io
import requests  # NEU: Um den Live-Wechselkurs aus dem Internet abzufragen

# 1. Warnungen unterdrücken (für eine saubere Konsole)
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# 2. Konfiguration & Design
st.set_page_config(page_title="Raten-Finder Pro (40'HC)", layout="wide")
st.markdown("""
    <style>
    .all-in-box { background-color: #e6f4ea; border: 2px solid #28a745; padding: 15px; border-radius: 10px; text-align: center; }
    .basis-box { background-color: #e8f0fe; border: 1px solid #1a73e8; padding: 15px; border-radius: 10px; text-align: center; }
    .collect-box { background-color: #fff3cd; border: 1px solid #ffeeba; padding: 15px; border-radius: 10px; margin-bottom: 15px; }
    .fremd-waehrung { color: #d9534f; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

st.title("🚢 Speditions-Raten-Finder (Multi & PDF-Upload)")

# --- NEU: LIVE WECHSELKURS ABRUFEN ---
@st.cache_data(ttl=3600)  # Speichert den Kurs für 1 Stunde, damit die App schnell bleibt
def hole_live_wechselkurs():
    try:
        # Greift auf die offiziellen EZB-Kurse (Europäische Zentralbank) zu
        response = requests.get("https://api.frankfurter.app/latest?from=USD&to=EUR", timeout=5)
        data = response.json()
        live_kurs = data['rates']['EUR']
        return round(live_kurs, 3)
    except Exception:
        # Fallback-Wert, falls du mal kein Internet hast
        return 0.92

# Kurs abrufen
aktueller_kurs = hole_live_wechselkurs()

# 3. Sidebar für Wechselkurse
st.sidebar.header("💱 Einstellungen")
st.sidebar.write("*(Kurs wird stündlich live von der EZB aktualisiert)*")
# Hier setzen wir den abgerufenen Kurs als Standardwert (value) ein
usd_to_eur = st.sidebar.number_input("Wechselkurs: 1 USD in EUR", value=aktueller_kurs, step=0.01)

# 4. Datei-Upload 
uploaded_files = st.file_uploader("Raten-Dateien hochladen (.xlsx, .csv, .pdf)", type=["xlsx", "csv", "pdf"], accept_multiple_files=True)

# --- HILFSFUNKTIONEN FÜR BERECHNUNG ---
def berechne_gebuehren(zuschlaege_str):
    if not isinstance(zuschlaege_str, str) or zuschlaege_str.lower() in ['nan', 'none', '']:
        return []
    pattern = r'([A-Za-z0-9\s\(\)\-]+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})'
    treffer = re.findall(pattern, zuschlaege_str)
    liste = []
    for t in treffer:
        try:
            name = t[0].strip().lstrip(',')
            betrag = float(t[1].replace('.', '').replace(',', '.'))
            liste.append({"name": name, "betrag": betrag, "waehrung": t[2].upper()})
        except: pass
    return liste

def berechne_total_eur(row, price_col, surcharge_col):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    if pd.isna(basis) or basis <= 0: return 99999999 
    
    curr_basis = str(row.get('Currency', 'USD')).upper()
    basis_eur = basis * usd_to_eur if curr_basis == 'USD' else basis
    
    gebuehren = berechne_gebuehren(str(row.get(surcharge_col, '')))
    summe_gebuehren_eur = 0
    for g in gebuehren:
        if g['waehrung'] == 'USD': summe_gebuehren_eur += (g['betrag'] * usd_to_eur)
        elif g['waehrung'] == 'EUR': summe_gebuehren_eur += g['betrag']
            
    return basis_eur + summe_gebuehren_eur

def anzeige_container_daten(row, size_label, price_col, prep_surcharge_col, coll_surcharge_col, row_index):
    basis = pd.to_numeric(row.get(price_col), errors='coerce')
    curr_basis = str(row.get('Currency', 'USD')).upper()
    basis_eur = basis * usd_to_eur if curr_basis == 'USD' else basis
    
    prep_gebuehren = berechne_gebuehren(str(row.get(prep_surcharge_col, '')))
    coll_gebuehren = berechne_gebuehren(str(row.get(coll_surcharge_col, '')))
    
    summe_gebuehren_eur = 0
    fremd_gebuehren = [] 
    
    col_basis, col_prep, col_coll, col_total = st.columns([1, 1.2, 1.2, 1.2])
    
    with col_basis:
        st.markdown(f'<div class="basis-box"><b>Basisfracht {size_label}</b><br><span style="font-size:20px;">{basis:,.2f} {curr_basis}</span><br><small>≈ {basis_eur:.2f} EUR</small></div>', unsafe_allow_html=True)
        
    with col_prep:
        st.write("**Zusammensetzung (Prepaid):**")
        if not prep_gebuehren: st.write("<small>Keine extra Prepaid Gebühren</small>", unsafe_allow_html=True)
        
        for g in prep_gebuehren:
            if g['waehrung'] == 'USD':
                umgerechnet = g['betrag'] * usd_to_eur
                summe_gebuehren_eur += umgerechnet
                st.write(f"➕ {g['name']}: {g['betrag']:.2f} USD <small>(≈ {umgerechnet:.2f} EUR)</small>", unsafe_allow_html=True)
            elif g['waehrung'] == 'EUR':
                summe_gebuehren_eur += g['betrag']
                st.write(f"➕ {g['name']}: {g['betrag']:.2f} EUR", unsafe_allow_html=True)
            else:
                fremd_gebuehren.append(f"{g['betrag']:.2f} {g['waehrung']} ({g['name']})")
                st.markdown(f"➕ <span class='fremd-waehrung'>{g['name']}: {g['betrag']:.2f} {g['waehrung']} (Nicht in Summe)</span>", unsafe_allow_html=True)
                
    with col_coll:
        st.write("**🏢 Collect (Zahlbar Zielort):**")
        if not coll_gebuehren:
            st.write("<small>Keine Collect Gebühren</small>", unsafe_allow_html=True)
        else:
            st.write("<small>Für All-In einrechnen:</small>", unsafe_allow_html=True)
            for i, g in enumerate(coll_gebuehren):
                chk_key = f"chk_{row_index}_{i}_{g['name']}"
                if st.checkbox(f"{g['name']} ({g['betrag']:.2f} {g['waehrung']})", key=chk_key):
                    if g['waehrung'] == 'USD':
                        summe_gebuehren_eur += (g['betrag'] * usd_to_eur)
                    elif g['waehrung'] == 'EUR':
                        summe_gebuehren_eur += g['betrag']
                    else:
                        fremd_gebuehren.append(f"{g['betrag']:.2f} {g['waehrung']} ({g['name']} - Collect)")

    with col_total:
        total_eur = basis_eur + summe_gebuehren_eur
        zusatz_text = f"<br><br><span class='fremd-waehrung'><b>⚠️ Zzgl. Fremdwährungen:</b><br>" + "<br>".join(fremd_gebuehren) + "</span>" if fremd_gebuehren else ""
        st.markdown(f'<div class="all-in-box"><b>Echter All-In Preis</b><br><span style="font-size:26px; font-weight:bold; color:#1e7e34;">{total_eur:.2f} EUR</span>{zusatz_text}</div>', unsafe_allow_html=True)


# --- DATEN EINLESEN UND FORMATIEREN (MIT CACHING) ---
@st.cache_data(show_spinner=False)
def lade_und_uebersetze_cached(file_name, file_bytes):
    datei = io.BytesIO(file_bytes)
    datei.name = file_name
    
    # 1. PDF-DATEIEN VERARBEITEN
    if datei.name.lower().endswith('.pdf'):
        try:
            reader = PyPDF2.PdfReader(datei)
            text = " ".join([page.extract_text() for page in reader.pages])
            
            date_pattern = r'(\d{2,4}[.\-/]\d{2}[.\-/]\d{2,4})'
            valid_matches = re.findall(date_pattern, text)
            v_from = valid_matches[0] if len(valid_matches) > 0 else "Unbekannt"
            v_to = valid_matches[1] if len(valid_matches) > 1 else "Unbekannt"
            
            pol = "Hamburg" if "Hamburg" in text else "Unbekannt"
            pod = "Hamad" if "Hamad" in text else "Unbekannt"
            
            contract_no = "Unbekannt"
            contract_match = re.search(r'(?:Contract Filing Reference|Contract|Quote)[\s\S]{1,350}?\b([A-Z]*\d{5,}[A-Z0-9]*)\b', text, re.IGNORECASE)
            if contract_match:
                contract_no = contract_match.group(1)
            else:
                msc_match = re.search(r'\b(R\d{12,18})\b', text)
                if msc_match: contract_no = msc_match.group(1)
            
            rate_match = re.search(r'(\d{3,4})\s*(USD|EUR)', text)
            rate = float(rate_match.group(1)) if rate_match else 0
            curr = rate_match.group(2) if rate_match else "USD"
            
            surcharges = []
            erc_match = re.search(r'Logistic Fee.*?(\d+)\s*(EUR|USD)', text)
            if erc_match:
                surcharges.append(f"ERC = {erc_match.group(1)} {erc_match.group(2)}")
                
            df_pdf = pd.DataFrame([{
                'Carrier': 'MSC (aus PDF)',
                'Contract Number': contract_no,
                'Port of Loading': pol,
                'Port of Destination': pod,
                'Valid from': v_from,
                'Valid to': v_to,
                '40HC': rate,
                'Currency': curr,
                'Included Prepaid Surcharges 40HC': ", ".join(surcharges),
                'Included Collect Surcharges 40HC': "",
                'Remark': 'Automatisch aus PDF importiert'
            }])
            return df_pdf, "PDF-Angebot"
        except Exception as e:
            return pd.DataFrame(), f"PDF Fehler: {e}"

    # 2. EXCEL UND CSV VERARBEITEN (MULTI-SHEET LOGIK)
    else:
        if datei.name.endswith('.xlsx'):
            excel_data = pd.read_excel(datei, sheet_name=None, header=None)
            df_raw = pd.DataFrame()
            
            for sheet_name, df_sheet in excel_data.items():
                for i in range(min(20, len(df_sheet))):
                    row_str = " ".join(df_sheet.iloc[i].dropna().astype(str))
                    if '40HDRY' in row_str or 'Port of Destination' in row_str or '40HC All In' in row_str:
                        df_raw = df_sheet
                        break
                if not df_raw.empty:
                    break
            
            if df_raw.empty:
                df_raw = list(excel_data.values())[0]
        else:
            df_raw = pd.read_csv(datei, header=None, low_memory=False)

        is_maersk = False
        header_idx = 0
        global_contract = "Unbekannt"
        
        # Contract im Dateinamen suchen
        fn_match = re.search(r'(?:rate|quote|contract|ref)[\s_0-9-]*?(\d{5,})', datei.name, re.IGNORECASE)
        if fn_match: global_contract = fn_match.group(1)
            
        # Header im korrekten Sheet suchen
        for i in range(min(20, len(df_raw))):
            row_str = " ".join(df_raw.iloc[i].dropna().astype(str))
            row_lower = row_str.lower()
            
            # PRIORITÄT 1: Contract Number
            if 'contract' in row_lower:
                found_numbers = re.findall(r'\b\d{6,12}\b', row_str)
                if found_numbers:
                    global_contract = " / ".join(dict.fromkeys(found_numbers))
                    
            # PRIORITÄT 2: Quote Number Fallback
            elif global_contract == "Unbekannt" and any(word in row_lower for word in ['quote', 'reference', 'ref']):
                found_numbers = re.findall(r'\b\d{6,12}\b', row_str)
                if found_numbers:
                    global_contract = " / ".join(dict.fromkeys(found_numbers))
                
            if '40HDRY' in row_str and 'Charge' in row_str:
                is_maersk = True
                header_idx = i
                break
            elif '40HC All In' in row_str or 'Port of Destination' in row_str:
                header_idx = i
                break
                
        rohe_spalten = df_raw.iloc[header_idx].astype(str).tolist()
        neue_spalten = []
        gesehen = {}
        for spalte in rohe_spalten:
            spalte = spalte.strip()
            if spalte in gesehen:
                gesehen[spalte] += 1
                neue_spalten.append(f"{spalte}.{gesehen[spalte]}")
            else:
                gesehen[spalte] = 0
                neue_spalten.append(spalte)
                
        df_raw.columns = neue_spalten
        df_raw = df_raw.iloc[header_idx+1:].reset_index(drop=True)
        
        contract_col = next((c for c in df_raw.columns if any(x in c.lower() for x in ['contract', 'quote', 'reference'])), None)
        
        if is_maersk:
            standard_rows = []
            df_raw = df_raw.dropna(subset=['40HDRY'])
            for name, group in df_raw.groupby(['POL', 'POD', 'Effective Date', 'Expiry Date']):
                pol, pod, eff, exp = name
                bas_row = group[group['Charge'] == 'BAS']
                if bas_row.empty: continue
                bas_val_raw = str(bas_row['40HDRY'].values[0]).strip()
                if bas_val_raw.lower() in ['nan', 'inclusive', 'none', '']: continue
                
                parts = bas_val_raw.split()
                if len(parts) >= 2:
                    curr, val = parts[0], float(parts[1].replace(',', ''))
                else: continue
                
                row_contract = str(bas_row[contract_col].values[0]) if contract_col else global_contract
                surcharges_prep = [f"{s_row['Charge']} = {s_row['40HDRY']}" for _, s_row in group[group['Charge'] != 'BAS'].iterrows() if ' ' in str(s_row['40HDRY'])]

                standard_rows.append({
                    'Carrier': 'Maersk', 'Contract Number': row_contract, 'Port of Loading': pol, 'Port of Destination': pod,
                    'Valid from': eff, 'Valid to': exp, '40HC': val, 'Currency': curr,
                    'Included Prepaid Surcharges 40HC': ", ".join(surcharges_prep),
                    'Included Collect Surcharges 40HC': "", 
                    'Remark': f"Transit Time: {bas_row['Transit Time'].values[0]}" if 'Transit Time' in bas_row.columns else ""
                })
            return pd.DataFrame(standard_rows), "Maersk"
        else:
            df_raw['Contract Number'] = df_raw[contract_col].astype(str).fillna(global_contract) if contract_col else global_contract
            return df_raw, "Standard/MSC"

# --- HAUPTPROGRAMM ---
if not uploaded_files:
    st.info("💡 Bitte lade oben eine Excel- oder PDF-Datei hoch, um die Suche zu starten.")
else: 
    alle_daten = []
    
    with st.spinner("Lese Dateien ein..."):
        for datei in uploaded_files:
            try:
                file_bytes = datei.getvalue()
                df_teil, format_name = lade_und_uebersetze_cached(datei.name, file_bytes)
                
                if not df_teil.empty:
                    alle_daten.append(df_teil)
                    st.success(f"✅ {datei.name} erfolgreich verarbeitet ({format_name})")
            except Exception as e: 
                st.error(f"Fehler beim Verarbeiten von {datei.name}: {e}")
    
    if alle_daten:
        df = pd.concat(alle_daten, ignore_index=True)
        st.markdown("---")
        
        if 'Valid from' in df.columns and 'Valid to' in df.columns:
            df['Valid from dt'] = pd.to_datetime(df['Valid from'], dayfirst=True, errors='coerce')
            df['Valid to dt'] = pd.to_datetime(df['Valid to'], dayfirst=True, errors='coerce')
        
        st.write("### 🔍 Suche in ALLEN hochgeladenen Dokumenten")
        search_col1, search_col2, search_col3, search_col4 = st.columns(4)
        with search_col1: 
            such_pol = st.text_input("📍 Ladehafen (POL):", placeholder="z.B. Hamburg")
        with search_col2: 
            such_pod = st.text_input("🏁 Zielhafen (POD):", placeholder="z.B. Hamad")
        with search_col3: 
            such_contract = st.text_input("📄 Contract Nr.:", placeholder="z.B. 299424203")
        with search_col4:
            filter_datum_aktiv = st.checkbox("📅 Datumsfilter aktiv", value=True)
            such_datum = st.date_input("Rate gültig am:", disabled=not filter_datum_aktiv)

        mask = pd.Series([True] * len(df))
        if such_pol and 'Port of Loading' in df.columns: 
            mask &= df['Port of Loading'].astype(str).str.contains(such_pol, case=False, na=False)
        if such_pod and 'Port of Destination' in df.columns: 
            mask &= df['Port of Destination'].astype(str).str.contains(such_pod, case=False, na=False)
        if such_contract and 'Contract Number' in df.columns:
            mask &= df['Contract Number'].astype(str).str.contains(such_contract, case=False, na=False)
        if filter_datum_aktiv and 'Valid from dt' in df.columns:
            such_dt = pd.to_datetime(such_datum)
            mask &= (df['Valid from dt'] <= such_dt) & (df['Valid to dt'] >= such_dt)
        
        treffer = df[mask].copy()
        if '40HC' in treffer.columns:
            treffer['40HC_Check'] = pd.to_numeric(treffer['40HC'], errors='coerce')
            treffer = treffer[treffer['40HC_Check'] > 0]
            
            if not treffer.empty:
                treffer['Total_EUR_Sort'] = treffer.apply(lambda row: berechne_total_eur(row, '40HC', 'Included Prepaid Surcharges 40HC'), axis=1)
                treffer = treffer.sort_values(by='Total_EUR_Sort')
                
                st.success(f"✅ {len(treffer)} gültige Raten gefunden")
                for index, row in treffer.iterrows():
                    is_best = (row['Total_EUR_Sort'] == treffer['Total_EUR_Sort'].iloc[0])
                    label = f"{'🏆 BESTER PREIS | ' if is_best else ''}🚢 {row.get('Carrier')} | 📄 {row.get('Contract Number')} | {row.get('Port of Loading')} ➡️ {row.get('Port of Destination')}"
                    with st.expander(label):
                        anzeige_container_daten(row, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', index)
                        if pd.notna(row.get('Remark')): st.info(f"**💡 Bemerkung:** {row['Remark']}")
            else:
                st.warning("Keine gültigen Raten für diese Suche gefunden.")