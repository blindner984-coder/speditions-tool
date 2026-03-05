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



# --- DATEI READER FÜR DEN ADMIN-UPLOAD ---

def lade_und_uebersetze_cached(file_name, file_bytes):

    datei = io.BytesIO(file_bytes)

    datei.name = file_name

    

    if datei.name.lower().endswith('.pdf'):

        try:

            reader = PyPDF2.PdfReader(datei)

            text = " ".join([page.extract_text() for page in reader.pages])

            

            date_matches = re.findall(r'(\d{2,4}[.\-/]\d{2}[.\-/]\d{2,4})', text)

            v_from = date_matches[0] if len(date_matches) > 0 else "Unbekannt"

            v_to = date_matches[1] if len(date_matches) > 1 else "Unbekannt"

            

            pol_match = re.search(r'(?:POL|Port of Loading|From)[\s:]{1,3}([A-Za-z\s\.,]+)(?:POD|Port of Discharge|To|Vessel|Voyage|\n)', text, re.IGNORECASE)

            pod_match = re.search(r'(?:POD|Port of Discharge|Destination|To)[\s:]{1,3}([A-Za-z\s\.,]+)(?:Vessel|Voyage|Commodity|Term|\n)', text, re.IGNORECASE)



            contract_match = re.search(r'(?:Contract Filing Reference|Contract|Quote)[\s\S]{1,350}?\b([A-Z]*\d{5,}[A-Z0-9]*)\b', text, re.IGNORECASE)

            contract_no = contract_match.group(1) if contract_match else (re.search(r'\b(R\d{12,18})\b', text).group(1) if re.search(r'\b(R\d{12,18})\b', text) else "Unbekannt")

            

            rate_match = re.search(r'(\d{3,4})\s*(USD|EUR)', text)

            erc_match = re.search(r'Logistic Fee.*?(\d+)\s*(EUR|USD)', text)

            

            df_pdf = pd.DataFrame([{

                'Carrier': 'MSC (aus PDF)',

                'Contract Number': contract_no,

                'Port of Loading': pol_match.group(1).strip() if pol_match else "Unbekannt",

                'Port of Destination': pod_match.group(1).strip() if pod_match else "Unbekannt",

                'Valid from': v_from,

                'Valid to': v_to,

                '40HC': float(rate_match.group(1)) if rate_match else 0,

                'Currency': rate_match.group(2) if rate_match else "USD",

                'Included Prepaid Surcharges 40HC': f"ERC = {erc_match.group(1)} {erc_match.group(2)}" if erc_match else "",

                'Included Collect Surcharges 40HC': "",

                'Remark': 'Automatisch aus PDF importiert'

            }])

            return df_pdf, "PDF"

        except Exception as e: return pd.DataFrame(), f"Fehler: {e}"



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



        # --- 🚨 FIX FÜR DIE PRÄZISE CONTRACT NUMBER (Ignoriert Quote Number) ---

        global_contract = "Unbekannt"

        

        # 1. Zuerst gezielt nach dem Wort "Contract" in den Excel-Kopfzeilen suchen

        for i in range(min(20, len(df_raw))):

            row_vals = df_raw.iloc[i].dropna().astype(str).tolist()

            for j, val in enumerate(row_vals):

                v_low = val.lower()

                # Wir suchen explizit nach "Contract", um nicht die darüber stehende Quote zu nehmen

                if 'contract' in v_low:

                    # Suche nach einer Nummer direkt in dieser Zelle (z.B. "Contract Number 299424203")

                    nums = re.findall(r'\b\d{6,10}\b', val)

                    if nums:

                        global_contract = nums[0]

                        break

                    # Falls nicht in derselben Zelle, schaue in die nächsten 3 Zellen rechts daneben

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



        # 2. Nur wenn oben nichts gefunden wurde, Fallback auf den Dateinamen

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

        if 'Valid to' in df_raw.columns: df_raw['Valid to dt'] = pd.to_datetime(df_raw['Valid to'], dayfirst=True,
