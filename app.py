import streamlit as st
import pandas as pd
import re
import PyPDF2
import warnings
import io
import os
import hmac
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

MAX_UPLOAD_SIZE_MB = 15
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024
MAX_ADMIN_LOGIN_ATTEMPTS = 5
ADMIN_LOCK_SECONDS = 300


def hole_konfiguration(key, default=None):
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)

# --- MONGODB ANBINDUNG ---
MONGO_URI = hole_konfiguration("MONGO_URI")
ADMIN_PASSWORD = hole_konfiguration("ADMIN_PASSWORD")

if not MONGO_URI:
    st.error("Sicherheits-Konfiguration fehlt: Bitte MONGO_URI als Secret oder Umgebungsvariable setzen.")
    st.stop()

@st.cache_resource
def init_db():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["SpeditionsDB"]
        collection = db["Raten"]
        collection.create_index("createdAt", expireAfterSeconds=15552000)
        return collection
    except Exception:
        st.error("Datenbankverbindung fehlgeschlagen. Bitte MONGO_URI und Netzfreigaben prüfen.")
        st.stop()

collection = init_db()

# --- LIVE WECHSELKURS ---
@st.cache_data(ttl=3600)
def hole_live_wechselkurs():
    try:
        response = requests.get("https://api.frankfurter.app/latest?from=USD&to=EUR", timeout=5)
        response.raise_for_status()
        eur_rate = response.json().get("rates", {}).get("EUR")
        if isinstance(eur_rate, (int, float)):
            return round(float(eur_rate), 3)
    except Exception:
        pass
    return 0.92

aktueller_kurs = hole_live_wechselkurs()

st.sidebar.header("💱 Einstellungen")
st.sidebar.write("*(Kurs wird stündlich live von der EZB aktualisiert)*")
usd_to_eur = st.sidebar.number_input("Wechselkurs: 1 USD in EUR", value=aktueller_kurs, step=0.01)


def parse_decimal_wert(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none"}:
        return None

    text = text.replace(" ", "")
    sign = -1 if text.startswith("-") else 1
    text = text.lstrip("+-")
    cleaned = re.sub(r"[^0-9,\.]", "", text)
    if not cleaned:
        return None

    if "." in cleaned and "," in cleaned:
        decimal_sep = "." if cleaned.rfind(".") > cleaned.rfind(",") else ","
        thousands_sep = "," if decimal_sep == "." else "."
        normalized = cleaned.replace(thousands_sep, "")
        if decimal_sep == ",":
            normalized = normalized.replace(",", ".")
    elif "." in cleaned or "," in cleaned:
        sep = "." if "." in cleaned else ","
        parts = cleaned.split(sep)

        if len(parts) == 2:
            whole, fractional = parts
            if len(fractional) == 3 and len(whole) <= 3:
                normalized = whole + fractional
            else:
                normalized = whole + "." + fractional
        else:
            last = parts[-1]
            if len(last) == 3:
                normalized = "".join(parts)
            else:
                normalized = "".join(parts[:-1]) + "." + last
    else:
        normalized = cleaned

    try:
        return sign * float(normalized)
    except ValueError:
        return None


def extrahiere_waehrung_und_betrag(text, default_currency="USD"):
    text_str = str(text)
    waehrung_match = re.search(r"\b(USD|EUR)\b", text_str, re.IGNORECASE)
    betrag_match = re.search(r"(\d[\d\.,]*)", text_str)
    betrag = parse_decimal_wert(betrag_match.group(1)) if betrag_match else None
    waehrung = waehrung_match.group(1).upper() if waehrung_match else default_currency
    return waehrung, betrag


def normalisiere_datum_token(date_text):
    token = str(date_text).strip()
    parts = re.split(r"[./-]", token)
    if len(parts) == 3 and len(parts[1]) == 3 and parts[1].startswith("0"):
        parts[1] = parts[1][:2]
    if len(parts) == 3:
        return f"{parts[0]}.{parts[1]}.{parts[2]}"
    return token


def normalisiere_pol_text(pol_text):
    abkuerzungen = {
        "HAM": "Hamburg",
        "BRV": "Bremerhaven",
        "ANR": "Antwerp",
        "RTM": "Rotterdam",
    }
    text = str(pol_text or "")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^via\s+POL\s+", "", text, flags=re.IGNORECASE)
    text = re.split(r"\bValidity\b", text, flags=re.IGNORECASE)[0]

    teile = [t.strip().strip(" .,-") for t in re.split(r"[/,]|\band\b", text, flags=re.IGNORECASE)]
    out = []
    gesehen = set()
    for t in teile:
        if not t:
            continue
        norm = abkuerzungen.get(t.upper(), t)
        if norm not in gesehen:
            out.append(norm)
            gesehen.add(norm)
    return "/".join(out) if out else "Unbekannt"


def extrahiere_msc_quote_pdf_daten(text):
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in str(text).splitlines() if ln and ln.strip()]
    if not lines:
        return None

    joined = "\n".join(lines)
    lower_joined = joined.lower()
    if "our quote for export" not in lower_joined or "port of discharge" not in lower_joined:
        return None

    contract_match = re.search(r"\bR\d{8,}\b", joined, re.IGNORECASE)
    contract_no = contract_match.group(0).upper() if contract_match else "Unbekannt"

    pol_match = re.search(r"Ports?\s+of\s+Loading\s+([A-Za-z][A-Za-z\s\-/,]+)", joined, re.IGNORECASE)
    pol_raw = pol_match.group(1).strip() if pol_match else "Unbekannt"
    pol_str = normalisiere_pol_text(pol_raw)

    normalized_text = re.sub(r"\s+", " ", joined)
    valid_match = re.search(
        r"Valid(?:ity)?\s*Valid\s+as\s+from\s*(\d{1,2}[./-]\d{1,3}[./-]\d{2,4}).{0,140}?(?:not\s+beyond|until)\s*(\d{1,2}[./-]\d{1,3}[./-]\d{2,4})",
        normalized_text,
        re.IGNORECASE,
    )
    if valid_match:
        v_from = normalisiere_datum_token(valid_match.group(1))
        v_to = normalisiere_datum_token(valid_match.group(2))
    else:
        date_matches = [normalisiere_datum_token(d) for d in re.findall(r"(\d{1,2}[./-]\d{1,3}[./-]\d{2,4})", normalized_text)]
        v_from = date_matches[0] if len(date_matches) > 0 else "Unbekannt"
        v_to = date_matches[1] if len(date_matches) > 1 else "Unbekannt"

    prepaid_liste = []
    collect_liste = []

    def fuege_gebuehr_hinzu(ziel, code, amount, waehrung):
        if amount is None:
            return
        ziel.append((str(code).strip().upper() or "SUR", float(amount), str(waehrung).upper()))

    sea_start = next((i for i, line in enumerate(lines) if "surcharges related to sea freight" in line.lower()), None)
    if sea_start is not None:
        sea_end = len(lines)
        for i in range(sea_start + 1, len(lines)):
            if re.match(r"^\d+\.\s", lines[i]):
                sea_end = i
                break

        beste_codes = {}
        seq = 0
        for line in lines[sea_start + 1:sea_end]:
            low = line.lower()
            if "not subject to" in low:
                continue
            if low.startswith("code ") or "surcharge name amount" in low:
                continue
            if low.startswith("haz ") or "hazardous cargo" in low:
                continue

            treffer = list(re.finditer(r"(\d[\d\.,]*)\s*(USD|EUR)\b", line, re.IGNORECASE))
            if not treffer:
                continue

            code_match = re.match(r"^([A-Z]{2,5})\b", line)
            code = code_match.group(1).upper() if code_match else "SUR"

            if re.search(r"per\s*40|40['´]", line, re.IGNORECASE):
                priority = 4
                multiplier = 1
            elif re.search(r"\bTEU\b", line, re.IGNORECASE):
                priority = 3
                multiplier = 2
            elif re.search(r"\bContainer\b", line, re.IGNORECASE):
                priority = 2
                multiplier = 1
            elif re.search(r"per\s*20|20['´]", line, re.IGNORECASE):
                priority = 1
                multiplier = 1
            else:
                priority = 0
                multiplier = 1

            amount = parse_decimal_wert(treffer[0].group(1))
            waehrung = treffer[0].group(2).upper()
            if amount is None:
                continue
            seq += 1
            kandidat = {
                "priority": priority,
                "seq": seq,
                "amount": amount * multiplier,
                "waehrung": waehrung,
            }
            alt = beste_codes.get(code)
            if alt is None or kandidat["priority"] > alt["priority"] or (kandidat["priority"] == alt["priority"] and kandidat["seq"] > alt["seq"]):
                beste_codes[code] = kandidat

        for code, val in sorted(beste_codes.items(), key=lambda kv: kv[1]["seq"]):
            fuege_gebuehr_hinzu(prepaid_liste, code, val["amount"], val["waehrung"])

    dest_start = next((i for i, line in enumerate(lines) if "destination charges" in line.lower()), None)
    if dest_start is not None:
        dest_end = len(lines)
        for i in range(dest_start + 1, len(lines)):
            low = lines[i].lower()
            if "named account" in low or "contract filing reference" in low or "rate applicability" in low:
                dest_end = i
                break

        last_code = "COLLECT"
        collect_code_counter = {}
        for line in lines[dest_start + 1:dest_end]:
            low = line.lower()
            if "collect" not in low:
                continue

            treffer = list(re.finditer(r"(\d[\d\.,]*)\s*(USD|EUR)\b", line, re.IGNORECASE))
            if not treffer:
                continue

            code_match = re.match(r"^([A-Z]{2,5})\b", line)
            if code_match:
                last_code = code_match.group(1).upper()

            bevorzugt = []
            for m in treffer:
                tail = line[m.end(): m.end() + 30].lower()
                if "per 40" in tail or "40´" in tail or "40'" in tail:
                    bevorzugt.append(m)
            kandidaten = bevorzugt if bevorzugt else treffer

            for m in kandidaten:
                amount = parse_decimal_wert(m.group(1))
                waehrung = m.group(2).upper()
                if amount is None:
                    continue
                collect_code_counter[last_code] = collect_code_counter.get(last_code, 0) + 1
                suffix = "" if collect_code_counter[last_code] == 1 else f"_{collect_code_counter[last_code]}"
                fuege_gebuehr_hinzu(collect_liste, f"{last_code}{suffix}", amount, waehrung)

    prepaid_str = ", ".join([f"{c} = {a:.2f} {w}" for c, a, w in prepaid_liste])
    collect_str = ", ".join([f"{c} = {a:.2f} {w}" for c, a, w in collect_liste])

    route_rows = []
    current_pod = None
    for line in lines:
        header_match = re.search(r"^Port of Discharge\s+(.+?)\s+20['´]DV\s+40['´]DV/HC", line, re.IGNORECASE)
        if header_match:
            current_pod = header_match.group(1).strip(" .,-")
            continue

        if current_pod and re.search(r"^via\s+POL\s+", line, re.IGNORECASE):
            if "on rqst" in line.lower():
                continue

            treffer = list(re.finditer(r"(\d[\d\.,]*)\s*(USD|EUR)\b", line, re.IGNORECASE))
            if len(treffer) < 2:
                continue

            forty_match = treffer[1]
            rate_value = parse_decimal_wert(forty_match.group(1))
            rate_currency = forty_match.group(2).upper()
            if rate_value is None:
                continue

            via_match = re.search(r"via\s+POL\s+(.+?)(?=\s+\d[\d\.,]*\s*(?:USD|EUR)|\s+on\s+rqst|$)", line, re.IGNORECASE)
            via_pol_raw = via_match.group(1).strip() if via_match else pol_str
            via_pol = normalisiere_pol_text(via_pol_raw)
            via_hinweis = f"via POL {via_pol_raw}" if via_match else ""

            route_rows.append({
                'Carrier': 'MSC (aus PDF)',
                'Contract Number': contract_no,
                'Port of Loading': via_pol,
                'Port of Destination': current_pod,
                'Valid from': v_from,
                'Valid to': v_to,
                '40HC': rate_value,
                'Currency': rate_currency,
                'Included Prepaid Surcharges 40HC': prepaid_str,
                'Included Collect Surcharges 40HC': collect_str,
                'Remark': f"Automatisch aus MSC Quote PDF importiert ({via_hinweis})".strip(),
            })

    if route_rows:
        return route_rows

    route_line = ""
    for idx, line in enumerate(lines):
        if "port of discharge" in line.lower():
            for probe in lines[idx + 1: idx + 5]:
                if re.search(r"\d[\d\.,]*\s*(USD|EUR)\b", probe, re.IGNORECASE):
                    route_line = probe
                    break
            if route_line:
                break

    rate_match = re.search(r"(\d[\d\.,]*)\s*(USD|EUR)\b", route_line, re.IGNORECASE)
    if not rate_match:
        return None

    rate_value = parse_decimal_wert(rate_match.group(1))
    if rate_value is None:
        return None
    rate_currency = rate_match.group(2).upper()

    pod_raw = route_line[:rate_match.start()].strip()
    pod_raw = re.sub(r"\b\d+\s*TEU\b", "", pod_raw, flags=re.IGNORECASE)
    pod_raw = re.sub(r"^[A-Z]{2,3}\s+(?=[A-Za-z])", "", pod_raw).strip()
    pod_str = re.sub(r"\s+", " ", pod_raw).strip(" .,-") or "Unbekannt"

    return {
        'Carrier': 'MSC (aus PDF)',
        'Contract Number': contract_no,
        'Port of Loading': pol_str,
        'Port of Destination': pod_str,
        'Valid from': v_from,
        'Valid to': v_to,
        '40HC': rate_value,
        'Currency': rate_currency,
        'Included Prepaid Surcharges 40HC': prepaid_str,
        'Included Collect Surcharges 40HC': collect_str,
        'Remark': 'Automatisch aus MSC Quote PDF importiert',
    }


def admin_login_bereich():
    st.write("### 🔐 Admin-Zugang")
    if not ADMIN_PASSWORD:
        st.error("ADMIN_PASSWORD ist nicht gesetzt. Admin-Funktionen sind aus Sicherheitsgründen deaktiviert.")
        return False

    expected_password = str(ADMIN_PASSWORD)
    now_ts = datetime.now(timezone.utc).timestamp()
    locked_until = float(st.session_state.get("admin_locked_until", 0))

    if locked_until > now_ts:
        wait_seconds = int(locked_until - now_ts)
        st.error(f"Zu viele Fehlversuche. Bitte in {wait_seconds} Sekunden erneut versuchen.")
        return False

    if st.session_state.get("admin_authenticated", False):
        st.success("Admin-Zugang aktiv.")
        if st.button("🔓 Admin abmelden", key="admin_logout"):
            st.session_state["admin_authenticated"] = False
            st.rerun()
        return True

    input_password = st.text_input("Admin-Passwort", type="password", key="admin_password_input")
    if st.button("🔐 Admin anmelden", key="admin_login"):
        if hmac.compare_digest(str(input_password), expected_password):
            st.session_state["admin_authenticated"] = True
            st.session_state["admin_failed_attempts"] = 0
            st.session_state["admin_locked_until"] = 0
            st.success("Anmeldung erfolgreich.")
            st.rerun()
        else:
            failed_attempts = int(st.session_state.get("admin_failed_attempts", 0)) + 1
            st.session_state["admin_failed_attempts"] = failed_attempts

            if failed_attempts >= MAX_ADMIN_LOGIN_ATTEMPTS:
                st.session_state["admin_locked_until"] = now_ts + ADMIN_LOCK_SECONDS
                st.session_state["admin_failed_attempts"] = 0
                st.error(f"Zu viele Fehlversuche. Login ist für {ADMIN_LOCK_SECONDS} Sekunden gesperrt.")
            else:
                remaining = MAX_ADMIN_LOGIN_ATTEMPTS - failed_attempts
                st.error(f"Falsches Passwort. Verbleibende Versuche: {remaining}")
    return False

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
        try:
            betrag = parse_decimal_wert(t[1])
            if betrag is None:
                continue
            liste.append({"name": t[0].strip().lstrip(','), "betrag": betrag, "waehrung": t[2].upper()})
        except (ValueError, IndexError):
            pass
    return liste

def berechne_total_eur_dynamic(row, price_col, prep_surcharge_col, coll_surcharge_col, row_index):
    basis = parse_decimal_wert(row.get(price_col))
    if basis is None:
        return 99999999
    
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
    basis = parse_decimal_wert(row.get(price_col))
    if basis is None:
        st.warning("Basispreis konnte nicht eindeutig gelesen werden.")
        return

    curr_basis = str(row.get('Currency', 'USD')).upper()
    basis_eur = basis * usd_to_eur if curr_basis == 'USD' else basis
    
    prep_gebuehren = berechne_gebuehren(str(row.get(prep_surcharge_col, '')))
    coll_gebuehren = berechne_gebuehren(str(row.get(coll_surcharge_col, '')))
    summe_gebuehren_eur, fremd_gebuehren, doc_gebuehren = 0, [], [] 
    
    col_basis, col_prep, col_coll, col_doc, col_total = st.columns([1, 1.1, 1.1, 1.1, 1.2])
    
    with col_basis: 
        basis_hinweis = "<br><small class='fremd-waehrung'>Hinweis: negative Basisfracht (z.B. Rabatt/Korrektur)</small>" if basis < 0 else ""
        st.markdown(f'<div class="basis-box"><b>Basisfracht {size_label}</b><br><span style="font-size:20px;">{basis:,.2f} {curr_basis}</span><br><small>≈ {basis_eur:.2f} EUR</small>{basis_hinweis}</div>', unsafe_allow_html=True)
        
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
            text = "\n".join([(page.extract_text() or "") for page in reader.pages])
            if not text.strip():
                return pd.DataFrame(), "Fehler: PDF enthält keinen auslesbaren Text."

            msc_quote_data = extrahiere_msc_quote_pdf_daten(text)
            if msc_quote_data:
                msc_rows = msc_quote_data if isinstance(msc_quote_data, list) else [msc_quote_data]
                df_pdf = pd.DataFrame(msc_rows)
                if 'Valid from' in df_pdf.columns:
                    df_pdf['Valid from dt'] = pd.to_datetime(df_pdf['Valid from'], dayfirst=True, errors='coerce').astype(str)
                if 'Valid to' in df_pdf.columns:
                    df_pdf['Valid to dt'] = pd.to_datetime(df_pdf['Valid to'], dayfirst=True, errors='coerce').astype(str)
                return df_pdf, "PDF (MSC Quote)"
            
            date_matches = re.findall(r'(\d{2,4}[.\-/]\d{2}[.\-/]\d{2,4})', text)
            v_from = normalisiere_datum_token(date_matches[0]) if len(date_matches) > 0 else "Unbekannt"
            v_to = normalisiere_datum_token(date_matches[1]) if len(date_matches) > 1 else "Unbekannt"
            
            pol_match = re.search(r'(?:POL|Port of Loading|From)[\s:]{1,3}([A-Za-z\s\.,]+)(?:POD|Port of Discharge|To|Vessel|Voyage|\n)', text, re.IGNORECASE)
            pod_match = re.search(r'(?:POD|Port of Discharge|Destination|To)[\s:]{1,3}([A-Za-z\s\.,]+)(?:Vessel|Voyage|Commodity|Term|\n)', text, re.IGNORECASE)

            contract_match = re.search(r'(?:Contract Filing Reference|Contract|Quote)[\s\S]{1,350}?\b([A-Z]*\d{5,}[A-Z0-9]*)\b', text, re.IGNORECASE)
            contract_no = contract_match.group(1) if contract_match else (re.search(r'\b(R\d{12,18})\b', text).group(1) if re.search(r'\b(R\d{12,18})\b', text) else "Unbekannt")
            
            rate_match = re.search(r'(\d[\d\.,]*)\s*(USD|EUR)\b', text)
            rate_value = parse_decimal_wert(rate_match.group(1)) if rate_match else None
            rate_currency = rate_match.group(2).upper() if rate_match else "USD"
            erc_match = re.search(r'Logistic Fee.*?(\d[\d\.,]*)\s*(EUR|USD)', text, re.IGNORECASE | re.DOTALL)
            
            df_pdf = pd.DataFrame([{
                'Carrier': 'MSC (aus PDF)',
                'Contract Number': contract_no,
                'Port of Loading': pol_match.group(1).strip() if pol_match else "Unbekannt",
                'Port of Destination': pod_match.group(1).strip() if pod_match else "Unbekannt",
                'Valid from': v_from,
                'Valid to': v_to,
                '40HC': rate_value if rate_value is not None else 0,
                'Currency': rate_currency,
                'Included Prepaid Surcharges 40HC': f"ERC = {erc_match.group(1)} {erc_match.group(2)}" if erc_match else "",
                'Included Collect Surcharges 40HC': "",
                'Remark': 'Automatisch aus PDF importiert'
            }])

            if 'Valid from' in df_pdf.columns:
                df_pdf['Valid from dt'] = pd.to_datetime(df_pdf['Valid from'], dayfirst=True, errors='coerce').astype(str)
            if 'Valid to' in df_pdf.columns:
                df_pdf['Valid to dt'] = pd.to_datetime(df_pdf['Valid to'], dayfirst=True, errors='coerce').astype(str)

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
        if 'Valid to' in df_raw.columns: df_raw['Valid to dt'] = pd.to_datetime(df_raw['Valid to'], dayfirst=True, errors='coerce').astype(str)

        if '40HDRY' in df_raw.columns and 'Charge' in df_raw.columns:
            standard_rows = []
            for name, group in df_raw.dropna(subset=['40HDRY']).groupby(['POL', 'POD', 'Effective Date', 'Expiry Date']):
                bas_row = group[group['Charge'] == 'BAS']
                if bas_row.empty: continue
                bas_text = str(bas_row['40HDRY'].values[0]).strip()
                waehrung, basis_betrag = extrahiere_waehrung_und_betrag(bas_text, default_currency='USD')
                if basis_betrag is None or basis_betrag <= 0:
                    continue
                
                # Wir priorisieren die gefundene globale Contract Number
                row_contract = global_contract
                
                standard_rows.append({
                    'Carrier': 'Maersk', 'Contract Number': row_contract, 
                    'Port of Loading': name[0], 'Port of Destination': name[1], 'Valid from': name[2], 'Valid to': name[3], 
                    '40HC': basis_betrag, 'Currency': waehrung,
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
            historische_raten = st.checkbox(
                "🕘 Historische/abgelaufene Raten anzeigen",
                value=False,
                help="Wenn aktiviert, werden auch alte und nicht mehr gültige Raten angezeigt.",
            )
            such_datum = st.date_input(
                "Rate gültig am:",
                value=datetime.now(timezone.utc).date(),
                disabled=historische_raten,
            )

        mask = pd.Series([True] * len(df))
        if such_pol and 'Port of Loading' in df.columns: mask &= df['Port of Loading'].astype(str).str.contains(such_pol, case=False, na=False)
        if such_pod and 'Port of Destination' in df.columns: mask &= df['Port of Destination'].astype(str).str.contains(such_pod, case=False, na=False)
        if such_contract and 'Contract Number' in df.columns: mask &= df['Contract Number'].astype(str).str.contains(such_contract, case=False, na=False)
        if not historische_raten and 'Valid from dt' in df.columns and 'Valid to dt' in df.columns:
            dt_search = pd.to_datetime(such_datum)
            mask &= (df['Valid from dt'] <= dt_search) & (df['Valid to dt'] >= dt_search)
        elif historische_raten:
            st.warning("Historische Ansicht aktiv: Es werden auch abgelaufene Raten angezeigt.")
        
        treffer = df[mask].copy()
        if '40HC' in treffer.columns:
            treffer['40HC_Check'] = treffer['40HC'].apply(parse_decimal_wert)
            treffer = treffer[treffer['40HC_Check'].notna()].reset_index(drop=True)
            
            if not treffer.empty:
                treffer['Total_EUR_Sort'] = treffer.apply(lambda r: berechne_total_eur_dynamic(r, '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', r.name), axis=1)
                treffer = treffer.sort_values(by='Total_EUR_Sort')

                if historische_raten:
                    st.success(f"✅ {len(treffer)} Raten gefunden (inkl. historischer). Zeige die Top {min(50, len(treffer))} günstigsten an:")
                else:
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
    is_admin = admin_login_bereich()

    if is_admin:
        st.write("### 📥 Neue Raten-Dateien in die Datenbank importieren")
        st.caption(f"Maximale Dateigröße pro Datei: {MAX_UPLOAD_SIZE_MB} MB")
        uploaded_files = st.file_uploader("Dateien auswählen (.xlsx, .csv, .pdf)", type=["xlsx", "csv", "pdf"], accept_multiple_files=True)
        
        if uploaded_files:
            if st.button("🚀 Hochladen & in MongoDB speichern", type="primary"):
                alle_daten = []
                with st.spinner("Lese Dateien und speichere in Datenbank..."):
                    for datei in uploaded_files:
                        try:
                            datei_bytes = datei.getvalue()
                            if len(datei_bytes) > MAX_UPLOAD_SIZE_BYTES:
                                st.error(f"Datei {datei.name} ist größer als {MAX_UPLOAD_SIZE_MB} MB und wurde übersprungen.")
                                continue

                            df_teil, format_name = lade_und_uebersetze_cached(datei.name, datei_bytes)
                            if not df_teil.empty:
                                alle_daten.append(df_teil)
                        except Exception as e:
                            st.error(f"Fehler bei {datei.name}: {e}")
                
                    if alle_daten:
                        df_upload = pd.concat(alle_daten, ignore_index=True)
                        df_upload['createdAt'] = datetime.now(timezone.utc)
                        records = df_upload.to_dict('records')
                        
                        if records:
                            collection.insert_many(records)
                            st.success(f"✅ Super! {len(records)} Raten-Zeilen wurden erfolgreich in die Datenbank geschrieben. Sie werden in 6 Monaten automatisch gelöscht.")
                            st.balloons()
        
        # --- GEFAHRENZONE (DATENBANK LEEREN) ---
        st.markdown("---")
        st.write("### 🚨 Gefahrenzone")
        st.error("Achtung: Der folgende Button löscht **alle** gespeicherten Raten unwiderruflich aus der Datenbank.")

        delete_confirm = st.checkbox("Ich bestätige, dass ich alle Raten endgültig löschen will.", key="delete_confirm_checkbox")
        delete_text = st.text_input("Zur Bestätigung exakt `DELETE ALL` eingeben:", key="delete_confirm_text")
        delete_allowed = delete_confirm and delete_text.strip() == "DELETE ALL"
        
        if st.button("🗑️ Ganze Datenbank leeren (Alle Raten löschen)", disabled=not delete_allowed):
            ergebnis_all = collection.delete_many({})
            st.success(f"✅ Datenbank erfolgreich geleert! Es wurden {ergebnis_all.deleted_count} alte Einträge gelöscht.")
    else:
        st.info("Upload und Löschfunktionen sind gesperrt. Bitte als Admin anmelden.")
