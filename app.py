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
from typing import List
from pydantic import BaseModel, Field
from google import genai
from google.genai import types
from rapidfuzz import process as fuzz_process


# --- PYDANTIC-MODELLE FÜR STRUKTURIERTE EXTRAKTION ---
class Surcharge(BaseModel):
    code: str = Field(description="Abkürzung des Zuschlags (z.B. THC, BAF, PSS, ETS)")
    amount: float = Field(description="Betrag des Zuschlags als Zahl")
    currency: str = Field(description="Währung (USD oder EUR)")


class FreightRate(BaseModel):
    carrier: str = Field(description="Name der Reederei, die das Angebot erstellt hat (z.B. MSC, Hapag-Lloyd, Maersk, etc.)")
    contract_number: str = Field(description="Vertragsnummer, z.B. R45925010000228")
    port_of_loading: str = Field(description="Ladehafen (POL). Darf nur EIN Hafen sein.")
    port_of_destination: str = Field(description="Zielhafen (POD). Darf nur EIN Hafen sein.")
    valid_from: str = Field(description="Gültig ab, Format DD.MM.YYYY")
    valid_to: str = Field(description="Gültig bis, Format DD.MM.YYYY")
    rate_40hc: float = Field(description="Basisfrachtrate für 40'DV/HC")
    currency: str = Field(description="Währung der Basisrate, meist USD")
    prepaid_surcharges: List[Surcharge] = Field(description="Zuschläge am Origin und Seefracht-Zuschläge (Prepaid)")
    collect_surcharges: List[Surcharge] = Field(description="Zuschläge am Zielort (Destination / Collect)")
    remark: str = Field(description="Bemerkungen oder Besonderheiten")


class ExtractionResponse(BaseModel):
    rates: List[FreightRate]


# ---------------------------------------------------------------------------
# ZENTRALES ALIAS-DICTIONARY
# ---------------------------------------------------------------------------
COLUMN_ALIASES: dict = {
    "Carrier": [
        "Carrier", "Reederei", "Shipping Line", "Vessel Operator",
        "Shipping Company",
    ],
    "Contract Number": [
        "Contract Number", "Contract number", "Contract No", "Contract Nr",
        "Contract", "Contract Reference", "Contract Filing Reference",
        "Quote Number", "Quote No", "Reference Number", "Ref No",
        "Agreement Number", "Rate Agreement",
    ],
    "Port of Loading": [
        "Port of Loading", "POL", "Port of Load", "Origin Port",
        "Origin", "Load Port", "Loading Port", "POL Name",
        "Departure Port", "From Port", "Pol",
        "Receipt", "Place of Receipt",
    ],
    "Port of Destination": [
        "Port of Destination", "POD", "Destination Port", "Destination",
        "Discharge Port", "Port of Discharge", "Dest Port", "POD Name",
        "Arrival Port", "To Port", "Delivery Port", "Pod",
        "Delivery", "Place of Delivery",
    ],
    "Valid from": [
        "Valid from", "Valid From", "Effective Date", "Start Date",
        "Validity From", "Rate Valid From", "From Date", "Date From",
        "Commencement Date", "Start",
    ],
    "Valid to": [
        "Valid to", "Valid To", "Expiry Date", "Expiration Date",
        "End Date", "Validity To", "Rate Valid To", "To Date", "Date To",
        "Not Beyond", "Expiry", "End", "Valid until", "Valid Until",
    ],
    "40HC": [
        "40HC", "40HC All In", "40HC All In.1", "40HDRY",
        "40' HC", "40'HC", "40HC Rate", "40 HC", "40DV/HC",
        "40HQ", "40' HQ", "Rate 40HC", "Rate 40HQ", "40H",
        "40 DRY", "40DRY", "O/F", "OF", "Ocean Freight", "Base Rate",
        "40'rates", "40'rate", "current rate", "new rate", "Rate",
    ],
    "Currency": [
        "Currency", "Currency.1", "Currency.2", "Currency.3",
        "Currency.4", "Currency.5", "Cur", "Curr", "Rate Currency",
        "CCY", "Ccy",
    ],
    "Included Prepaid Surcharges 40HC": [
        "Included Prepaid Surcharges 40HC", "Prepaid Surcharges",
        "Origin Surcharges", "Prepaid Charges", "Origin Charges",
        "Surcharges Prepaid", "Prepaid",
    ],
    "Included Collect Surcharges 40HC": [
        "Included Collect Surcharges 40HC", "Collect Surcharges",
        "Destination Surcharges", "Collect Charges", "Destination Charges",
        "Surcharges Collect", "Collect",
    ],
    "Remark": [
        "Remark", "Remarks", "Remark / Notes", "Notes", "Note",
        "Comments", "Comment", "Additional Info", "Bemerkung",
    ],
}

FUZZY_SCORE_THRESHOLD = 85


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
    st.sidebar.image("logo_farbig.png", width="stretch")
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
GEMINI_API_KEY = hole_konfiguration("GEMINI_API_KEY")

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
        response = requests.get("https://api.frankfurter.app/latest?from=USD&to=EUR", timeout=2)
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
    waehrung_match = re.search(r"\b([A-Z]{3})\b", text_str, re.IGNORECASE)
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


def extrahiere_pol_tokens(pol_text):
    normalisiert = normalisiere_pol_text(pol_text)
    if normalisiert == "Unbekannt":
        return set()
    teile = [t.strip().lower() for t in re.split(r"[/,]", normalisiert) if t.strip()]
    return set(teile)


_SYSTEM_PROMPT = """
Du bist ein Experte für die Analyse von Seefrachtdokumenten.
Extrahiere alle Frachtraten aus dem folgenden Reederei-Angebot (z.B. MSC, Hapag-Lloyd, Maersk etc.) strukturiert.

Regeln:
1. Ein FreightRate-Objekt pro POL-POD-Kombination: Jede einzigartige Kombination aus
   Port of Loading (POL) und Port of Discharge (POD) ergibt ein EIGENES FreightRate-Objekt.
   Beispiel: 4 POLs × 2 PODs = 8 FreightRate-Objekte.
   WICHTIG: Wenn das Dokument mehrere PODs auflistet (z.B. Jeddah und Aqaba), müssen für
   JEDEN POD separate FreightRate-Objekte erstellt werden – auch wenn der POL derselbe ist.
   WICHTIG: Wenn das Dokument mehrere POLs auflistet (z.B. "Hamburg, Bremerhaven, Antwerp"),
   muss für JEDEN POL ein separates FreightRate-Objekt erstellt werden.
2. Surcharges filtern: Priorisiere Zuschläge für 40' Container. Ignoriere Zuschläge nur für 20'.
   Wenn eine Surcharge "per Container" ist, verwende den Betrag direkt.
   Wenn eine Surcharge "per TEU" ist, verdopple den Betrag für 40' (1 TEU × 2 = 40').
3. Monatswerte: Wenn es für einen Zuschlag (z. B. ETS, BAF) mehrere Beträge für verschiedene
   Monate gibt, nimm immer den neuesten/aktuellsten Wert.
4. Collect vs. Prepaid: Destination Charges und als 'Collect' markierte Gebühren kommen in
   collect_surcharges. ALLE ANDEREN Zuschläge kommen in prepaid_surcharges, insbesondere:
   - ERC / Logistic Fee → PREPAID
   - EFS / Emergency Fuel Surcharge → PREPAID
   - FTS / Flexi Tank Surcharge → PREPAID
   - THC / Terminal Handling → PREPAID (wenn am Origin)
   - BRQ / Bunker Recovery → PREPAID
   - PRS / Piracy Risk → PREPAID
   - ISPS / Security → PREPAID
   Zuschläge mit "not subject to" oder ohne Betrag ignorieren.
5. Zuschläge zu Raten zuordnen: Die Zuschläge aus der Surcharge-Tabelle gelten für ALLE
   Frachtraten im gleichen Dokument/Sheet. Jedes FreightRate-Objekt bekommt dieselben
   prepaid_surcharges.
6. Datumsformat: Immer DD.MM.YYYY.
7. Gib ausschließlich valides JSON zurück, das dem vorgegebenen Schema entspricht.
8. Raten mit "upon request" oder "on request" statt einem Preis ignorieren.
"""


def extrahiere_msc_quote_pdf_daten(text, monatswert_modus="neu"):
    """Extrahiert MSC-Quote-Daten per Google Gemini Structured Outputs (Pydantic)."""
    if not GEMINI_API_KEY:
        st.warning("GEMINI_API_KEY ist nicht gesetzt – PDF-Extraktion via LLM nicht verfügbar.")
        return None

    if not str(text).strip():
        return None

    _GEMINI_MODELS = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-flash-lite-latest"]
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        safe_text = str(text)[:15000]
        contents = _SYSTEM_PROMPT + "\n\n<pdf_inhalt>\n" + safe_text + "\n</pdf_inhalt>"
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=ExtractionResponse,
            temperature=0.1,
        )
        response = None
        for model_name in _GEMINI_MODELS:
            try:
                response = client.models.generate_content(
                    model=model_name, contents=contents, config=config,
                )
                break  # Modell hat funktioniert
            except Exception as e:
                if "503" in str(e) or "UNAVAILABLE" in str(e):
                    continue
                raise
        if response is None:
            st.warning("Alle Gemini-Modelle sind aktuell überlastet. Bitte versuche es in wenigen Minuten erneut.")
            return None
        result = ExtractionResponse.model_validate_json(response.text)
        if not result.rates:
            return None

        return [
            {
                "Carrier": rate.carrier,
                "Contract Number": rate.contract_number,
                "Port of Loading": rate.port_of_loading,
                "Port of Destination": rate.port_of_destination,
                "Valid from": rate.valid_from,
                "Valid to": rate.valid_to,
                "40HC": rate.rate_40hc,
                "Currency": rate.currency,
                "Included Prepaid Surcharges 40HC": ", ".join(
                    [f"{s.code} = {s.amount:.2f} {s.currency}" for s in rate.prepaid_surcharges]
                ),
                "Included Collect Surcharges 40HC": ", ".join(
                    [f"{s.code} = {s.amount:.2f} {s.currency}" for s in rate.collect_surcharges]
                ),
                "Remark": rate.remark,
            }
            for rate in result.rates
        ]
    except Exception as e:
        st.error(f"Gemini-Extraktion fehlgeschlagen: {e}")
        return None


def extrahiere_excel_mit_gemini(file_bytes, file_name):
    """Liest eine Excel/CSV-Datei ein (alle Sheets), chunked die Daten und extrahiert
    Frachtraten per Gemini Structured Outputs."""
    if not GEMINI_API_KEY:
        return pd.DataFrame(), "GEMINI_API_KEY nicht gesetzt."

    # 1. Einlesen – alle Sheets zusammenführen
    try:
        buf = io.BytesIO(file_bytes)
        if file_name.lower().endswith(".csv"):
            df_raw = pd.read_csv(buf, header=None, low_memory=False)
        else:
            all_sheets = pd.read_excel(buf, sheet_name=None, header=None)
            sheet_parts = []
            for sheet_name, df_sheet in all_sheets.items():
                if df_sheet.empty:
                    continue
                # Sheet-Trennzeile einfügen, damit Gemini den Kontext erkennt
                separator = pd.DataFrame([[f"=== Sheet: {sheet_name} ==="]], columns=[0])
                sheet_parts.append(separator)
                sheet_parts.append(df_sheet)
            if not sheet_parts:
                return pd.DataFrame(), "Datei ist leer."
            df_raw = pd.concat(sheet_parts, ignore_index=True)
    except Exception as e:
        return pd.DataFrame(), f"Fehler beim Einlesen: {e}"

    if df_raw.empty:
        return pd.DataFrame(), "Datei ist leer."

    # 2. Leere Zeilen/Spalten entfernen (Token sparen)
    df_raw = df_raw.dropna(how="all").dropna(axis=1, how="all").reset_index(drop=True)

    # 3. In CSV-String umwandeln
    csv_text = df_raw.to_csv(index=False, header=False)
    zeilen = csv_text.split("\n")

    # 4. Metadaten-Kontext + Chunking
    #    Kleine Dateien (< 500 Zeilen) komplett in einem Request senden,
    #    damit Gemini Raten UND Surcharges zusammen sieht.
    MAX_SINGLE_REQUEST = 500
    CHUNK_SIZE = 80

    if len(zeilen) <= MAX_SINGLE_REQUEST:
        # Alles in einem Request – kein Chunking nötig
        meta_kontext = ""
        chunks = ["\n".join(zeilen)]
    else:
        META_ZEILEN = 20
        meta_kontext = "\n".join(zeilen[:META_ZEILEN])
        daten_zeilen = zeilen[META_ZEILEN:]
        chunks = []
        for start in range(0, len(daten_zeilen), CHUNK_SIZE):
            chunk = "\n".join(daten_zeilen[start : start + CHUNK_SIZE])
            if chunk.strip():
                chunks.append(chunk)
        if not chunks:
            chunks = [meta_kontext]
            meta_kontext = ""

    # 6. Gemini-Client vorbereiten (mit Modell-Fallback bei Überlastung)
    _GEMINI_MODELS = ["gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-flash-lite-latest"]
    client = genai.Client(api_key=GEMINI_API_KEY)
    alle_raten = []
    fortschritt = st.progress(0)
    status = st.empty()

    for idx, chunk in enumerate(chunks):
        status.caption(f"🤖 Gemini analysiert Chunk {idx + 1}/{len(chunks)} …")
        prompt = (
            _SYSTEM_PROMPT
            + f"\n\n--- DATEINAME ---\n{file_name}"
            + "\n\n--- METADATEN / KOPFZEILEN DER DATEI ---\n"
            + meta_kontext
            + "\n\n--- DATENZEILEN (CHUNK) ---\n"
            + chunk
        )

        chunk_success = False
        for model_name in _GEMINI_MODELS:
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=ExtractionResponse,
                        temperature=0.1,
                    ),
                )
                result = ExtractionResponse.model_validate_json(response.text)
                for rate in result.rates:
                    alle_raten.append({
                        "Carrier": rate.carrier,
                        "Contract Number": rate.contract_number,
                        "Port of Loading": rate.port_of_loading,
                        "Port of Destination": rate.port_of_destination,
                        "Valid from": rate.valid_from,
                        "Valid to": rate.valid_to,
                        "40HC": rate.rate_40hc,
                        "Currency": rate.currency,
                        "Included Prepaid Surcharges 40HC": ", ".join(
                            [f"{s.code} = {s.amount:.2f} {s.currency}" for s in rate.prepaid_surcharges]
                        ),
                        "Included Collect Surcharges 40HC": ", ".join(
                            [f"{s.code} = {s.amount:.2f} {s.currency}" for s in rate.collect_surcharges]
                        ),
                        "Remark": rate.remark,
                    })
                chunk_success = True
                break  # Modell hat funktioniert
            except Exception as e:
                if "503" in str(e) or "UNAVAILABLE" in str(e):
                    continue  # Nächstes Modell versuchen
                st.warning(f"Chunk {idx + 1}/{len(chunks)} fehlgeschlagen ({model_name}): {e}")
                break  # Anderer Fehler → nicht weiter versuchen

        if not chunk_success:
            st.warning(f"Chunk {idx + 1}/{len(chunks)}: Alle Modelle überlastet oder fehlgeschlagen.")

        fortschritt.progress(min((idx + 1) / len(chunks), 1.0))

    fortschritt.empty()
    status.empty()

    if not alle_raten:
        return pd.DataFrame(), "Gemini konnte keine Raten extrahieren."

    df = pd.DataFrame(alle_raten)

    # Carrier-Namen normalisieren (Gemini liefert manchmal den vollen Firmennamen)
    _CARRIER_NORMALIZE = {
        'msc': 'MSC', 'hapag': 'Hapag-Lloyd', 'hapag-lloyd': 'Hapag-Lloyd',
        'maersk': 'Maersk', 'one': 'ONE', 'ocean network': 'ONE',
        'cma cgm': 'CMA CGM', 'cma-cgm': 'CMA CGM', 'cosco': 'COSCO',
        'evergreen': 'Evergreen', 'yang ming': 'Yang Ming', 'zim': 'ZIM',
        'hmm': 'HMM', 'hyundai': 'HMM', 'pil': 'PIL', 'wan hai': 'Wan Hai',
        'oocl': 'OOCL',
    }
    def _normalize_carrier(name):
        n_low = str(name).lower()
        for kw, short in _CARRIER_NORMALIZE.items():
            if kw in n_low:
                return short
        return str(name).strip()
    df['Carrier'] = df['Carrier'].apply(_normalize_carrier)

    df["Valid from dt"] = pd.to_datetime(df["Valid from"], dayfirst=True, errors="coerce")
    df["Valid to dt"] = pd.to_datetime(df["Valid to"], dayfirst=True, errors="coerce")
    return df, "Excel/CSV (Gemini-Extraktion)"


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
    if 'b/l' in n:
        return True
    if re.search(r'\b(bl|doc|docs|documentation|bill of lading)\b', n):
        return True
    return False


# --- HILFSFUNKTIONEN FÜR BERECHNUNGEN ---
def berechne_gebuehren(zuschlaege_str):
    if not isinstance(zuschlaege_str, str) or zuschlaege_str.lower() in ['nan', 'none', '']:
        return []
    treffer = re.findall(r"([A-Za-z0-9\s\(\)\-\./,:+'&]+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})", zuschlaege_str)
    liste = []
    for t in treffer:
        try:
            betrag = parse_decimal_wert(t[1])
            if betrag is None:
                continue
            liste.append({"name": t[0].strip().strip(',').strip(), "betrag": betrag, "waehrung": t[2].upper()})
        except (ValueError, IndexError):
            pass
    return liste


def berechne_total_eur_dynamic(row, price_col, prep_surcharge_col, coll_surcharge_col, row_index, include_all_collect=False):
    basis = parse_decimal_wert(row.get(price_col))
    if basis is None:
        return 99999999

    basis_eur = basis * usd_to_eur if str(row.get('Currency', 'USD')).upper() == 'USD' else basis
    summe_gebuehren_eur = 0

    for g in berechne_gebuehren(str(row.get(prep_surcharge_col, ''))):
        if ist_doc_gebuehr(g['name']):
            continue
        summe_gebuehren_eur += (g['betrag'] * usd_to_eur) if g['waehrung'] == 'USD' else g['betrag'] if g['waehrung'] == 'EUR' else 0

    for i, g in enumerate(berechne_gebuehren(str(row.get(coll_surcharge_col, '')))):
        if ist_doc_gebuehr(g['name']):
            continue
        if include_all_collect or st.session_state.get(f"chk_{row_index}_{i}_{g['name']}", False):
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
        if not has_prep:
            st.write("<small>Keine extra Prepaid Gebühren</small>", unsafe_allow_html=True)

    with col_coll:
        st.write("**🏢 Collect (Zielort):**")
        has_coll = False
        for i, g in enumerate(coll_gebuehren):
            if ist_doc_gebuehr(g['name']):
                doc_gebuehren.append(g)
                continue
            has_coll = True
            if st.checkbox(f"{g['name']} ({g['betrag']:.2f} {g['waehrung']})", key=f"chk_{row_index}_{i}_{g['name']}"):
                if g['waehrung'] == 'USD':
                    summe_gebuehren_eur += (g['betrag'] * usd_to_eur)
                elif g['waehrung'] == 'EUR':
                    summe_gebuehren_eur += g['betrag']
                else:
                    fremd_gebuehren.append(f"{g['betrag']:.2f} {g['waehrung']} ({g['name']} - Collect)")
        if not has_coll:
            st.write("<small>Keine Collect Gebühren</small>", unsafe_allow_html=True)

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


RATEN_PROJECTION = {
    '_id': 0, 'Carrier': 1, 'Contract Number': 1, 'Port of Loading': 1, 'Port of Destination': 1,
    'Valid from': 1, 'Valid to': 1, 'Valid from dt': 1, 'Valid to dt': 1, '40HC': 1,
    'Currency': 1, 'Included Prepaid Surcharges 40HC': 1, 'Included Collect Surcharges 40HC': 1, 'Remark': 1,
}

MAX_DB_FETCH = 1200
MAX_RESULT_ANZEIGE = 50
RESULTS_PRO_SEITE = 50
DB_INSERT_BATCH_SIZE = 2500


@st.cache_data(ttl=120)
def lade_raten_aus_db(such_pol="", such_pod="", such_contract="", fetch_limit=MAX_DB_FETCH):
    query = {}
    if str(such_pol).strip():
        query['Port of Loading'] = {'$regex': re.escape(str(such_pol).strip()), '$options': 'i'}
    if str(such_pod).strip():
        query['Port of Destination'] = {'$regex': re.escape(str(such_pod).strip()), '$options': 'i'}
    if str(such_contract).strip():
        query['Contract Number'] = {'$regex': re.escape(str(such_contract).strip()), '$options': 'i'}

    cursor = collection.find(query, RATEN_PROJECTION).limit(int(fetch_limit) + 1)
    rows = list(cursor)
    ist_gekuerzt = len(rows) > int(fetch_limit)
    if ist_gekuerzt:
        rows = rows[:int(fetch_limit)]
    return pd.DataFrame(rows), ist_gekuerzt


def formatiere_datum_fuer_header(value):
    # 1. Fängt leere Werte ab (None, NaN, NaT)
    if pd.isna(value):
        return "?"

    # 2. Wenn es ein gültiges Datetime-Objekt ist
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%d.%m.%Y")

    # 3. Wenn es als Text vorliegt
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return "?"

    # 4. Versuch, den Text in ein Datum umzuwandeln
    parsed = pd.to_datetime(text, dayfirst=True, errors='coerce')

    # 5. Prüfen, ob die Umwandlung erfolgreich war (also nicht NaT ergab)
    if pd.notna(parsed):
        return parsed.strftime("%d.%m.%Y")

    return text


def ermittle_erste_spalte(df, kandidaten):
    """Findet die erste passende Spalte im DataFrame (2-Pass: Exakt -> Fuzzy)."""
    spalten_map = {str(c).strip().lower(): c for c in df.columns}
    df_cols_lower = list(spalten_map.keys())

    # 1. PASS: Schneller exakter Treffer
    for kandidat in kandidaten:
        k_low = str(kandidat).strip().lower()
        if k_low in spalten_map:
            return spalten_map[k_low]

    # 2. PASS: Fuzzy-Suche (nur wenn exakt nichts gefunden wurde)
    for kandidat in kandidaten:
        k_low = str(kandidat).strip().lower()
        result = fuzz_process.extractOne(k_low, df_cols_lower, score_cutoff=85)
        if result is not None:
            return spalten_map[result[0]]

    return None


def ermittle_preisspalte_40hc(df):
    """Ermittelt robust die 40' Preis-Spalte und vermeidet Surcharge-Textspalten."""
    sperrwoerter = {
        "surcharge", "collect", "prepaid", "included", "remark", "comment",
        "charge section", "charge code",
    }

    bevorzugt = [
        "40HC", "40 HC", "40HQ", "40' HC", "40'HC", "40ST", "40DV/HC", "40DRY", "40DC",
    ]
    spalten_map = {str(c).strip().lower(): c for c in df.columns}

    for name in bevorzugt:
        key = name.strip().lower()
        if key in spalten_map:
            return spalten_map[key]

    muster = re.compile(r"(?:^|\b)40\s*(?:hc|hq|st|h|dry|dc)(?:\b|$)|40'?\s*(?:hc|hq)", re.IGNORECASE)
    for col in df.columns:
        c = str(col).strip()
        c_low = c.lower()
        if any(w in c_low for w in sperrwoerter):
            continue
        if muster.search(c_low):
            return col

    return None


def zaehle_bekannte_spalten(zeile_werte: list) -> int:
    """Gibt der Zeile einen Score. So finden wir die ECHTE Tabellen-Kopfzeile."""
    alle_aliases = [alias.lower() for aliases in COLUMN_ALIASES.values() for alias in aliases]
    treffer = 0
    for val in zeile_werte:
        val_str = str(val).strip().lower()
        if not val_str or len(val_str) < 2 or val_str in {"nan", "none"}:
            continue
        if val_str in alle_aliases:
            treffer += 1
        else:
            match = fuzz_process.extractOne(val_str, alle_aliases, score_cutoff=85)
            if match is not None:
                treffer += 1
    return treffer


def zeile_hat_bekannte_spalten(zeile_werte: list, min_treffer: int = 3) -> bool:
    # Wir verlangen jetzt MINDESTENS 3 Treffer, damit keine Datenzeilen aus Versehen als Header gelten!
    return zaehle_bekannte_spalten(zeile_werte) >= min_treffer


def standardisiere_spalten(df):
    """Benennt Spalten im 2-Pass-Verfahren um, damit Fuzzy-Matches keine echten Treffer klauen."""
    rename_map = {}
    bereits_gemappt = set() 
    spalten_map = {str(c).strip().lower(): c for c in df.columns}

    # Strukturelle Spalten, die NIEMALS umbenannt werden dürfen (Maersk-Format braucht diese)
    geschuetzte_namen = {'charge', 'charge code', 'charge type', 'chrg'}
    for c in df.columns:
        if str(c).strip().lower() in geschuetzte_namen:
            bereits_gemappt.add(c)

    # Spalten, die bereits den korrekten Zielnamen tragen, vor Umbenennungen schützen.
    # Ohne diesen Schutz könnte z.B. '40HC' durch den Alias-Token '40hc' in
    # 'Included Prepaid Surcharges 40HC' per Fuzzy-Match (token_set_ratio=100%) überschrieben werden.
    for ziel in COLUMN_ALIASES.keys():
        if ziel in df.columns:
            bereits_gemappt.add(ziel)

    # --- PASS 1: Nur EXAKTE Treffer ---
    for ziel, kandidaten in COLUMN_ALIASES.items():
        if ziel in df.columns:
            continue
        for kandidat in kandidaten:
            k_low = str(kandidat).strip().lower()
            if k_low in spalten_map:
                original = spalten_map[k_low]
                if original not in bereits_gemappt:
                    rename_map[original] = ziel
                    bereits_gemappt.add(original)
                    break

    # --- PASS 2: FUZZY MATCHING für den Rest ---
    for ziel, kandidaten in COLUMN_ALIASES.items():
        if ziel in df.columns or ziel in rename_map.values():
            continue
        verfuegbare_cols = [c for c in df.columns if c not in bereits_gemappt]
        if not verfuegbare_cols:
            break
        spalten_map_avail = {str(c).strip().lower(): c for c in verfuegbare_cols}
        df_cols_avail_lower = list(spalten_map_avail.keys())

        for kandidat in kandidaten:
            k_low = str(kandidat).strip().lower()
            result = fuzz_process.extractOne(k_low, df_cols_avail_lower, score_cutoff=85)
            if result is not None:
                original = spalten_map_avail[result[0]]
                rename_map[original] = ziel
                bereits_gemappt.add(original)
                break

    return df.rename(columns=rename_map)


def parse_datum_standard(value):
    if isinstance(value, (pd.Timestamp, datetime)):
        if pd.isna(value):
            return None
        return value.strftime("%d.%m.%Y")

    if pd.isna(value):
        return None

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return None

    if re.fullmatch(r"\d{8}", text):
        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    elif re.match(r"^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$", text):
        parsed = pd.to_datetime(text, errors="coerce")
    else:
        parsed = pd.to_datetime(text, dayfirst=True, errors="coerce")

    if pd.notna(parsed):
        return parsed.strftime("%d.%m.%Y")
    return text


def parse_timestamp_standard(value):
    text = parse_datum_standard(value)
    if text is None:
        return pd.NaT
    return pd.to_datetime(text, format="%d.%m.%Y", errors="coerce")


def dataframe_mit_header_aus_zeile(df_sheet, header_idx, data_start_offset=1):
    rohe_spalten = df_sheet.iloc[header_idx].astype(str).str.strip().tolist()
    neue_spalten = []
    gesehen = {}
    for spalte in rohe_spalten:
        if spalte in gesehen:
            gesehen[spalte] += 1
            neue_spalten.append(f"{spalte}.{gesehen[spalte]}")
        else:
            gesehen[spalte] = 0
            neue_spalten.append(spalte)

    df_clean = df_sheet.iloc[header_idx + data_start_offset:].reset_index(drop=True).copy()
    df_clean.columns = neue_spalten
    return df_clean.dropna(how="all").reset_index(drop=True)


def extrahiere_codes_aus_liste(text):
    if pd.isna(text):
        return []
    teile = [t.strip() for t in re.split(r"[,;\n]", str(text)) if t and str(t).strip()]
    return list(dict.fromkeys([t for t in teile if t.lower() not in {"nan", "none"}]))


def dedupliziere_eintraege(eintraege):
    return list(dict.fromkeys([str(e).strip() for e in eintraege if str(e).strip()]))


def dedupliziere_surcharge_string(text):
    if not isinstance(text, str) or not text.strip():
        return ""
    teile = [t.strip() for t in text.split(",") if t.strip()]
    return ", ".join(dedupliziere_eintraege(teile))


def extrahiere_mehrfach_pols(pol_text):
    text = str(pol_text or "").strip()
    if not text:
        return []

    if "/" in text:
        teile = [t.strip() for t in text.split("/") if t.strip()]
        return teile if len(teile) > 1 else [text]

    if re.search(r"\band\b", text, flags=re.IGNORECASE):
        teile = [t.strip() for t in re.split(r"\band\b", text, flags=re.IGNORECASE) if t.strip()]
        return teile if len(teile) > 1 else [text]

    komma_teile = [t.strip() for t in text.split(",") if t.strip()]
    if len(komma_teile) >= 3:
        return komma_teile

    return [text]


def expandiere_mehrfach_pol_zeilen(df):
    if 'Port of Loading' not in df.columns or df.empty:
        return df

    neue_zeilen = []
    for _, row in df.iterrows():
        pols = extrahiere_mehrfach_pols(row.get('Port of Loading'))
        if len(pols) <= 1:
            neue_zeilen.append(row.to_dict())
            continue

        for pol in pols:
            neue_row = row.to_dict()
            neue_row['Port of Loading'] = pol
            neue_zeilen.append(neue_row)

    return pd.DataFrame(neue_zeilen)


def extrahiere_hapag_quotation_excel(excel_dict, file_name):
    alle_rows = []

    for sheet_name, df_sheet in excel_dict.items():
        header_idx = None
        for i in range(min(len(df_sheet), 40)):
            row_vals = [str(v).strip().lower() for v in df_sheet.iloc[i].tolist() if str(v).strip()]
            if 'charge type' in row_vals and 'charge code' in row_vals and 'amount' in row_vals:
                header_idx = i
                break

        if header_idx is None:
            continue

        meta_text = " ".join(df_sheet.iloc[:header_idx].fillna("").astype(str).values.flatten())
        quote_match = re.search(r"\bQ\d{4}[A-Z]{3}\d{5,}(?:/\d+)?\b", meta_text)
        meta_dates = re.findall(r"\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?|\d{8}|\d{2}[./-]\d{2}[./-]\d{4}", meta_text)
        valid_from_value = meta_dates[0] if len(meta_dates) >= 1 else None
        valid_to_value = meta_dates[1] if len(meta_dates) >= 2 else None

        df_clean = dataframe_mit_header_aus_zeile(df_sheet, header_idx)

        charge_type_col = ermittle_erste_spalte(df_clean, ['Charge Type'])
        charge_code_col = ermittle_erste_spalte(df_clean, ['Charge Code'])
        container_col = ermittle_erste_spalte(df_clean, ['Container'])
        amount_col = ermittle_erste_spalte(df_clean, ['Amount'])
        currency_col = ermittle_erste_spalte(df_clean, ['Curr.', 'Currency'])
        pol_col = ermittle_erste_spalte(df_clean, ['Port of Loading'])
        pod_col = ermittle_erste_spalte(df_clean, ['Port of Discharge', 'Port of Destination'])
        srv_col = ermittle_erste_spalte(df_clean, ['Srv ID'])
        tt_col = ermittle_erste_spalte(df_clean, ['T/T*'])
        commodity_col = ermittle_erste_spalte(df_clean, ['Commodity'])
        mfr_col = ermittle_erste_spalte(df_clean, ['Marine Fuel Recovery included in Lumpsum'])
        uom_col = ermittle_erste_spalte(df_clean, ['Unit of Measure'])

        if not all([charge_type_col, charge_code_col, container_col, amount_col, currency_col, pol_col, pod_col]):
            continue

        df_40 = df_clean[df_clean[container_col].astype(str).str.contains('40', case=False, na=False)].copy()
        if df_40.empty:
            continue

        group_keys = [pol_col, pod_col]
        if srv_col and srv_col in df_40.columns:
            group_keys.insert(0, srv_col)

        for _, group in df_40.groupby(group_keys, dropna=False):
            charge_types = group[charge_type_col].astype(str).str.strip().str.lower()
            base_rows = group[charge_types.eq('freight rate')]
            if base_rows.empty:
                continue

            base_row = base_rows.iloc[0]
            basis = parse_decimal_wert(base_row.get(amount_col))
            if basis is None or basis <= 0:
                continue

            prepaid = []
            collect = []

            # MFR ist in der Lumpsum enthalten (Not Subject to Charges) → NICHT als Zuschlag
            # stattdessen im Remark vermerken
            mfr_text = str(base_row.get(mfr_col, '')).strip() if mfr_col else ''

            surcharge_rows = group[group.index != base_row.name]
            for _, surcharge_row in surcharge_rows.iterrows():
                code = str(surcharge_row.get(charge_code_col, '')).strip().upper()
                if not code:
                    continue
                amount = parse_decimal_wert(surcharge_row.get(amount_col))
                if amount in (None, 0):
                    continue
                curr = str(surcharge_row.get(currency_col, base_row.get(currency_col, 'USD'))).strip().upper() or 'USD'
                entry = f"{code} = {amount:.2f} {curr}"
                charge_type = str(surcharge_row.get(charge_type_col, '')).strip().lower()
                if 'import surcharge' in charge_type:
                    collect.append(entry)
                else:
                    prepaid.append(entry)

            remark_parts = []
            if mfr_text:
                remark_parts.append(f"MFR inkl. ({mfr_text})")
            if tt_col and pd.notna(base_row.get(tt_col)):
                remark_parts.append(f"Transit Time: {base_row.get(tt_col)}")
            if commodity_col and pd.notna(base_row.get(commodity_col)):
                remark_parts.append(f"Commodity: {base_row.get(commodity_col)}")
            if uom_col and pd.notna(base_row.get(uom_col)) and str(base_row.get(uom_col)).strip().upper() != 'CTR':
                remark_parts.append(f"Unit: {base_row.get(uom_col)}")

            alle_rows.append({
                'Carrier': 'Hapag-Lloyd',
                'Contract Number': quote_match.group(0) if quote_match else 'Unbekannt',
                'Port of Loading': str(base_row.get(pol_col, '')).strip(),
                'Port of Destination': str(base_row.get(pod_col, '')).strip(),
                'Valid from': parse_datum_standard(valid_from_value),
                'Valid to': parse_datum_standard(valid_to_value),
                '40HC': basis,
                'Currency': str(base_row.get(currency_col, 'USD')).strip().upper() or 'USD',
                'Included Prepaid Surcharges 40HC': ", ".join(dedupliziere_eintraege(prepaid)),
                'Included Collect Surcharges 40HC': ", ".join(dedupliziere_eintraege(collect)),
                'Remark': " | ".join(remark_parts),
            })

    return pd.DataFrame(alle_rows) if alle_rows else None


def baue_ccpr_surcharge_lookup(df_sheet):
    header_idx = None
    for i in range(min(len(df_sheet), 20)):
        row_vals = [str(v).strip() for v in df_sheet.iloc[i].tolist() if str(v).strip()]
        if 'CHARGE_TYPE_CODE' in row_vals and 'GROUP_NAME_3' in row_vals:
            header_idx = i
            break

    if header_idx is None:
        return {}

    df_lookup = dataframe_mit_header_aus_zeile(df_sheet, header_idx, data_start_offset=2)
    lookup = {}
    for _, row in df_lookup.iterrows():
        code = str(row.get('CHARGE_TYPE_CODE', '')).strip().upper()
        if not code:
            continue
        amount = parse_decimal_wert(row.get('GROUP_NAME_3'))
        if amount is None:
            continue
        lookup.setdefault(code, []).append({
            'amount': amount,
            'currency': str(row.get('CURRENCY', 'USD')).strip().upper() or 'USD',
            'valid_from': pd.to_datetime(row.get('VALID_FROM'), errors='coerce'),
            'account': str(row.get('ACCOUNT_MC_NAME', '')).strip(),
            'geo_from': str(row.get('INFO_GEO_FROM', '')).strip(),
        })

    for code in lookup:
        lookup[code] = sorted(lookup[code], key=lambda item: (pd.Timestamp.min if pd.isna(item['valid_from']) else item['valid_from']))
    return lookup


def waehle_ccpr_surcharge(code, surcharge_lookup, account_name='', geo_from=''):
    kandidaten = surcharge_lookup.get(code, [])
    if not kandidaten:
        return None

    if account_name:
        account_treffer = [k for k in kandidaten if k.get('account') == account_name]
        if account_treffer:
            kandidaten = account_treffer

    if geo_from:
        geo_treffer = [k for k in kandidaten if not k.get('geo_from') or k.get('geo_from') == geo_from]
        if geo_treffer:
            kandidaten = geo_treffer

    return kandidaten[-1]


def extrahiere_ccpr_excel(excel_dict, file_name):
    if 'Seafreights' not in excel_dict:
        return None

    df_sheet = excel_dict['Seafreights']
    header_idx = None
    for i in range(min(len(df_sheet), 20)):
        row_vals = [str(v).strip() for v in df_sheet.iloc[i].tolist() if str(v).strip()]
        if 'BFR_DESCRIPTION' in row_vals and 'VALID_FROM' in row_vals and 'GROUP_NAME_2' in row_vals:
            header_idx = i
            break

    if header_idx is None:
        return None

    meta_text = " ".join(df_sheet.iloc[:header_idx].fillna("").astype(str).values.flatten())
    contract_match = re.search(r"\b\d{6,}\b", meta_text)
    valid_from_match = re.search(r"CONTRACT VALID FROM\s*(\d{4}-\d{2}-\d{2}|\d{8}|\d{2}[./-]\d{2}[./-]\d{4})", meta_text, re.IGNORECASE)
    valid_to_match = re.search(r"CONTRACT VALID TO\s*(\d{4}-\d{2}-\d{2}|\d{8}|\d{2}[./-]\d{2}[./-]\d{4})", meta_text, re.IGNORECASE)

    df_rates = dataframe_mit_header_aus_zeile(df_sheet, header_idx, data_start_offset=2)
    surcharge_lookup = baue_ccpr_surcharge_lookup(excel_dict.get('Surcharges', pd.DataFrame())) if 'Surcharges' in excel_dict else {}

    rows = []
    collect_codes = {'THD', 'DDF', 'EMF', 'ISF', 'LFD', 'CDC', 'SMD', 'TAD'}

    for _, row in df_rates.iterrows():
        basis = parse_decimal_wert(row.get('GROUP_NAME_2'))
        if basis is None or basis <= 0:
            continue
        applicable_codes = dedupliziere_eintraege(
            extrahiere_codes_aus_liste(row.get('CHG_SUBJECT_TO_CONTRACT'))
            + extrahiere_codes_aus_liste(row.get('CHG_SUBJECT_TO_TARIFF'))
        )
        not_subject_codes = set(extrahiere_codes_aus_liste(row.get('CHG_NOT_SUBJECT_TO')))

        prepaid = []
        collect = []
        account_name = str(row.get('ACCOUNT_MC_NAME', '')).strip()
        geo_from = str(row.get('INFO_GEO_FROM', '')).strip()

        for code in applicable_codes:
            if code in not_subject_codes:
                continue
            surcharge = waehle_ccpr_surcharge(code, surcharge_lookup, account_name=account_name, geo_from=geo_from)
            if surcharge is None or surcharge['amount'] == 0:
                continue
            entry = f"{code} = {surcharge['amount']:.2f} {surcharge['currency']}"
            if code in collect_codes:
                collect.append(entry)
            else:
                prepaid.append(entry)

        contract_no = str(row.get('QUOTATION_NUMBER', '')).strip()
        if not contract_no or contract_no.lower() in {'nan', 'none'}:
            contract_no = contract_match.group(0) if contract_match else 'Unbekannt'
        valid_from = parse_datum_standard(row.get('VALID_FROM')) or (parse_datum_standard(valid_from_match.group(1)) if valid_from_match else None)
        valid_to = parse_datum_standard(row.get('VALID_TO')) or (parse_datum_standard(valid_to_match.group(1)) if valid_to_match else None)

        remark_parts = []
        commodity = str(row.get('COMMODITY_DESCRIPTION', '')).strip()
        if commodity:
            remark_parts.append(f"Commodity: {commodity}")
        remark_parts.append("CMA CGM Tarif-Rabattvertrag")

        end_description = row.get('END_DESCRIPTION', '')
        pod_value = str(end_description).strip() if pd.notna(end_description) else ''
        if not pod_value or pod_value.lower() in {'nan', 'none'}:
            pod_value = str(row.get('BTO_DESCRIPTION', '')).strip()

        rows.append({
            'Carrier': 'CMA CGM',
            'Contract Number': contract_no,
            'Port of Loading': str(row.get('BFR_DESCRIPTION', '')).strip(),
            'Port of Destination': pod_value,
            'Valid from': valid_from,
            'Valid to': valid_to,
            '40HC': basis,
            'Currency': str(row.get('CURRENCY', 'USD')).strip().upper() or 'USD',
            'Included Prepaid Surcharges 40HC': ", ".join(dedupliziere_eintraege(prepaid)),
            'Included Collect Surcharges 40HC': ", ".join(dedupliziere_eintraege(collect)),
            'Remark': " | ".join(remark_parts),
        })

    return pd.DataFrame(rows) if rows else None


def extrahiere_evergreen_excel(excel_dict, file_name):
    rows = []

    for sheet_name, df_sheet in excel_dict.items():
        if not re.match(r'^(SOC\s+)?SQ', str(sheet_name), flags=re.IGNORECASE):
            continue

        header_idx = None
        for i in range(min(len(df_sheet), 15)):
            row_vals = [str(v).strip() for v in df_sheet.iloc[i].tolist() if str(v).strip()]
            if 'POL' in row_vals and 'POD' in row_vals and any(v in row_vals for v in ["40' HC", '40HC']):
                header_idx = i
                break

        if header_idx is None:
            continue

        meta_text = " ".join(df_sheet.iloc[:header_idx].fillna("").astype(str).values.flatten())
        sq_match = re.search(r"SQ NO:\s*([A-Z0-9]+)", meta_text, re.IGNORECASE)
        ref_match = re.search(r"REFERENCE NO:\s*([A-Z0-9]+)", meta_text, re.IGNORECASE)
        validity_match = re.search(r"VALIDITY\s*:\s*(\d{8})\s*-\s*(\d{8})", meta_text, re.IGNORECASE)

        df_rates = dataframe_mit_header_aus_zeile(df_sheet, header_idx)
        rate_col = ermittle_erste_spalte(df_rates, ["40' HC", '40HC'])
        pol_col = ermittle_erste_spalte(df_rates, ['POL'])
        pod_col = ermittle_erste_spalte(df_rates, ['POD'])
        currency_col = ermittle_erste_spalte(df_rates, ['Currency'])
        remark_col = ermittle_erste_spalte(df_rates, ['Remark'])
        manifest_col = ermittle_erste_spalte(df_rates, ['Manifest Items'])
        local_surcharge_col = ermittle_erste_spalte(df_rates, ['Local Surcharges'])

        if not all([rate_col, pol_col, pod_col, currency_col]):
            continue

        for _, row in df_rates.iterrows():
            price = parse_decimal_wert(row.get(rate_col))
            if price is None:
                continue

            remark_parts = []
            for spalte in [remark_col, manifest_col, local_surcharge_col]:
                if spalte and pd.notna(row.get(spalte)):
                    text = str(row.get(spalte)).strip()
                    if text:
                        remark_parts.append(re.sub(r'\s+', ' ', text))

            # Vertragsnummer: ref_match > sq_match > Sheet-Name-Extraktion
            _ref = ref_match.group(1) if ref_match else None
            if _ref and _ref.upper() in ('NONE', 'N/A', ''):
                _ref = None
            _sq = sq_match.group(1) if sq_match else None
            if not _ref and not _sq:
                # Fallback: SQ-Nummer aus Sheet-Name extrahieren (z.B. 'SQK500741 fak' → 'SQK500741')
                _sq_name_match = re.search(r'SQK?\d+', str(sheet_name), re.IGNORECASE)
                _sq = _sq_name_match.group(0) if _sq_name_match else 'Unbekannt'
            _contract_nr = _ref or _sq or 'Unbekannt'

            rows.append({
                'Carrier': 'Evergreen',
                'Contract Number': _contract_nr,
                'Port of Loading': str(row.get(pol_col, '')).strip(),
                'Port of Destination': str(row.get(pod_col, '')).strip(),
                'Valid from': parse_datum_standard(validity_match.group(1)) if validity_match else None,
                'Valid to': parse_datum_standard(validity_match.group(2)) if validity_match else None,
                '40HC': price,
                'Currency': str(row.get(currency_col, 'USD')).strip().upper() or 'USD',
                'Included Prepaid Surcharges 40HC': '',
                'Included Collect Surcharges 40HC': '',
                'Remark': ' | '.join(dedupliziere_eintraege(remark_parts)),
            })

    return pd.DataFrame(rows) if rows else None


def normalisiere_upload_dataframe(df_upload):
    out = df_upload.copy()

    # 1. FILTER: Wirf alle 20-Fuß Container raus (Strenge Suche)
    size_col = None
    for col in out.columns:
        if str(col).strip().lower() in ['ctr', 'size', 'equipment', 'container', 'type']:
            size_col = col
            break
            
    if size_col is not None:
        mask_40 = out[size_col].astype(str).str.contains('40|hc|hq|nan', na=True, case=False)
        out = out[mask_40].copy()

    def stelle_spalte_sicher(ziel, kandidaten, default=""):
        if ziel in out.columns:
            return
        quelle = ermittle_erste_spalte(out, kandidaten)
        if quelle is not None:
            out[ziel] = out[quelle]
        else:
            out[ziel] = default

    # Pflichtfelder befüllen
    stelle_spalte_sicher('Carrier', COLUMN_ALIASES['Carrier'], default='FMS')
    stelle_spalte_sicher('Contract Number', COLUMN_ALIASES['Contract Number'], default='Unbekannt')
    stelle_spalte_sicher('Port of Loading', COLUMN_ALIASES['Port of Loading'], default='Unbekannt')
    stelle_spalte_sicher('Port of Destination', COLUMN_ALIASES['Port of Destination'], default='Unbekannt')
    stelle_spalte_sicher('Valid from', COLUMN_ALIASES['Valid from'], default=None)
    stelle_spalte_sicher('Valid to', COLUMN_ALIASES['Valid to'], default=None)

    if '40HC' not in out.columns:
        preis_col = ermittle_preisspalte_40hc(out)
        if preis_col is None:
            preis_col = ermittle_erste_spalte(out, COLUMN_ALIASES['40HC'])
        out['40HC'] = out[preis_col] if preis_col is not None else None

    # Währungs-Check
    if 'Currency' not in out.columns:
        waehrung_col = ermittle_erste_spalte(out, COLUMN_ALIASES['Currency'])
        out['Currency'] = out[waehrung_col] if waehrung_col is not None else 'USD'
    else:
        out['Currency'] = out['Currency'].fillna('USD')

    # Preis-Konvertierung
    out['40HC'] = out['40HC'].apply(parse_decimal_wert)

    # --- RADIKALE VALIDIERUNG (TRASH-FILTER) ---
    ungueltige = {'UNBEKANNT', 'NAN', 'NONE', '', 'NIL', 'NULL'}
    
    # Regel A: Preis muss vorhanden und größer als 0 sein
    mask_preis = out['40HC'].notna() & (out['40HC'] != 0)
    
    # Regel B: Häfen toleranter prüfen (ab 2 Buchstaben erlaubt für "US", "DE")
    def ist_gueltiger_hafen(val):
        s = str(val).strip()
        if s.upper() in ungueltige or len(s) < 2 or len(s) > 200: return False
        if re.match(r'^-?\d+(?:[.,]\d+)?$', s): return False # Nur Zahlen = kein Hafen
        _s_low = s.lower()
        # Schlüsselwörter, die nie ein Hafenname sind (verhindert Surcharge-Zeilen als Raten)
        _hafen_blacklist = ('potential', 'average', 'surcharge', 'fee', 'charge',
                            'handling', 'fuel', 'security', 'logistic', 'bunker',
                            'piracy', 'tank', 'reefer', 'hazardous', 'overweight')
        if any(kw in _s_low for kw in _hafen_blacklist): return False
        return True

    mask_pol = out['Port of Loading'].apply(ist_gueltiger_hafen)
    mask_pod = out['Port of Destination'].apply(ist_gueltiger_hafen)
    
    out = out[mask_preis & mask_pol & mask_pod].copy()

    # Datums-Konvertierung
    for col in ['Valid from', 'Valid to']:
        target = col + " dt"
        parsed_text = out[col].apply(parse_datum_standard)
        out[col] = parsed_text
        out[target] = parsed_text.apply(parse_timestamp_standard)

    # Fehlende Spalten sicherstellen
    if 'Remark' not in out.columns:
        out['Remark'] = ""
    if 'Included Prepaid Surcharges 40HC' not in out.columns:
        out['Included Prepaid Surcharges 40HC'] = ""
    if 'Included Collect Surcharges 40HC' not in out.columns:
        out['Included Collect Surcharges 40HC'] = ""

    # Bereinigung Strings
    out['Carrier'] = out['Carrier'].replace('Unbekannt', 'FMS').astype(str).str.strip()
    out['Contract Number'] = out['Contract Number'].astype(str).str.strip().replace({'': 'Unbekannt'})
    out['Port of Loading'] = out['Port of Loading'].astype(str).str.strip()
    out['Port of Destination'] = out['Port of Destination'].astype(str).str.strip()
    out['Included Prepaid Surcharges 40HC'] = out['Included Prepaid Surcharges 40HC'].apply(dedupliziere_surcharge_string)
    out['Included Collect Surcharges 40HC'] = out['Included Collect Surcharges 40HC'].apply(dedupliziere_surcharge_string)
    out = expandiere_mehrfach_pol_zeilen(out)

    ziel_spalten = [
        'Carrier', 'Contract Number', 'Port of Loading', 'Port of Destination',
        'Valid from', 'Valid to', 'Valid from dt', 'Valid to dt',
        '40HC', 'Currency', 'Included Prepaid Surcharges 40HC',
        'Included Collect Surcharges 40HC', 'Remark'
    ]
    return out[ziel_spalten]


# --- DATEI READER FÜR DEN ADMIN-UPLOAD ---
def lade_und_uebersetze_cached(file_name, file_bytes, monatswert_modus="neu"):
    datei = io.BytesIO(file_bytes)
    datei.name = file_name

    # === PDF VERARBEITUNG ===
    if datei.name.lower().endswith('.pdf'):
        try:
            reader = PyPDF2.PdfReader(datei)
            text = "\n".join([(page.extract_text() or "") for page in reader.pages])
            if not text.strip():
                return pd.DataFrame(), "Fehler: PDF enthält keinen auslesbaren Text."

            msc_quote_data = extrahiere_msc_quote_pdf_daten(text, monatswert_modus=monatswert_modus)
            if msc_quote_data:
                msc_rows = msc_quote_data if isinstance(msc_quote_data, list) else [msc_quote_data]
                df_pdf = pd.DataFrame(msc_rows)
                if 'Valid from' in df_pdf.columns:
                    df_pdf['Valid from dt'] = pd.to_datetime(df_pdf['Valid from'], dayfirst=True, errors='coerce')
                if 'Valid to' in df_pdf.columns:
                    df_pdf['Valid to dt'] = pd.to_datetime(df_pdf['Valid to'], dayfirst=True, errors='coerce')
                return df_pdf, "PDF (MSC Quote)"

            date_matches = re.findall(r'(\d{2,4}[.\-/]\d{2}[.\-/]\d{2,4})', text)
            v_from = normalisiere_datum_token(date_matches[0]) if len(date_matches) > 0 else "Unbekannt"
            v_to = normalisiere_datum_token(date_matches[1]) if len(date_matches) > 1 else "Unbekannt"
            pol_match = re.search(r'(?:POL|Port of Loading|From)[\s:]{1,3}([A-Za-z\s\.,]+)(?:POD|Port of Discharge|To|Vessel|Voyage|\n)', text, re.IGNORECASE)
            pod_match = re.search(r'(?:POD|Port of Discharge|Destination|To)[\s:]{1,3}([A-Za-z\s\.,]+)(?:Vessel|Voyage|Commodity|Term|\n)', text, re.IGNORECASE)
            contract_match = re.search(r'(?:Contract Filing Reference|Contract|Quote)[\s\S]{1,350}?\b([A-Z]*\d{5,}[A-Z0-9]*)\b', text, re.IGNORECASE)
            _r_match = re.search(r'\b(R\d{12,18})\b', text)
            contract_no = contract_match.group(1) if contract_match else (_r_match.group(1) if _r_match else "Unbekannt")
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
                df_pdf['Valid from dt'] = pd.to_datetime(df_pdf['Valid from'], dayfirst=True, errors='coerce')
            if 'Valid to' in df_pdf.columns:
                df_pdf['Valid to dt'] = pd.to_datetime(df_pdf['Valid to'], dayfirst=True, errors='coerce')

            return df_pdf, "PDF"
        except Exception as e:
            return pd.DataFrame(), f"Fehler: {e}"

    # === EXCEL / CSV VERARBEITUNG ===
    else:
        # Strategie: Heuristik zuerst (schnell & kostenlos),
        # Gemini nur als Fallback bei schlechten Ergebnissen.

        df_raw = pd.DataFrame()
        global_contract = "Unbekannt"

        if datei.name.lower().endswith('.xlsx'):
            datei.seek(0)
            try:
                df_fast = pd.read_excel(datei, sheet_name=0)
                df_fast_std = standardisiere_spalten(df_fast)
                # Maersk-Tender haben eine 'Charge'-Spalte → NICHT den Schnellpfad nehmen,
                # sonst werden Surcharges nicht gruppiert und BAS/Surcharges als separate Raten importiert.
                hat_charge_spalte = any(
                    str(c).strip().lower() in ['charge', 'charge code', 'charge type', 'chrg']
                    for c in df_fast_std.columns
                )
                if not hat_charge_spalte and 'Port of Destination' in df_fast_std.columns and '40HC' in df_fast_std.columns:
                    # FIX: Receipt/Delivery haben Vorrang vor leeren POL/POD-Spalten
                    for _ziel, _vorrang in [('Port of Loading', ['Receipt', 'Place of Receipt']),
                                            ('Port of Destination', ['Delivery', 'Place of Delivery'])]:
                        _vorrang_col = ermittle_erste_spalte(df_fast_std, _vorrang)
                        if _vorrang_col:
                            _hat_daten = not df_fast_std[_vorrang_col].astype(str).str.strip().replace(
                                {'nan': '', 'None': '', 'NaN': ''}
                            ).eq('').all()
                            if _hat_daten:
                                df_fast_std[_ziel] = df_fast_std[_vorrang_col]
                    if 'Contract Number' not in df_fast_std.columns:
                        if fn_match := re.search(r'(?:contract)[\s_0-9-]*?(\d{6,10})', datei.name, re.IGNORECASE):
                            df_fast_std['Contract Number'] = fn_match.group(1)
                    return df_fast_std, "Excel (Schnell)"
            except Exception:
                pass

            # --- Multi-Sheet Verarbeitung ---
            datei.seek(0)
            try:
                excel_dict = pd.read_excel(datei, sheet_name=None, header=None)
            except Exception as e:
                return pd.DataFrame(), f"Fehler beim Lesen der Excel: {e}"

            for parser_funktion, parser_name in [
                (extrahiere_hapag_quotation_excel, 'Excel (Hapag-Quotation)'),
                (extrahiere_ccpr_excel, 'Excel (CCPR-Vertrag)'),
                (extrahiere_evergreen_excel, 'Excel (Evergreen-SQ)'),
            ]:
                try:
                    df_spezial = parser_funktion(excel_dict, datei.name)
                except Exception:
                    df_spezial = None
                if isinstance(df_spezial, pd.DataFrame) and not df_spezial.empty:
                    return df_spezial, parser_name

            alle_sheets_dfs = []

            for sheet_name, df_sheet in excel_dict.items():
                if df_sheet.empty:
                    continue

                # A. Bestimme Header-Zeile durch SCORING (Deep-Scan)
                sheet_header_idx = None
                
                # Gehe alle Zeilen durch (auch tausende), aber überspringe leere Zeilen sofort
                for i in range(len(df_sheet)):
                    row_vals = df_sheet.iloc[i].dropna().astype(str).tolist()
                    if len(row_vals) < 3: 
                        continue # Überspringt Zeilen mit weniger als 3 Werten (spart enorm Zeit)
                        
                    # Strenge Suche (3 Treffer)
                    if zaehle_bekannte_spalten(row_vals) >= 3:
                        sheet_header_idx = i
                        break 
                
                # 2. Versuch: Lockere Suche (2 Treffer), falls nichts gefunden wurde
                if sheet_header_idx is None:
                    for i in range(len(df_sheet)):
                        row_vals = df_sheet.iloc[i].dropna().astype(str).tolist()
                        if len(row_vals) < 2:
                            continue
                        if zaehle_bekannte_spalten(row_vals) >= 2:
                            sheet_header_idx = i
                            break 

                if sheet_header_idx is None:
                    continue 

                # B. Metadaten VOR dem Header aus dem Freitext "scrapen"
                df_top = df_sheet.iloc[:sheet_header_idx]
                sheet_contract = global_contract
                sheet_pol = "Unbekannt"
                sheet_valid_from = None
                sheet_valid_to = None
                sheet_carrier = None
                sheet_currency = None

                _BEKANNTE_CARRIER = {
                    'msc': 'MSC', 'hapag': 'Hapag-Lloyd', 'hapag-lloyd': 'Hapag-Lloyd',
                    'maersk': 'Maersk', 'one': 'ONE', 'ocean network express': 'ONE',
                    'cma cgm': 'CMA CGM', 'cma-cgm': 'CMA CGM', 'cosco': 'COSCO',
                    'evergreen': 'Evergreen', 'yang ming': 'Yang Ming', 'zim': 'ZIM',
                    'hmm': 'HMM', 'hyundai': 'HMM', 'pil': 'PIL', 'wan hai': 'Wan Hai',
                    'oocl': 'OOCL',
                }
                _DATE_PATTERN = re.compile(r'\d{4}[./-]\d{2}[./-]\d{2}|\d{2}[./-]\d{2}[./-]\d{2,4}|\d{8}')

                for i in range(len(df_top)):
                    row_vals = df_top.iloc[i].dropna().astype(str).tolist()
                    full_row_text = " ".join(row_vals).lower()

                    # Carrier-Erkennung aus Metadaten
                    if not sheet_carrier:
                        for kw, carrier_name in _BEKANNTE_CARRIER.items():
                            if re.search(r'\b' + re.escape(kw) + r'\b', full_row_text):
                                sheet_carrier = carrier_name
                                break

                    for j, val in enumerate(row_vals):
                        v_low = val.lower()
                        if any(x in v_low for x in ['contract', 'quote', 'ref.', 'reference', 'sq']):
                            if j + 1 < len(row_vals):
                                sheet_contract = row_vals[j + 1]
                                global_contract = sheet_contract
                        if 'loading' in v_low or v_low.strip() == 'pol' or 'pol name' in v_low:
                            if j + 1 < len(row_vals):
                                sheet_pol = row_vals[j + 1]

                        # Währungs-Erkennung
                        if not sheet_currency and ('currency' in v_low or 'cur' == v_low.strip()):
                            if j + 1 < len(row_vals):
                                _c = row_vals[j + 1].strip().upper()
                                if _c in ('USD', 'EUR', 'GBP', 'CNY'):
                                    sheet_currency = _c

                        # Validity-Erkennung (erweitert)
                        if 'valid' in v_low:
                            # Datums-Suche: Zuerst im aktuellen Feld, dann im nächsten
                            search_texts = [val]
                            if j + 1 < len(row_vals):
                                search_texts.append(row_vals[j + 1])
                            combined = " ".join(search_texts)
                            # Unterstützt auch '~'-getrennte Bereiche (Yang Ming: 2026/04/01~2026/04/30)
                            dates = _DATE_PATTERN.findall(combined)
                            if len(dates) >= 1 and not sheet_valid_from:
                                sheet_valid_from = dates[0]
                            if len(dates) >= 2 and not sheet_valid_to:
                                sheet_valid_to = dates[1]

                # C. Spaltennamen setzen und bereinigen
                rohe = df_sheet.iloc[sheet_header_idx].astype(str).str.strip().tolist()
                neu, gesehen = [], {}
                for s in rohe:
                    if s in gesehen:
                        gesehen[s] += 1
                        neu.append(f"{s}.{gesehen[s]}")
                    else:
                        gesehen[s] = 0
                        neu.append(s)

                df_clean = df_sheet.copy()
                df_clean.columns = neu
                df_clean = df_clean.iloc[sheet_header_idx + 1:].reset_index(drop=True)
                df_clean = standardisiere_spalten(df_clean)

                # C2. Fix für zusammengeführte Spalten: Wenn Port of Destination
                #     nur 2-3 Zeichen hat (Ländercodes wie AE, BH, IQ), liegt der
                #     echte Portname oft in der nächsten 'nan'-Spalte.
                if 'Port of Destination' in df_clean.columns:
                    _pod_vals = df_clean['Port of Destination'].dropna().astype(str).str.strip()
                    _pod_lens = _pod_vals.str.len()
                    if len(_pod_vals) > 0 and (_pod_lens <= 3).mean() > 0.7:
                        _pod_col_idx = df_clean.columns.get_loc('Port of Destination')
                        if _pod_col_idx + 1 < len(df_clean.columns):
                            _next_col = df_clean.columns[_pod_col_idx + 1]
                            _next_vals = df_clean[_next_col].dropna().astype(str).str.strip()
                            _next_lens = _next_vals.str.len()
                            if len(_next_vals) > 0 and (_next_lens > 3).mean() > 0.5:
                                df_clean['Port of Destination'] = df_clean[_next_col]

                # D. Gefundene Metadaten in die Tabelle injizieren
                if 'Contract Number' not in df_clean.columns and sheet_contract != "Unbekannt":
                    df_clean['Contract Number'] = sheet_contract
                if 'Port of Loading' not in df_clean.columns and sheet_pol != "Unbekannt":
                    df_clean['Port of Loading'] = sheet_pol
                if 'Valid from' not in df_clean.columns and sheet_valid_from:
                    df_clean['Valid from'] = sheet_valid_from
                if 'Valid to' not in df_clean.columns and sheet_valid_to:
                    df_clean['Valid to'] = sheet_valid_to
                if 'Carrier' not in df_clean.columns and sheet_carrier:
                    df_clean['Carrier'] = sheet_carrier
                if 'Currency' not in df_clean.columns and sheet_currency:
                    df_clean['Currency'] = sheet_currency

                # E. Sheet-Validierung: Nur anhängen wenn es mindestens eine
                #    Spalte mit numerischen/Preis-Daten gibt (verhindert Müll-Sheets
                #    wie Comments/Freetimes ohne Raten)
                _hat_numerische_daten = False
                for _col in df_clean.columns:
                    _c_low = str(_col).strip().lower()
                    if any(kw in _c_low for kw in ['40', 'rate', 'price', 'amount', 'freight', 'o/f']):
                        _parsed = df_clean[_col].apply(parse_decimal_wert)
                        if (_parsed.notna() & (_parsed > 0)).any():
                            _hat_numerische_daten = True
                            break
                if not _hat_numerische_daten and '40HC' in df_clean.columns:
                    _parsed = df_clean['40HC'].apply(parse_decimal_wert)
                    _hat_numerische_daten = (_parsed.notna() & (_parsed > 0)).any()

                if not _hat_numerische_daten:
                    continue

                alle_sheets_dfs.append(df_clean)

            if not alle_sheets_dfs:
                return pd.DataFrame(), "Keine verwertbaren Header-Zeilen in den Tabs gefunden."

            df_raw = pd.concat(alle_sheets_dfs, ignore_index=True)

        elif datei.name.lower().endswith('.csv'):
            datei.seek(0)
            try:
                df_csv = pd.read_csv(datei, header=None, low_memory=False)
            except Exception as e:
                return pd.DataFrame(), f"Fehler beim Lesen der CSV: {e}"

            header_idx = None
            
            # 1. Versuch: Strenge Suche (Deep-Scan)
            for i in range(len(df_csv)):
                row_vals = df_csv.iloc[i].dropna().astype(str).tolist()
                if len(row_vals) < 3:
                    continue
                if zaehle_bekannte_spalten(row_vals) >= 3:
                    header_idx = i
                    break
                    
            # 2. Versuch: Lockere Suche
            if header_idx is None:
                for i in range(len(df_csv)):
                    row_vals = df_csv.iloc[i].dropna().astype(str).tolist()
                    if len(row_vals) < 2:
                        continue
                    if zaehle_bekannte_spalten(row_vals) >= 2:
                        header_idx = i
                        break

            if header_idx is None:
                return pd.DataFrame(), "Kein gültiger Header in CSV gefunden."

            # Metadaten scrapen
            df_top = df_csv.iloc[:header_idx]
            csv_contract = global_contract
            csv_pol = "Unbekannt"
            csv_valid_from = None
            csv_valid_to = None
            csv_carrier = None
            csv_currency = None

            _BEKANNTE_CARRIER_CSV = {
                'msc': 'MSC', 'hapag': 'Hapag-Lloyd', 'hapag-lloyd': 'Hapag-Lloyd',
                'maersk': 'Maersk', 'one': 'ONE', 'ocean network express': 'ONE',
                'cma cgm': 'CMA CGM', 'cma-cgm': 'CMA CGM', 'cosco': 'COSCO',
                'evergreen': 'Evergreen', 'yang ming': 'Yang Ming', 'zim': 'ZIM',
                'hmm': 'HMM', 'pil': 'PIL', 'wan hai': 'Wan Hai', 'oocl': 'OOCL',
            }
            _DATE_PAT_CSV = re.compile(r'\d{4}[./-]\d{2}[./-]\d{2}|\d{2}[./-]\d{2}[./-]\d{2,4}|\d{8}')

            for i in range(len(df_top)):
                row_vals = df_top.iloc[i].dropna().astype(str).tolist()
                full_row_text = " ".join(row_vals).lower()

                if not csv_carrier:
                    for kw, carrier_name in _BEKANNTE_CARRIER_CSV.items():
                        if re.search(r'\b' + re.escape(kw) + r'\b', full_row_text):
                            csv_carrier = carrier_name
                            break

                for j, val in enumerate(row_vals):
                    v_low = val.lower()
                    if any(x in v_low for x in ['contract', 'quote', 'ref.', 'reference', 'sq']):
                        if j + 1 < len(row_vals):
                            csv_contract = row_vals[j + 1]
                    if 'loading' in v_low or v_low.strip() == 'pol' or 'pol name' in v_low:
                        if j + 1 < len(row_vals):
                            csv_pol = row_vals[j + 1]

                    if not csv_currency and ('currency' in v_low or 'cur' == v_low.strip()):
                        if j + 1 < len(row_vals):
                            _c = row_vals[j + 1].strip().upper()
                            if _c in ('USD', 'EUR', 'GBP', 'CNY'):
                                csv_currency = _c

                    if 'valid' in v_low:
                        search_texts = [val]
                        if j + 1 < len(row_vals):
                            search_texts.append(row_vals[j + 1])
                        combined = " ".join(search_texts)
                        dates = _DATE_PAT_CSV.findall(combined)
                        if len(dates) >= 1 and not csv_valid_from:
                            csv_valid_from = dates[0]
                        if len(dates) >= 2 and not csv_valid_to:
                            csv_valid_to = dates[1]

            rohe_spalten = df_csv.iloc[header_idx].astype(str).str.strip().tolist()
            neue_spalten, gesehen = [], {}
            for s in rohe_spalten:
                if s in gesehen:
                    gesehen[s] += 1
                    neue_spalten.append(f"{s}.{gesehen[s]}")
                else:
                    gesehen[s] = 0
                    neue_spalten.append(s)

            df_csv.columns = neue_spalten
            df_raw = df_csv.iloc[header_idx + 1:].reset_index(drop=True)
            df_raw = standardisiere_spalten(df_raw)

            if 'Contract Number' not in df_raw.columns and csv_contract != "Unbekannt":
                df_raw['Contract Number'] = csv_contract
            if 'Port of Loading' not in df_raw.columns and csv_pol != "Unbekannt":
                df_raw['Port of Loading'] = csv_pol
            if 'Valid from' not in df_raw.columns and csv_valid_from:
                df_raw['Valid from'] = csv_valid_from
            if 'Valid to' not in df_raw.columns and csv_valid_to:
                df_raw['Valid to'] = csv_valid_to
            if 'Carrier' not in df_raw.columns and csv_carrier:
                df_raw['Carrier'] = csv_carrier
            if 'Currency' not in df_raw.columns and csv_currency:
                df_raw['Currency'] = csv_currency

        # === NACHBEREITUNG FÜR BEIDE (EXCEL & CSV) ===
        if df_raw.empty:
            # === GEMINI FALLBACK #1: Heuristik hat keinen Header gefunden ===
            if GEMINI_API_KEY:
                st.info("📡 Heuristik konnte keine Daten extrahieren – versuche Gemini-Extraktion…")
                return extrahiere_excel_mit_gemini(file_bytes, file_name)
            return pd.DataFrame(), "Datei ist leer nach der Verarbeitung."

        # =================================================================
        # FIX 1: DELIVERY/RECEIPT HABEN VORRANG VOR POD/POL
        # Delivery = tatsächliche Enddestination, Receipt = tatsächlicher Abgangsort
        # Muss VOR dem Grouping passieren, damit die Häfen bekannt sind!
        # =================================================================
        for ziel, vorrang in [('Port of Loading', ['Receipt', 'Place of Receipt']), 
                              ('Port of Destination', ['Delivery', 'Place of Delivery'])]:
            vorrang_col = ermittle_erste_spalte(df_raw, vorrang)
            if vorrang_col:
                hat_daten = not df_raw[vorrang_col].astype(str).str.strip().replace({'nan': '', 'None': '', 'NaN': ''}).eq('').all()
                if hat_daten:
                    df_raw[ziel] = df_raw[vorrang_col]
            elif ziel not in df_raw.columns:
                fallback_col = ermittle_erste_spalte(df_raw, ['Origin', 'Dest'] if 'Loading' in ziel else ['Dest', 'Origin'])
                if fallback_col:
                    df_raw[ziel] = df_raw[fallback_col]

        if global_contract == "Unbekannt":
            if fn_match := re.search(r'(?:contract|ext\.\s+sul)[\s_0-9-]*?(\d{6,10})', datei.name, re.IGNORECASE):
                global_contract = fn_match.group(1)

        # --- Maersk-Format Check ---
        charge_col = None
        for preferred in ['charge code', 'charge type', 'chrg', 'charge']:
            for col in df_raw.columns:
                if str(col).strip().lower() == preferred:
                    charge_col = col
                    break
            if charge_col is not None:
                break

        # Wenn 40HC nicht vorhanden ist (z.B. nur 40ST), mappen wir die passende Spalte vorab auf 40HC.
        if '40HC' not in df_raw.columns:
            preis_spalte = ermittle_preisspalte_40hc(df_raw)
            if preis_spalte is not None:
                df_raw['40HC'] = df_raw[preis_spalte]

        ist_maersk_format = False
        charge_desc_col = ermittle_erste_spalte(df_raw, ['Charge description', 'Description'])
        charge_section_col = ermittle_erste_spalte(df_raw, ['Charge section', 'Section'])
        if charge_col is not None and '40HC' in df_raw.columns and 'Port of Loading' in df_raw.columns and 'Port of Destination' in df_raw.columns:
            codes = df_raw[charge_col].astype(str).str.strip().str.upper()
            has_bas_code = codes.isin(['BAS', 'BASIC']).any()
            has_freight_section = False
            if charge_section_col in df_raw.columns:
                has_freight_section = df_raw[charge_section_col].astype(str).str.strip().str.lower().eq('freight').any()
            ist_maersk_format = has_bas_code or has_freight_section

        if ist_maersk_format:
            eff_col = 'Valid from' if 'Valid from' in df_raw.columns else ermittle_erste_spalte(df_raw, COLUMN_ALIASES['Valid from'])
            exp_col = 'Valid to' if 'Valid to' in df_raw.columns else ermittle_erste_spalte(df_raw, COLUMN_ALIASES['Valid to'])
            tt_col = ermittle_erste_spalte(df_raw, ['Transit Time', 'Transit Days', 'TT'])

            # Carrier-Erkennung für charge-basierte Formate (Maersk, CMA CGM, etc.)
            _detected_carrier = 'Maersk'
            _fn_low = datei.name.lower()
            _carrier_hints = {
                'cma': 'CMA CGM', 'mfr': 'CMA CGM',
                'hapag': 'Hapag-Lloyd', 'hlcu': 'Hapag-Lloyd',
                'msc': 'MSC', 'maersk': 'Maersk', 'one': 'ONE',
                'evergreen': 'Evergreen', 'cosco': 'COSCO', 'zim': 'ZIM',
            }
            for _hint, _name in _carrier_hints.items():
                if _hint in _fn_low:
                    _detected_carrier = _name
                    break
            # Fallback: Sales-Contact-Email
            _sales_col = ermittle_erste_spalte(df_raw, ['Sales Contact', 'Sales Rep', 'Contact'])
            if _detected_carrier == 'Maersk' and _sales_col and _sales_col in df_raw.columns:
                _emails = df_raw[_sales_col].dropna().astype(str).head(5)
                for _em in _emails:
                    if 'cma-cgm' in _em.lower() or 'cma.cgm' in _em.lower():
                        _detected_carrier = 'CMA CGM'
                        break
                    elif 'hapag' in _em.lower():
                        _detected_carrier = 'Hapag-Lloyd'
                        break

            # CMA-CGM-Format: Surcharge-Zeilen haben oft NaN bei Valid from/to.
            # Forward-Fill, damit sie zur gleichen Gruppe wie die Fracht-Zeile gehören.
            for _date_col in [eff_col, exp_col]:
                if _date_col and _date_col in df_raw.columns:
                    df_raw[_date_col] = df_raw[_date_col].ffill()

            group_cols = [c for c in ['Port of Loading', 'Port of Destination', eff_col, exp_col] if c and c in df_raw.columns]
            # Zusätzliche Spalten aufnehmen, damit Raten für verschiedene Commodities/Line refs etc. nicht zusammengemischt werden
            for extra_col in ['Commodity Name', 'Service Mode', 'Equipment Type', 'Line ref']:
                if extra_col in df_raw.columns and extra_col not in group_cols:
                    group_cols.append(extra_col)

            standard_rows = []
            for name, group in df_raw.dropna(subset=['40HC']).groupby(group_cols):
                code_series = group[charge_col].astype(str).str.strip().str.upper()
                bas_mask = code_series.isin(['BAS', 'BASIC'])
                if charge_section_col in group.columns:
                    bas_mask = bas_mask | group[charge_section_col].astype(str).str.strip().str.lower().eq('freight')
                if charge_desc_col in group.columns:
                    bas_mask = bas_mask | group[charge_desc_col].astype(str).str.contains(
                        r'rate\s*per\s*container|ocean\s*freight|base\s*rate',
                        flags=re.IGNORECASE,
                        regex=True,
                        na=False,
                    )

                bas_row = group[bas_mask]
                if bas_row.empty: continue
                
                bas_text = str(bas_row['40HC'].values[0]).strip()
                waehrung, basis_betrag = extrahiere_waehrung_und_betrag(bas_text, default_currency='USD')
                if basis_betrag is None or basis_betrag <= 0: continue

                group_dict = dict(zip(group_cols, name if isinstance(name, tuple) else (name,)))
                pol_val = group_dict.get('Port of Loading', 'Unbekannt')
                pod_val = group_dict.get('Port of Destination', 'Unbekannt')
                eff_val = group_dict.get(eff_col, '') if eff_col else ''
                exp_val = group_dict.get(exp_col, '') if exp_col else ''
                commodity_val = group_dict.get('Commodity Name', '')

                # =================================================================
                # FIX 2: ZUSCHLÄGE SAUBER EXTRAHIEREN (Ignoriert 0 USD & Inclusive)
                # =================================================================
                prepaid_surcharges = []
                collect_surcharges = []
                collect_codes = {'CP1', 'CP2', 'VP1', 'THD', 'DTHC', 'DDF', 'DDC', 'THC34', 'LPC51', 'CAR45'}
                surcharge_rows = group[~bas_mask]
                
                for _, r in surcharge_rows.iterrows():
                    code = str(r[charge_col]).strip()
                    val_text = str(r['40HC']).strip()
                    beschreibung_raw = str(r.get(charge_desc_col, '')).strip() if charge_desc_col else ''
                    beschreibung = beschreibung_raw.lower()
                    section = str(r.get(charge_section_col, '')).lower().strip() if charge_section_col else ''
                    
                    # Zeilen-Währung bevorzugen (z.B. THC in INR statt USD)
                    row_currency = waehrung
                    if 'Currency' in r.index and pd.notna(r['Currency']):
                        _rc = str(r['Currency']).strip().upper()
                        if len(_rc) == 3 and _rc.isalpha():
                            row_currency = _rc
                    s_curr, s_amt = extrahiere_waehrung_und_betrag(val_text, default_currency=row_currency)
                    
                    # Nur Beträge > 0 übernehmen 
                    if s_amt is not None and s_amt > 0:
                        # Beschreibungsname statt nur Code (z.B. "Emergency Fuel Surcharge (BAF09)")
                        label = f"{beschreibung_raw} ({code})" if beschreibung_raw and beschreibung_raw.lower() not in ('nan', 'none', '') else code
                        eintrag = f"{label} = {s_amt:.2f} {s_curr}"
                        ist_collect = (
                            code.upper() in collect_codes
                            or 'destination' in beschreibung
                            or 'dthc' in beschreibung
                            or 'local terminal recovery' in beschreibung
                            or section == 'destination'
                        )
                        if ist_collect:
                            collect_surcharges.append(eintrag)
                        else:
                            prepaid_surcharges.append(eintrag)

                remark_parts = []
                if tt_col and tt_col in bas_row.columns:
                    remark_parts.append(f"Transit Time: {bas_row[tt_col].values[0]}")
                if commodity_val:
                    remark_parts.append(f"Commodity: {commodity_val}")

                standard_rows.append({
                    'Carrier': _detected_carrier,
                    'Contract Number': global_contract,
                    'Port of Loading': pol_val,
                    'Port of Destination': pod_val,
                    'Valid from': eff_val,
                    'Valid to': exp_val,
                    '40HC': basis_betrag,
                    'Currency': waehrung,
                    'Included Prepaid Surcharges 40HC': ", ".join(prepaid_surcharges),
                    'Included Collect Surcharges 40HC': ", ".join(collect_surcharges),
                    'Remark': " | ".join(remark_parts),
                })
            
            if standard_rows:
                df_return = pd.DataFrame(standard_rows)
            else:
                if 'Contract Number' not in df_raw.columns or df_raw['Contract Number'].isna().all():
                    df_raw['Contract Number'] = global_contract
                elif global_contract != "Unbekannt":
                    df_raw['Contract Number'] = global_contract
                df_return = df_raw
                
        else:
            if 'Contract Number' not in df_raw.columns or df_raw['Contract Number'].isna().all():
                df_raw['Contract Number'] = global_contract
            elif global_contract != "Unbekannt":
                df_raw['Contract Number'] = global_contract
            df_return = df_raw

        # === GEMINI FALLBACK #2: Heuristik hat Daten, aber alles Trash ===
        if df_return.empty and GEMINI_API_KEY:
            st.info("📡 Heuristische Verarbeitung ergab keine verwertbaren Zeilen – versuche Gemini-Extraktion…")
            return extrahiere_excel_mit_gemini(file_bytes, file_name)

        # === GEMINI FALLBACK #3: Heuristik hat Daten, aber keine Surcharges erkannt ===
        # Bei wenigen Zeilen ODER fehlenden Surcharges lohnt sich Gemini
        if GEMINI_API_KEY and not df_return.empty:
            _prep_col = 'Included Prepaid Surcharges 40HC'
            _hat_surcharges = (
                _prep_col in df_return.columns
                and df_return[_prep_col].astype(str).str.strip().replace({'': None, 'nan': None, 'None': None}).notna().any()
            )
            if not _hat_surcharges:
                st.info("📡 Heuristik hat keine Zuschläge erkannt – versuche Gemini für vollständige Extraktion…")
                df_gemini, methode_gemini = extrahiere_excel_mit_gemini(file_bytes, file_name)
                if not df_gemini.empty:
                    return df_gemini, methode_gemini
                # Gemini hat nichts → Heuristik-Ergebnis behalten

        return df_return, "Excel/CSV (Multi-Sheet)"


def speichere_dataframe_batchweise(df_upload):
    total = len(df_upload)
    if total == 0:
        return 0

    for col in df_upload.columns:
        if pd.api.types.is_datetime64_any_dtype(df_upload[col]):
            df_upload[col] = [
                v.to_pydatetime().replace(tzinfo=timezone.utc) if pd.notna(v) else None
                for v in df_upload[col]
            ]
    df_upload = df_upload.astype(object).where(pd.notna(df_upload), None)

    records = df_upload.to_dict('records')

    fortschritt = st.progress(0)
    status = st.empty()
    gespeichert = 0

    for start_idx in range(0, total, DB_INSERT_BATCH_SIZE):
        end_idx = min(start_idx + DB_INSERT_BATCH_SIZE, total)
        batch_records = records[start_idx:end_idx]
        if not batch_records:
            continue
        try:
            collection.insert_many(batch_records, ordered=False)
            gespeichert += len(batch_records)
        except pymongo.errors.BulkWriteError as bwe:
            inserted = bwe.details.get('nInserted', 0)
            gespeichert += inserted
            st.warning(f"Batch teilweise eingefügt: {inserted}/{len(batch_records)} Zeilen (restliche z.B. Duplikate übersprungen).")
        fortschritt.progress(min(gespeichert / total, 1.0))
        status.caption(f"💾 Speichere in Datenbank: {gespeichert}/{total} Zeilen")

    fortschritt.empty()
    status.empty()
    return gespeichert


# --- TABS FÜR UI ---
tab_suche, tab_upload, tab_analytics = st.tabs(["🔍 Raten suchen", "⚙️ Daten hochladen (Admin)", "📈 Analytics"])

# === TAB 1: SUCHEN ===
with tab_suche:
    st.write("### Suche in der Datenbank")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        such_pol = st.text_input("📍 Ladehafen (POL):", placeholder="z.B. Hamburg")
    with c2:
        such_pod = st.text_input("🏁 Zielhafen (POD):", placeholder="z.B. Hamad")
    with c3:
        such_contract = st.text_input("📄 Contract Nr.:", placeholder="z.B. 299424203")
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

    action_col, info_col = st.columns([1, 2])
    with action_col:
        suche_starten = st.button("🔎 Suche starten", type="primary", width="stretch")
    with info_col:
        st.caption("Schneller Start aktiv: Daten werden erst nach Klick auf 'Suche starten' geladen.")

    if suche_starten:
        st.session_state["suche_gestartet"] = True
        st.session_state["search_page"] = 1

    suchlauf_aktiv = st.session_state.get("suche_gestartet", False)
    if not suchlauf_aktiv:
        st.info("Für schnelleren Seitenstart wurde die Auto-Suche deaktiviert. Bitte auf 'Suche starten' klicken.")

    df = None
    ist_gekuerzt = False
    if suchlauf_aktiv:
        with st.spinner("Lade Raten aus Datenbank..."):
            df, ist_gekuerzt = lade_raten_aus_db(such_pol, such_pod, such_contract, fetch_limit=MAX_DB_FETCH)

    if df is None:
        pass
    elif df.empty:
        if any([such_pol, such_pod, such_contract]):
            st.warning("Keine Raten für diese Suchkriterien gefunden.")
        else:
            st.info("💡 Die Datenbank ist aktuell leer. Bitte lade im Reiter 'Daten hochladen (Admin)' zuerst Raten hoch.")
    else:
        if ist_gekuerzt:
            st.warning(
                f"Es wurden mehr als {MAX_DB_FETCH} Raten gefunden. Aus Performance-Gründen wurden nur die ersten {MAX_DB_FETCH} geladen. "
                "Bitte Suche weiter eingrenzen (POL/POD/Contract)."
            )

        if 'Valid from dt' in df.columns:
            df['Valid from dt'] = pd.to_datetime(df['Valid from dt'], errors='coerce')
        if 'Valid to dt' in df.columns:
            df['Valid to dt'] = pd.to_datetime(df['Valid to dt'], errors='coerce')

        st.caption(f"{len(df)} Raten im geladenen Suchbereich")

        mask = pd.Series([True] * len(df))
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

                anzeige_df = treffer.head(MAX_RESULT_ANZEIGE).reset_index(drop=True)
                seiten_anzahl = (len(anzeige_df) + RESULTS_PRO_SEITE - 1) // RESULTS_PRO_SEITE
                if seiten_anzahl > 1:
                    seite = st.number_input(
                        "Seite",
                        min_value=1,
                        max_value=seiten_anzahl,
                        value=1,
                        step=1,
                        key="search_page",
                    )
                else:
                    seite = 1

                start_idx = (seite - 1) * RESULTS_PRO_SEITE
                end_idx = start_idx + RESULTS_PRO_SEITE
                seiten_df = anzeige_df.iloc[start_idx:end_idx]

                if historische_raten:
                    st.success(f"✅ {len(treffer)} Raten gefunden (inkl. historischer). Zeige Top {len(anzeige_df)} (Seite {seite}/{max(seiten_anzahl, 1)}) an:")
                else:
                    st.success(f"✅ {len(treffer)} gültige Raten gefunden. Zeige Top {len(anzeige_df)} (Seite {seite}/{max(seiten_anzahl, 1)}) an:")

                for _, row in seiten_df.iterrows():
                    is_best = (row['Total_EUR_Sort'] == treffer['Total_EUR_Sort'].iloc[0])
                    valid_from_label = formatiere_datum_fuer_header(row.get('Valid from'))
                    valid_to_label = formatiere_datum_fuer_header(row.get('Valid to'))
                    gueltigkeit_label = f"{valid_from_label} bis {valid_to_label}" if (valid_from_label != "?" or valid_to_label != "?") else "Unbekannt"
                    label = (
                        f"{'🏆 BESTER PREIS | ' if is_best else ''}🚢 {row.get('Carrier')} | 📄 {row.get('Contract Number')} | "
                        f"{row.get('Port of Loading')} ➡️ {row.get('Port of Destination')} | 📅 {gueltigkeit_label}"
                    )

                    with st.expander(label):
                        anzeige_container_daten(row, "40' HC", '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', row.name)
                        if pd.notna(row.get('Remark')) and row.get('Remark') != "":
                            st.info(f"**💡 Bemerkung:** {row['Remark']}")
            else:
                st.warning("Keine gültigen Raten für diese Suche gefunden.")


# === TAB 2: ADMIN UPLOAD & LÖSCHEN ===
with tab_upload:
    is_admin = admin_login_bereich()

    if is_admin:
        st.write("### 📥 Neue Raten-Dateien in die Datenbank importieren")
        st.caption(f"Maximale Dateigröße pro Datei: {MAX_UPLOAD_SIZE_MB} MB")
        monatswert_auswahl = st.radio(
            "Bei mehrfachen Monatswerten in PDF-Zuschlägen verwenden:",
            options=["Neuester Betrag", "Älterer Betrag"],
            index=0,
            horizontal=True,
            help="Gilt nur für MSC Quote PDFs mit mehrfachen Monatswerten (z.B. Feb/März).",
        )
        monatswert_modus = "neu" if monatswert_auswahl == "Neuester Betrag" else "alt"
        uploaded_files = st.file_uploader("Dateien auswählen (.xlsx, .csv, .pdf)", type=["xlsx", "csv", "pdf"], accept_multiple_files=True)

        if uploaded_files:
            if st.button("🚀 Hochladen & in MongoDB speichern", type="primary"):
                alle_daten = []
                with st.spinner("Lese Dateien und speichere in Datenbank..."):
                    file_progress = st.progress(0)
                    file_status = st.empty()

                    for idx, datei in enumerate(uploaded_files, start=1):
                        file_status.caption(f"📄 Lese Datei {idx}/{len(uploaded_files)}: {datei.name}")
                        try:
                            datei_bytes = datei.getvalue()
                            if len(datei_bytes) > MAX_UPLOAD_SIZE_BYTES:
                                st.error(f"Datei {datei.name} ist größer als {MAX_UPLOAD_SIZE_MB} MB und wurde übersprungen.")
                                file_progress.progress(min(idx / len(uploaded_files), 1.0))
                                continue

                            df_teil, format_name = lade_und_uebersetze_cached(
                                datei.name,
                                datei_bytes,
                                monatswert_modus=monatswert_modus,
                            )

                            if not df_teil.empty:
                                alle_daten.append(df_teil)
                            else:
                                st.warning(f"⚠️ {datei.name} übersprungen: {format_name}")

                        except Exception as e:
                            st.error(f"Fehler bei {datei.name}: {e}")

                        file_progress.progress(min(idx / len(uploaded_files), 1.0))

                    file_progress.empty()
                    file_status.empty()

                if alle_daten:
                    try:
                        df_upload = pd.concat(alle_daten, ignore_index=True)

                        with st.expander("🔍 Debug: Rohdaten vor Normalisierung", expanded=True):
                            st.write(f"**Zeilen gesamt:** {len(df_upload)}")
                            ziel_cols = ['Port of Loading', 'Port of Destination', '40HC', 'Valid from', 'Valid to', 'Carrier', 'Contract Number', 'Currency']
                            gefunden = [c for c in ziel_cols if c in df_upload.columns]
                            fehlend = [c for c in ziel_cols if c not in df_upload.columns]
                            st.write(f"**Standard-Spalten gefunden:** {gefunden}")
                            st.write(f"**Standard-Spalten FEHLEN (werden per Fuzzy gesucht):** {fehlend}")
                            st.write(f"**Alle Spalten in der Datei:** {list(df_upload.columns)}")
                            st.dataframe(df_upload.head(5).astype(str))

                        df_norm = df_upload.copy()
                        with st.expander("🔍 Debug: Fuzzy-Matching Ergebnis", expanded=True):
                            for key in ['Port of Loading', 'Port of Destination', '40HC']:
                                if key not in df_norm.columns:
                                    treffer = ermittle_erste_spalte(df_norm, COLUMN_ALIASES[key])
                                    st.write(f"**{key}** → Fuzzy-Treffer: `{treffer}` | Werte: {list(df_norm[treffer].dropna().head(5)) if treffer else 'NICHT GEFUNDEN'}")
                                else:
                                    st.write(f"**{key}** → bereits als Spalte vorhanden | Werte: {list(df_norm[key].dropna().head(5))}")

                        df_upload = normalisiere_upload_dataframe(df_upload)

                        with st.expander("🔍 Debug: Nach Normalisierung", expanded=True):
                            st.write(f"**Zeilen nach Normalisierung:** {len(df_upload)}")
                            if not df_upload.empty:
                                st.dataframe(df_upload[['Port of Loading', 'Port of Destination', '40HC', 'Currency', 'Carrier']].head(10))
                            else:
                                st.write("Alle Zeilen wurden weggefiltert.")

                        df_upload['createdAt'] = datetime.now(timezone.utc)
                        gespeichert = speichere_dataframe_batchweise(df_upload)

                        if gespeichert > 0:
                            lade_raten_aus_db.clear()
                            st.success(f"✅ Super! {gespeichert} Raten-Zeilen wurden erfolgreich in die Datenbank geschrieben. Sie werden in 6 Monaten automatisch gelöscht.")
                            st.balloons()
                        else:
                            st.error("❌ Keine Raten gespeichert! Die Dateien wurden zwar gelesen, enthielten aber keine gültigen Raten (es fehlt POL, POD oder ein Preis).")
                    except Exception as e:
                        st.error(f"Fehler beim Speichern in MongoDB: {e}")
                else:
                    st.error("❌ Keine der hochgeladenen Dateien enthielt verwertbare Daten.")

        st.markdown("---")
        st.write("### 🚨 Gefahrenzone")
        st.error("Achtung: Der folgende Button löscht **alle** gespeicherten Raten unwiderruflich aus der Datenbank.")

        delete_confirm = st.checkbox("Ich bestätige, dass ich alle Raten endgültig löschen will.", key="delete_confirm_checkbox")
        delete_text = st.text_input("Zur Bestätigung exakt `DELETE ALL` eingeben:", key="delete_confirm_text")
        delete_allowed = delete_confirm and delete_text.strip() == "DELETE ALL"

        if st.button("🗑️ Ganze Datenbank leeren (Alle Raten löschen)", disabled=not delete_allowed):
            ergebnis_all = collection.delete_many({})
            lade_raten_aus_db.clear()
            st.success(f"✅ Datenbank erfolgreich geleert! Es wurden {ergebnis_all.deleted_count} alte Einträge gelöscht.")
    else:
        st.info("Upload und Löschfunktionen sind gesperrt. Bitte als Admin anmelden.")


# === TAB 3: ANALYTICS ===
with tab_analytics:
    st.write("### 📈 Preisentwicklung einer Route")
    col_a, col_b = st.columns(2)
    with col_a:
        analytics_pol = st.text_input("Port of Loading (POL)", placeholder="z.B. Hamburg", key="analytics_pol")
    with col_b:
        analytics_pod = st.text_input("Port of Destination (POD)", placeholder="z.B. Jeddah", key="analytics_pod")

    if st.button("📊 Trend analysieren", type="primary"):
        if not analytics_pol.strip() or not analytics_pod.strip():
            st.warning("Bitte POL und POD eingeben.")
        else:
            df_trend, _ = lade_raten_aus_db(such_pol=analytics_pol.strip(), such_pod=analytics_pod.strip())
            if df_trend.empty:
                st.warning(f"Keine Daten gefunden für {analytics_pol} → {analytics_pod}.")
            else:
                df_trend['Valid from dt'] = pd.to_datetime(df_trend.get('Valid from dt', df_trend.get('Valid from')), dayfirst=True, errors='coerce')
                df_trend = df_trend.dropna(subset=['Valid from dt', '40HC'])
                if df_trend.empty:
                    st.warning("Keine verwertbaren Datensätze (fehlende Datum- oder Preisangaben).")
                else:
                    df_trend['All-In EUR'] = df_trend.apply(
                        lambda r: berechne_total_eur_dynamic(r, '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', r.name, include_all_collect=True),
                        axis=1
                    )
                    df_trend = df_trend.sort_values('Valid from dt')

                    st.markdown("---")
                    kpi1, kpi2, kpi3 = st.columns(3)
                    min_preis = df_trend['All-In EUR'].min()
                    avg_preis = df_trend['All-In EUR'].mean()

                    kpi1.metric("🟢 Historischer Bestpreis", f"{min_preis:.2f} €")
                    kpi2.metric("🟡 Durchschnittspreis", f"{avg_preis:.2f} €")
                    kpi3.metric("📋 Analysierte Raten", f"{len(df_trend)}")

                    # --- TOP 3 RATEN TABELLE ---
                    st.write("#### 🏆 Die 3 historisch günstigsten Raten")
                    top_3 = df_trend.sort_values('All-In EUR').head(3)

                    st.dataframe(
                        top_3[['Carrier', 'Contract Number', 'Valid from', 'Valid to', 'All-In EUR']],
                        width="stretch",
                        hide_index=True
                    )