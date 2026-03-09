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
# Hier alle bekannten Spaltennamen der Reedereien hinterlegen.
# Key   = interner Standardname  |  Value = Liste aller bekannten Variationen
# Um neue Reederei-Spalten zu unterstützen, einfach den passenden Key erweitern.
# ---------------------------------------------------------------------------
COLUMN_ALIASES: dict = {
    "Carrier": [
        "Carrier", "Reederei", "Shipping Line", "Vessel Operator",
        "Line", "Shipping Company",
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
    ],
    "Port of Destination": [
        "Port of Destination", "POD", "Destination Port", "Destination",
        "Discharge Port", "Port of Discharge", "Dest Port", "POD Name",
        "Arrival Port", "To Port", "Delivery Port", "Pod",
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

# Mindest-Score (0-100) für Fuzzy-Treffer – Werte unter diesem Schwellwert werden ignoriert
FUZZY_SCORE_THRESHOLD = 80


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
2. Surcharges filtern: Priorisiere Zuschläge für 40' Container. Ignoriere Zuschläge nur für 20'.
3. Monatswerte: Wenn es für einen Zuschlag (z. B. ETS, BAF) mehrere Beträge für verschiedene
   Monate gibt, nimm immer den neuesten/aktuellsten Wert.
4. Collect vs. Prepaid: Destination Charges und als 'Collect' markierte Gebühren kommen in
   collect_surcharges. Origin- und Seefracht-Zuschläge kommen in prepaid_surcharges.
5. Datumsformat: Immer DD.MM.YYYY.
6. Gib ausschließlich valides JSON zurück, das dem vorgegebenen Schema entspricht.
"""


def extrahiere_msc_quote_pdf_daten(text, monatswert_modus="neu"):
    """Extrahiert MSC-Quote-Daten per Google Gemini Structured Outputs (Pydantic).
    Fällt auf None zurück, wenn kein API-Key gesetzt ist oder die Extraktion fehlschlägt.
    """
    if not GEMINI_API_KEY:
        st.warning("GEMINI_API_KEY ist nicht gesetzt – PDF-Extraktion via LLM nicht verfügbar.")
        return None

    if not str(text).strip():
        return None

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=_SYSTEM_PROMPT + "\n\nPDF TEXT:\n" + str(text),
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=ExtractionResponse,
                temperature=0.1,
            ),
        )
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


def _UNUSED_extrahiere_msc_quote_pdf_daten_legacy(text, monatswert_modus="neu"):
    """Alte Regex-basierte Implementierung – nur als Fallback-Referenz behalten."""
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in str(text).splitlines() if ln and ln.strip()]
    if not lines:
        return None

    modus = str(monatswert_modus).strip().lower()
    bevorzuge_neu = modus not in {"alt", "aelter", "aeltere", "old", "older", "first"}

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
    origin_thc_eintraege = []

    origin_start = next((i for i, line in enumerate(lines) if "surcharges related to origin" in line.lower()), None)
    if origin_start is not None:
        origin_end = len(lines)
        for i in range(origin_start + 1, len(lines)):
            if sea_start is not None and i >= sea_start:
                origin_end = i
                break
            if re.match(r"^\d+\.\s", lines[i]):
                origin_end = i
                break

        for line in lines[origin_start + 1:origin_end]:
            low = line.lower()
            if low.startswith("code ") or "surcharge name amount" in low:
                continue
            if not re.match(r"^THC\b", line, re.IGNORECASE):
                continue

            betrag_match = re.search(r"(\d[\d\.,]*)\s*(USD|EUR)\b", line, re.IGNORECASE)
            if not betrag_match:
                continue

            betrag = parse_decimal_wert(betrag_match.group(1))
            waehrung = betrag_match.group(2).upper()
            if betrag is None:
                continue

            ort_match = re.search(r"-\s*(.+?)\s+\d[\d\.,]*\s*(USD|EUR)\b", line, re.IGNORECASE)
            ort_roh = ort_match.group(1).strip() if ort_match else ""
            ort_norm = normalisiere_pol_text(ort_roh) if ort_roh else ""

            origin_thc_eintraege.append(
                {
                    "amount": float(betrag),
                    "waehrung": waehrung,
                    "tokens": extrahiere_pol_tokens(ort_norm),
                    "seq": len(origin_thc_eintraege),
                }
            )

    if sea_start is not None:
        sea_end = len(lines)
        for i in range(sea_start + 1, len(lines)):
            if re.match(r"^\d+\.\s", lines[i]):
                sea_end = i
                break

        code_kandidaten = {}
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
            code_kandidaten.setdefault(code, []).append(kandidat)

        gewaehlte_codes = []
        for code, kandidaten in code_kandidaten.items():
            if not kandidaten:
                continue

            if bevorzuge_neu:
                best = max(kandidaten, key=lambda c: (c["priority"], c["seq"]))
                vergleich = [
                    c for c in kandidaten
                    if c["priority"] == best["priority"] and c["seq"] < best["seq"]
                ]
                nebenwert = max(vergleich, key=lambda c: c["seq"]) if vergleich else None
            else:
                best = min(kandidaten, key=lambda c: (-c["priority"], c["seq"]))
                vergleich = [
                    c for c in kandidaten
                    if c["priority"] == best["priority"] and c["seq"] > best["seq"]
                ]
                nebenwert = min(vergleich, key=lambda c: c["seq"]) if vergleich else None

            code_label = code
            if nebenwert and (
                nebenwert["amount"] != best["amount"]
                or nebenwert["waehrung"] != best["waehrung"]
            ):
                marker = "ALT" if bevorzuge_neu else "NEU"
                code_label = f"{code} ({marker} {nebenwert['amount']:.2f} {nebenwert['waehrung']})"

            gewaehlte_codes.append(
                {
                    "seq": best["seq"],
                    "label": code_label,
                    "amount": best["amount"],
                    "waehrung": best["waehrung"],
                }
            )

        for item in sorted(gewaehlte_codes, key=lambda x: x["seq"]):
            fuege_gebuehr_hinzu(prepaid_liste, item["label"], item["amount"], item["waehrung"])

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

    def baue_prepaid_string(via_pol_text):
        route_prepaid = list(prepaid_liste)
        if not any(str(code).upper().startswith("THC") for code, _, _ in route_prepaid):
            route_tokens = extrahiere_pol_tokens(via_pol_text)
            passende_thc = []

            for eintrag in origin_thc_eintraege:
                tokens = eintrag["tokens"]
                if not route_tokens:
                    passende_thc.append(eintrag)
                    continue
                # THC im Origin-Block kann Hafen-Alternativen enthalten (z.B. HAM/BRV).
                if tokens and route_tokens & tokens:
                    passende_thc.append(eintrag)

            if passende_thc:
                best = max(passende_thc, key=lambda t: (t["amount"], -t["seq"]))
                vergleich = [t for t in passende_thc if t is not best]
                nebenwert = max(vergleich, key=lambda t: (t["amount"], -t["seq"])) if vergleich else None

                code_label = "THC"
                if nebenwert and (
                    nebenwert["amount"] != best["amount"]
                    or nebenwert["waehrung"] != best["waehrung"]
                ):
                    code_label = f"THC (ALT {nebenwert['amount']:.2f} {nebenwert['waehrung']})"

                route_prepaid.append((code_label, best["amount"], best["waehrung"]))

        return ", ".join([f"{c} = {a:.2f} {w}" for c, a, w in route_prepaid])

    prepaid_str = baue_prepaid_string(pol_str)
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

            # Multi-POL Zeilen (z.B. ANR/RTM) werden aufgeteilt, damit THC pro Hafen korrekt ist.
            via_pol_einzeln = [p.strip() for p in via_pol.split('/') if p.strip()]
            if not via_pol_einzeln:
                via_pol_einzeln = [via_pol]

            for einzel_pol in via_pol_einzeln:
                route_rows.append({
                    'Carrier': 'MSC (aus PDF)',
                    'Contract Number': contract_no,
                    'Port of Loading': einzel_pol,
                    'Port of Destination': current_pod,
                    'Valid from': v_from,
                    'Valid to': v_to,
                    '40HC': rate_value,
                    'Currency': rate_currency,
                    'Included Prepaid Surcharges 40HC': baue_prepaid_string(einzel_pol),
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
    treffer = re.findall(r"([A-Za-z0-9\s\(\)\-\./,:+'&]+?)\s*=\s*([\d,\.]+)\s*([A-Za-z]{3})", zuschlaege_str)
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
                df_pdf['Valid from dt'] = pd.to_datetime(df_pdf['Valid from'], dayfirst=True, errors='coerce')
            if 'Valid to' in df_pdf.columns:
                df_pdf['Valid to dt'] = pd.to_datetime(df_pdf['Valid to'], dayfirst=True, errors='coerce')

            return df_pdf, "PDF"
        except Exception as e: return pd.DataFrame(), f"Fehler: {e}"

    # === EXCEL / CSV VERARBEITUNG ===
    else:
        df_raw = pd.DataFrame()
        global_contract = "Unbekannt"

        if datei.name.lower().endswith('.xlsx'):
            # --- 1. Schneller Pfad versuchen ---
            datei.seek(0)
            try:
                df_fast = pd.read_excel(datei, sheet_name=0)
                df_fast_std = standardisiere_spalten(df_fast)
                if 'Port of Destination' in df_fast_std.columns and '40HC' in df_fast_std.columns:
                    if 'Contract Number' not in df_fast_std.columns:
                        if fn_match := re.search(r'(?:contract)[\s_0-9-]*?(\d{6,10})', datei.name, re.IGNORECASE):
                            df_fast_std['Contract Number'] = fn_match.group(1)
                    return df_fast_std, "Excel (Schnell)"
            except Exception:
                pass

            # --- 2. Multi-Sheet Verarbeitung (Tiefen-Scan) ---
            datei.seek(0)
            try:
                excel_dict = pd.read_excel(datei, sheet_name=None, header=None)
            except Exception as e:
                return pd.DataFrame(), f"Fehler beim Lesen der Excel: {e}"

            alle_sheets_dfs = []

            for sheet_name, df_sheet in excel_dict.items():
                if df_sheet.empty:
                    continue

                # A. Contract Number in den ersten 60 Zeilen suchen (nur wenn noch nicht gefunden)
                if global_contract == "Unbekannt":
                    for i in range(min(60, len(df_sheet))):
                        row_vals = df_sheet.iloc[i].dropna().astype(str).tolist()
                        for j, val in enumerate(row_vals):
                            if 'contract' in val.lower() or 'quote' in val.lower():
                                nums = re.findall(r'\b\d{6,10}\b', val)
                                if nums:
                                    global_contract = nums[0]
                                    break
                                for k in range(1, 4):
                                    if j + k < len(row_vals):
                                        t = re.findall(r'\b\d{6,10}\b', row_vals[j + k])
                                        if t:
                                            global_contract = t[0]
                                            break
                            if global_contract != "Unbekannt":
                                break
                        if global_contract != "Unbekannt":
                            break

                # B. Header-Zeile suchen
                sheet_header_idx = None
                for i in range(min(60, len(df_sheet))):
                    if zeile_hat_bekannte_spalten(df_sheet.iloc[i].dropna().astype(str).tolist(), min_treffer=2):
                        sheet_header_idx = i
                        break

                # Wenn kein Header gefunden wurde, ignoriere dieses Sheet
                if sheet_header_idx is None:
                    continue

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

                # D. Spalten umbenennen
                df_clean = standardisiere_spalten(df_clean)

                # E. Nur anhängen, wenn 40HC-Spalte vorhanden
                if '40HC' in df_clean.columns:
                    alle_sheets_dfs.append(df_clean)

            if not alle_sheets_dfs:
                return pd.DataFrame(), "Keine verwertbaren Raten (40HC) in den Tabs gefunden."

            df_raw = pd.concat(alle_sheets_dfs, ignore_index=True)

        elif datei.name.lower().endswith('.csv'):
            # --- CSV Verarbeitung ---
            datei.seek(0)
            try:
                df_csv = pd.read_csv(datei, header=None, low_memory=False)
            except Exception as e:
                return pd.DataFrame(), f"Fehler beim Lesen der CSV: {e}"

            header_idx = None
            for i in range(min(60, len(df_csv))):
                if zeile_hat_bekannte_spalten(df_csv.iloc[i].dropna().astype(str).tolist(), min_treffer=2):
                    header_idx = i
                    break

            if header_idx is None:
                return pd.DataFrame(), "Kein gültiger Header in CSV gefunden."

            # Contract Number suchen
            for i in range(min(60, len(df_csv))):
                row_vals = df_csv.iloc[i].dropna().astype(str).tolist()
                for j, val in enumerate(row_vals):
                    if 'contract' in val.lower():
                        nums = re.findall(r'\b\d{6,10}\b', val)
                        if nums:
                            global_contract = nums[0]
                            break
                        for k in range(1, 4):
                            if j + k < len(row_vals):
                                t = re.findall(r'\b\d{6,10}\b', row_vals[j + k])
                                if t:
                                    global_contract = t[0]
                                    break
                    if global_contract != "Unbekannt":
                        break
                if global_contract != "Unbekannt":
                    break

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

        # === NACHBEREITUNG FÜR BEIDE (EXCEL & CSV) ===

        if df_raw.empty:
            return pd.DataFrame(), "Datei ist leer nach der Verarbeitung."

        # Fallback auf Dateinamen für Contract
        if global_contract == "Unbekannt":
            if fn_match := re.search(r'(?:contract|ext\.\s+sul)[\s_0-9-]*?(\d{6,10})', datei.name, re.IGNORECASE):
                global_contract = fn_match.group(1)

        # --- Maersk-Format Check ---
        charge_col = ermittle_erste_spalte(df_raw, ['Charge', 'Charge Code', 'Charge Type', 'Chrg'])
        ist_maersk_format = (
            charge_col is not None
            and '40HC' in df_raw.columns
            and 'Port of Loading' in df_raw.columns
            and 'Port of Destination' in df_raw.columns
        )

        if ist_maersk_format:
            eff_col = 'Valid from' if 'Valid from' in df_raw.columns else ermittle_erste_spalte(df_raw, COLUMN_ALIASES['Valid from'])
            exp_col = 'Valid to' if 'Valid to' in df_raw.columns else ermittle_erste_spalte(df_raw, COLUMN_ALIASES['Valid to'])
            tt_col = ermittle_erste_spalte(df_raw, ['Transit Time', 'Transit Days', 'TT'])

            group_cols = [c for c in ['Port of Loading', 'Port of Destination', eff_col, exp_col] if c and c in df_raw.columns]

            standard_rows = []
            for name, group in df_raw.dropna(subset=['40HC']).groupby(group_cols):
                bas_row = group[group[charge_col] == 'BAS']
                if bas_row.empty:
                    continue

                bas_text = str(bas_row['40HC'].values[0]).strip()
                waehrung, basis_betrag = extrahiere_waehrung_und_betrag(bas_text, default_currency='USD')
                if basis_betrag is None or basis_betrag <= 0:
                    continue

                group_name = name if isinstance(name, tuple) else (name,)
                pol_val = group_name[0] if len(group_name) > 0 else "Unbekannt"
                pod_val = group_name[1] if len(group_name) > 1 else "Unbekannt"
                eff_val = group_name[2] if len(group_name) > 2 else ""
                exp_val = group_name[3] if len(group_name) > 3 else ""

                standard_rows.append({
                    'Carrier': 'Maersk',
                    'Contract Number': global_contract,
                    'Port of Loading': pol_val,
                    'Port of Destination': pod_val,
                    'Valid from': eff_val,
                    'Valid to': exp_val,
                    '40HC': basis_betrag,
                    'Currency': waehrung,
                    'Included Prepaid Surcharges 40HC': ", ".join([
                        f"{r[charge_col]} = {r['40HC']}"
                        for _, r in group[group[charge_col] != 'BAS'].iterrows()
                        if ' ' in str(r['40HC'])
                    ]),
                    'Included Collect Surcharges 40HC': "",
                    'Remark': f"Transit Time: {bas_row[tt_col].values[0]}" if tt_col and tt_col in bas_row.columns else "",
                })
            df_return = pd.DataFrame(standard_rows)
        else:
            # Setze Contract Number für alle anderen Carrier
            if 'Contract Number' not in df_raw.columns or df_raw['Contract Number'].isna().all():
                df_raw['Contract Number'] = global_contract
            elif global_contract != "Unbekannt":
                df_raw['Contract Number'] = global_contract
            df_return = df_raw

        return df_return, "Excel/CSV (Multi-Sheet)"


RATEN_PROJECTION = {
    '_id': 0,
    'Carrier': 1,
    'Contract Number': 1,
    'Port of Loading': 1,
    'Port of Destination': 1,
    'Valid from': 1,
    'Valid to': 1,
    'Valid from dt': 1,
    'Valid to dt': 1,
    '40HC': 1,
    'Currency': 1,
    'Included Prepaid Surcharges 40HC': 1,
    'Included Collect Surcharges 40HC': 1,
    'Remark': 1,
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
    if value is None:
        return "?"
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.to_datetime(value).strftime("%d.%m.%Y")

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "nat"}:
        return "?"

    parsed = pd.to_datetime(text, dayfirst=True, errors='coerce')
    if pd.notna(parsed):
        return parsed.strftime("%d.%m.%Y")
    return text


def ermittle_erste_spalte(df, kandidaten):
    """Findet die erste passende Spalte im DataFrame per exakter oder Fuzzy-Suche.

    Prüft zuerst exakt (case-insensitive). Schlägt das fehl, kommt rapidfuzz
    zum Einsatz. Rückgabe: Originalspaltennamen oder None.
    """
    spalten_map = {str(c).strip().lower(): c for c in df.columns}
    df_cols_lower = list(spalten_map.keys())

    for kandidat in kandidaten:
        k_low = str(kandidat).strip().lower()

        # 1. Schneller exakter Treffer (case-insensitive)
        if k_low in spalten_map:
            return spalten_map[k_low]

        # 2. Fuzzy-Suche via rapidfuzz
        result = fuzz_process.extractOne(
            k_low, df_cols_lower, score_cutoff=FUZZY_SCORE_THRESHOLD
        )
        if result is not None:
            return spalten_map[result[0]]

    return None


def zeile_hat_bekannte_spalten(zeile_werte: list, min_treffer: int = 2) -> bool:
    """Prüft, ob eine Zeile mindestens min_treffer bekannte Spaltenbezeichnungen enthält.

    Wird genutzt, um die Header-Zeile in Excel/CSV-Dateien automatisch zu finden.
    """
    alle_aliases = [
        alias.lower()
        for aliases in COLUMN_ALIASES.values()
        for alias in aliases
    ]
    treffer = 0
    for val in zeile_werte:
        val_str = str(val).strip().lower()
        if not val_str or val_str in {"nan", "none"}:
            continue
        # Exakter Treffer ist ausreichend und schneller
        if val_str in alle_aliases:
            treffer += 1
        else:
            match = fuzz_process.extractOne(
                val_str, alle_aliases, score_cutoff=FUZZY_SCORE_THRESHOLD
            )
            if match is not None:
                treffer += 1
        if treffer >= min_treffer:
            return True
    return False


def standardisiere_spalten(df):
    """Benennt Spalten anhand von COLUMN_ALIASES (exakt + Fuzzy) in Standardnamen um.

    Jede Zielspalte wird nur einmal gemappt. Bereits vorhandene Standardspalten
    bleiben unberührt. So können neue Reederei-Varianten einfach ins Dictionary
    eingetragen werden, ohne den restlichen Code anfassen zu müssen.
    """
    rename_map = {}
    bereits_gemappt: set = set()  # verhindert Doppel-Mapping

    for ziel, kandidaten in COLUMN_ALIASES.items():
        # Standardspalte bereits unter diesem Namen vorhanden → überspringen
        if ziel in df.columns:
            continue

        # Nur noch nicht gemappte Spalten berücksichtigen
        verfuegbare_cols = [c for c in df.columns if c not in bereits_gemappt]
        spalten_map = {str(c).strip().lower(): c for c in verfuegbare_cols}
        df_cols_lower = list(spalten_map.keys())

        for kandidat in kandidaten:
            k_low = str(kandidat).strip().lower()

            # Exakter Treffer
            if k_low in spalten_map:
                original = spalten_map[k_low]
                rename_map[original] = ziel
                bereits_gemappt.add(original)
                break

            # Fuzzy-Treffer
            result = fuzz_process.extractOne(
                k_low, df_cols_lower, score_cutoff=FUZZY_SCORE_THRESHOLD
            )
            if result is not None:
                original = spalten_map[result[0]]
                rename_map[original] = ziel
                bereits_gemappt.add(original)
                break

    return df.rename(columns=rename_map)


def normalisiere_upload_dataframe(df_upload):
    out = df_upload.copy()

    def stelle_spalte_sicher(ziel, kandidaten, default=""):
        if ziel in out.columns:
            return
        quelle = ermittle_erste_spalte(out, kandidaten)
        if quelle is not None:
            out[ziel] = out[quelle]
        else:
            out[ziel] = default

    # Pflichtfelder per Fuzzy-Matching aus COLUMN_ALIASES befüllen
    stelle_spalte_sicher('Carrier', COLUMN_ALIASES['Carrier'], default='Unbekannt')
    stelle_spalte_sicher('Contract Number', COLUMN_ALIASES['Contract Number'], default='Unbekannt')
    stelle_spalte_sicher('Port of Loading', COLUMN_ALIASES['Port of Loading'], default='Unbekannt')
    stelle_spalte_sicher('Port of Destination', COLUMN_ALIASES['Port of Destination'], default='Unbekannt')
    stelle_spalte_sicher('Valid from', COLUMN_ALIASES['Valid from'], default=None)
    stelle_spalte_sicher('Valid to', COLUMN_ALIASES['Valid to'], default=None)

    if '40HC' not in out.columns:
        preis_col = ermittle_erste_spalte(out, COLUMN_ALIASES['40HC'])
        out['40HC'] = out[preis_col] if preis_col is not None else None

    if 'Currency' not in out.columns:
        waehrung_col = ermittle_erste_spalte(out, COLUMN_ALIASES['Currency'])
        out['Currency'] = out[waehrung_col] if waehrung_col is not None else 'USD'
    elif '40HC' in out.columns and 'Currency.4' in out.columns:
        # Bei breiten Excel-Exports beschreibt Currency.4 i.d.R. die 40HC-Währung.
        out['Currency'] = out['Currency.4']

    if 'Remark' not in out.columns:
        remark_col = ermittle_erste_spalte(out, COLUMN_ALIASES['Remark'])
        out['Remark'] = out[remark_col] if remark_col is not None else ""

    if 'Included Prepaid Surcharges 40HC' not in out.columns:
        out['Included Prepaid Surcharges 40HC'] = ""
    if 'Included Collect Surcharges 40HC' not in out.columns:
        out['Included Collect Surcharges 40HC'] = ""

    out['40HC'] = out['40HC'].apply(parse_decimal_wert)
    # Zeilen ohne gültige Basisfracht entfernen
    out = out[out['40HC'].notna()].copy()

    if 'Valid from dt' in out.columns:
        out['Valid from dt'] = pd.to_datetime(out['Valid from dt'], dayfirst=True, errors='coerce')
    else:
        out['Valid from dt'] = pd.to_datetime(out['Valid from'], dayfirst=True, errors='coerce')
    if 'Valid to dt' in out.columns:
        out['Valid to dt'] = pd.to_datetime(out['Valid to dt'], dayfirst=True, errors='coerce')
    else:
        out['Valid to dt'] = pd.to_datetime(out['Valid to'], dayfirst=True, errors='coerce')

    out['Carrier'] = out['Carrier'].fillna('Unbekannt').astype(str).str.strip()
    out['Contract Number'] = out['Contract Number'].fillna('Unbekannt').astype(str).str.strip()
    out['Port of Loading'] = out['Port of Loading'].fillna('Unbekannt').astype(str).str.strip()
    out['Port of Destination'] = out['Port of Destination'].fillna('Unbekannt').astype(str).str.strip()
    out['Currency'] = out['Currency'].fillna('USD').astype(str).str.upper().str.strip()
    out['Remark'] = out['Remark'].fillna('').astype(str)
    out['Included Prepaid Surcharges 40HC'] = out['Included Prepaid Surcharges 40HC'].fillna('').astype(str)
    out['Included Collect Surcharges 40HC'] = out['Included Collect Surcharges 40HC'].fillna('').astype(str)

    # --- Zeilen-Validierung: Pflichtfelder POL und POD müssen befüllt sein ---
    # Zeilen, bei denen Ladehafen ODER Zielhafen fehlen, werden verworfen.
    ungueltige_werte = {'UNBEKANNT', 'NAN', 'NONE', ''}
    out = out[
        ~(
            out['Port of Loading'].str.strip().str.upper().isin(ungueltige_werte)
            | out['Port of Destination'].str.strip().str.upper().isin(ungueltige_werte)
        )
    ].copy()

    ziel_spalten = [
        'Carrier',
        'Contract Number',
        'Port of Loading',
        'Port of Destination',
        'Valid from',
        'Valid to',
        'Valid from dt',
        'Valid to dt',
        '40HC',
        'Currency',
        'Included Prepaid Surcharges 40HC',
        'Included Collect Surcharges 40HC',
        'Remark',
    ]

    return out[ziel_spalten]


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
        collection.insert_many(batch_records, ordered=False)
        gespeichert += len(batch_records)
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

    action_col, info_col = st.columns([1, 2])
    with action_col:
        suche_starten = st.button("🔎 Suche starten", type="primary", use_container_width=True)
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

                            # HIER IST DER FIX: Zeige Fehler an, wenn das DF leer ist!
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
                        df_upload = normalisiere_upload_dataframe(df_upload)
                        df_upload['createdAt'] = datetime.now(timezone.utc)

                        gespeichert = speichere_dataframe_batchweise(df_upload)

                        # HIER IST DER ZWEITE FIX: Fehlermeldung bei 0 Zeilen!
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
        
        # --- GEFAHRENZONE (DATENBANK LEEREN) ---
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
                        lambda r: berechne_total_eur_dynamic(r, '40HC', 'Included Prepaid Surcharges 40HC', 'Included Collect Surcharges 40HC', r.name),
                        axis=1
                    )
                    df_trend = df_trend.sort_values('Valid from dt')

                    # Günstigster Preis pro Datum
                    df_min = df_trend.groupby('Valid from dt')['All-In EUR'].min().rename('Günstigster Preis (EUR)')
                    # Teuerster Preis pro Datum
                    df_max = df_trend.groupby('Valid from dt')['All-In EUR'].max().rename('Teuerster Preis (EUR)')

                    st.write(f"#### 💚 Günstigste Rate: {analytics_pol} → {analytics_pod}")
                    st.line_chart(df_min)
                    st.caption(f"Ø günstigster Preis: **{df_min.mean():.2f} EUR**")

                    st.write(f"#### 🔴 Teuerste Rate: {analytics_pol} → {analytics_pod}")
                    st.line_chart(df_max)
                    st.caption(f"Ø teuerster Preis: **{df_max.mean():.2f} EUR**")
