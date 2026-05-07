#!/usr/bin/env python3
"""
Script to add whitelist functionality to the Raten-Checks tab
"""
import re

# Read the current app.py
with open('app.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the line where to insert the whitelist collection init
# Look for: pickup_collection = init_pickup_collection()
pickup_collection_line = content.find('pickup_collection = init_pickup_collection()')
if pickup_collection_line == -1:
    print("ERROR: Could not find pickup_collection line")
    exit(1)

# Find the end of that line
pickup_collection_end = content.find('\n', pickup_collection_line)

# Whitelist collection init code
whitelist_init_code = '''

@st.cache_resource
def init_rate_whitelist_collection():
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client["SpeditionsDB"]
        whitelist_coll = db["RateWhitelist"]
        whitelist_coll.create_index("key", unique=True)
        return whitelist_coll
    except Exception:
        st.error("Rate-Whitelist Datenbankverbindung fehlgeschlagen.")
        st.stop()


rate_whitelist_collection = init_rate_whitelist_collection()


def genehmigte_konflikte_laden():
    """Lädt alle genehmigten Konflikt-IDs aus der Whitelist."""
    try:
        docs = list(rate_whitelist_collection.find({}, {"_id": 0, "key": 1}))
        return {doc["key"] for doc in docs if "key" in doc}
    except Exception as e:
        st.warning(f"Whitelist konnte nicht geladen werden: {e}")
        return set()


def genehmige_konflikt(contract, valid_from, valid_to, pol, pod, basis_40hc, currency, source_file):
    """Markiert einen Konflikt als genehmigt."""
    key = f"{contract}|{valid_from}|{valid_to}|{pol}|{pod}|{basis_40hc}|{currency}|{source_file}"
    try:
        rate_whitelist_collection.update_one(
            {"key": key},
            {"$set": {"key": key, "createdAt": datetime.now(timezone.utc)}},
            upsert=True,
        )
        return True
    except Exception as e:
        st.error(f"Fehler beim Speichern der Genehmigung: {e}")
        return False
'''

# Insert the whitelist init code after pickup_collection
content = content[:pickup_collection_end] + whitelist_init_code + content[pickup_collection_end:]

# Now find and replace the tab_rate_checks implementation
# Find: with tab_rate_checks:
tab_start = content.find('with tab_rate_checks:')
if tab_start == -1:
    print("ERROR: Could not find tab_rate_checks")
    exit(1)

# Find the start of the next major block: # === TAB 5: PICK UP ===
tab_end_marker = '# === TAB 5: PICK UP ==='
tab_end = content.find(tab_end_marker, tab_start)
if tab_end == -1:
    print("ERROR: Could not find end of tab_rate_checks")
    exit(1)

# Build new tab implementation
new_tab = '''with tab_rate_checks:
    st.write("### 📊 Raten-Checks")
    st.caption(
        "Automatische Anzeige aller Fahrgebiete mit gleicher Contract/Quotation + gleicher Gültigkeit + "
        "gleicher Route (POL → POD), bei denen **unterschiedliche Preise** hinterlegt sind."
    )
    st.caption("✅ Genehmigen: Varianten als 'OK' markieren, damit sie nicht mehr angezeigt werden.")

    if st.button("🔄 Jetzt prüfen", type="primary", key="ratecheck_start_btn"):
        st.session_state['ratecheck_gestartet'] = True
        st.session_state['ratecheck_approvals'] = {}

    if not st.session_state.get('ratecheck_gestartet', False):
        st.info("Auf 'Jetzt prüfen' klicken um die gesamte Datenbank automatisch zu analysieren.")
    else:
        with st.spinner("Lade alle Raten und suche nach Konflikten..."):
            df_ratecheck, ist_gekuerzt_ratecheck = lade_raten_aus_db(fetch_limit=MAX_DB_FETCH)

        if df_ratecheck is None or df_ratecheck.empty:
            st.info("Die Datenbank ist leer.")
        else:
            if ist_gekuerzt_ratecheck:
                st.warning(
                    f"Datenbank enthält mehr als {MAX_DB_FETCH} Raten – nur die ersten {MAX_DB_FETCH} wurden geprüft."
                )

            # Whitelist laden und Konflikte filtern
            whitelist = genehmigte_konflikte_laden()
            df_konflikte = ermittle_abweichende_raten(df_ratecheck)
            
            # Filter: Entfernen Sie bereits genehmigten Konflikte
            if not df_konflikte.empty and whitelist:
                def ist_genehmigt(row):
                    key = f"{row.get('Contract Number')}|{row.get('Valid from')}|{row.get('Valid to')}|{row.get('Port of Loading')}|{row.get('Port of Destination')}|{row.get('40HC')}|{row.get('Currency')}|{row.get('sourceFile')}"
                    return key in whitelist
                df_konflikte = df_konflikte[~df_konflikte.apply(ist_genehmigt, axis=1)].copy()

            if df_konflikte.empty:
                st.success("Alles sauber: Keine doppelten Raten mit abweichenden Preisen gefunden.")
            else:
                gruppen_spalten = ['contract_key', 'valid_from_key', 'valid_to_key', 'pol_key', 'pod_key']
                gruppen = list(df_konflikte.groupby(gruppen_spalten, dropna=False))
                st.warning(
                    f"⚠️ {len(gruppen)} Konflikt-Gruppe(n) gefunden – gleiche Contract + gleiche Gültigkeit + "
                    f"gleiche Route, aber unterschiedliche Preise."
                )

                if 'ratecheck_approvals' not in st.session_state:
                    st.session_state['ratecheck_approvals'] = {}

                for gruppe_index, (_, gruppe_df) in enumerate(gruppen, start=1):
                    gruppe_df = gruppe_df.reset_index(drop=True)
                    erste_zeile = gruppe_df.iloc[0]
                    valid_from_label = formatiere_datum_fuer_header(erste_zeile.get('Valid from'))
                    valid_to_label = formatiere_datum_fuer_header(erste_zeile.get('Valid to'))
                    label = (
                        f"📄 {erste_zeile.get('Contract Number', 'Unbekannt')} | "
                        f"{erste_zeile.get('Port of Loading', '?')} ➡️ {erste_zeile.get('Port of Destination', '?')} | "
                        f"📅 {valid_from_label} bis {valid_to_label} | "
                        f"{int(erste_zeile.get('group_variants', 0) or 0)} Varianten"
                    )

                    with st.expander(label, expanded=(gruppe_index <= 3)):
                        for row_index, (_, row) in enumerate(gruppe_df.iterrows(), start=1):
                            source_label = str(row.get('sourceFile') or '').strip()
                            approval_key = f"grp{gruppe_index}_var{row_index}"
                            
                            col_check, col_info = st.columns([0.08, 0.92])
                            with col_check:
                                is_approved = st.checkbox(
                                    "✓",
                                    value=st.session_state['ratecheck_approvals'].get(approval_key, False),
                                    key=f"cb_{approval_key}",
                                )
                                st.session_state['ratecheck_approvals'][approval_key] = is_approved
                            
                            with col_info:
                                header = f"**Variante {row_index} | Carrier: {row.get('Carrier', 'Unbekannt')}**"
                                if source_label and source_label not in {'nan', 'None', ''}:
                                    header += f" &nbsp;|&nbsp; <small>📁 {source_label}</small>"
                                st.markdown(header, unsafe_allow_html=True)
                            
                            anzeige_container_daten(
                                row,
                                "40' HC",
                                '40HC',
                                'Included Prepaid Surcharges 40HC',
                                'Included Collect Surcharges 40HC',
                                f"ratecheck_{gruppe_index}_{row_index}",
                            )
                            if pd.notna(row.get('Remark')) and row.get('Remark') != "":
                                st.info(f"💡 Bemerkung: {row['Remark']}")
                            if row_index < len(gruppe_df):
                                st.divider()

                st.divider()
                if st.session_state.get('ratecheck_approvals'):
                    approved_count = sum(1 for v in st.session_state['ratecheck_approvals'].values() if v)
                    if approved_count > 0:
                        if st.button(f"💾 {approved_count} Variante(n) als genehmigt speichern", type="primary", key="save_approvals"):
                            saved_count = 0
                            for approval_key in list(st.session_state['ratecheck_approvals'].keys()):
                                if st.session_state['ratecheck_approvals'][approval_key]:
                                    parts = approval_key.split('var')
                                    if len(parts) == 2:
                                        grp_num = int(parts[0].replace('grp', ''))
                                        row_num = int(parts[1])
                                        if 0 < grp_num <= len(gruppen):
                                            _, g_df = gruppen[grp_num - 1]
                                            g_df = g_df.reset_index(drop=True)
                                            if 0 < row_num <= len(g_df):
                                                row = g_df.iloc[row_num - 1]
                                                if genehmige_konflikt(
                                                    row.get('Contract Number'),
                                                    str(row.get('Valid from')),
                                                    str(row.get('Valid to')),
                                                    row.get('Port of Loading'),
                                                    row.get('Port of Destination'),
                                                    str(row.get('40HC')),
                                                    row.get('Currency'),
                                                    str(row.get('sourceFile') or '')
                                                ):
                                                    saved_count += 1
                            
                            if saved_count > 0:
                                st.success(f"✅ {saved_count} Variante(n) genehmigt. Seite wird aktualisiert...")
                                st.session_state['ratecheck_gestartet'] = False
                                time.sleep(1)
                                st.rerun()


'''

# Replace the tab
content = content[:tab_start] + new_tab + content[tab_end:]

# Write back
with open('app.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Whitelist-Funktionalität erfolgreich hinzugefügt!")
