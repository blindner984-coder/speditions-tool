try:
            reader = PyPDF2.PdfReader(datei)
            text = " ".join([page.extract_text() for page in reader.pages])
            
            # 1. DATUMS-EXTRAKTION (Flexibel für DD.MM.YYYY oder YYYY-MM-DD)
            date_matches = re.findall(r'(\d{2,4}[.\-/]\d{2}[.\-/]\d{2,4})', text)
            v_from = date_matches[0] if len(date_matches) > 0 else "Unbekannt"
            v_to = date_matches[-1] if len(date_matches) > 1 else "Unbekannt"

            # 2. POL / POD LOGIK (Suche nach gängigen Bezeichnungen)
            def find_port(keywords, full_text):
                for kw in keywords:
                    # Sucht nach Keyword + Wort bis zu 25 Zeichen (stoppt bei Zeilenumbruch oder Sonderzeichen)
                    match = re.search(f'{kw}[:\s]+([A-Za-z\s,\-]{{3,25}})', full_text, re.IGNORECASE)
                    if match:
                        res = match.group(1).strip().split('\n')[0] # Nur erste Zeile falls Umbruch
                        return re.sub(r'[^A-Za-z\s\-]', '', res).strip()
                return "Unbekannt"

            pol_str = find_port(['Port of Loading', 'POL', 'Origin Port', 'Loading Port'], text)
            pod_str = find_port(['Port of Discharge', 'POD', 'Destination Port', 'Discharge Port', 'Place of Delivery'], text)

            # 3. CARRIER ERKENNUNG
            carrier = "Unbekannt"
            if "msc" in text.lower(): carrier = "MSC"
            elif "hapag" in text.lower(): carrier = "Hapag-Lloyd"
            elif "maersk" in text.lower(): carrier = "Maersk"
            elif "cosco" in text.lower(): carrier = "COSCO"

            # 4. RATEN-LOGIK (Suche nach 40'HC Preisen)
            # Sucht nach Beträgen (z.B. 1.250,00 oder 950) gefolgt von USD/EUR/Currency
            rate = 0
            # Spezielle Suche nach 40ft High Cube Mustern
            rate_match = re.search(r'(?:40\'?HC|40ft?|High Cube)[\s:]*(?:USD|EUR|[\$€])?\s*([\d\.,]{3,9})', text, re.IGNORECASE)
            if not rate_match:
                # Fallback: Suche nach dem ersten großen Betrag neben einer Währung
                rate_match = re.search(r'(\d{3,4})\s*(?:USD|EUR)', text)
            
            if rate_match:
                rate_val = rate_match.group(1).replace('.', '').replace(',', '.')
                rate = float(re.sub(r'[^\d.]', '', rate_val))

            # 5. CONTRACT NUMMER
            contract_match = re.search(r'(?:Contract|Quote|Reference|Ref\.?|Agreement)[\s#:]*([A-Z0-9]{5,20})', text, re.IGNORECASE)
            contract_no = contract_match.group(1) if contract_match else "Unbekannt"

            df_pdf = pd.DataFrame([{
                'Carrier': carrier,
                'Contract Number': contract_no,
                'Port of Loading': pol_str,
                'Port of Destination': pod_str,
                'Valid from': v_from,
                'Valid to': v_to,
                '40HC': rate,
                'Currency': "EUR" if "EUR" in text.upper() else "USD",
                'Included Prepaid Surcharges 40HC': "Extrahiert aus PDF",
                'Included Collect Surcharges 40HC': "",
                'Remark': 'Multi-Carrier Auto-Import'
            }])
