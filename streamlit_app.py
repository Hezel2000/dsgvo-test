import streamlit as st
import sqlite3
from datetime import datetime, timezone
import hashlib
import uuid
import json
import pandas as pd
from contextlib import contextmanager

DB_PATH = "consent.db"

# ---------- Datenbank ----------
@contextmanager
def db():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys = ON;")
    try:
        yield con
        con.commit()
    finally:
        con.close()

def init_db():
    with db() as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS consent_texts(
            id INTEGER PRIMARY KEY,
            version TEXT NOT NULL,
            language TEXT NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            body_hash TEXT NOT NULL,    -- Nachweis, was genau stand drin
            created_at TEXT NOT NULL
        );""")
        con.execute("""
        CREATE TABLE IF NOT EXISTS consents(
            id TEXT PRIMARY KEY,       -- public id
            created_at TEXT NOT NULL,  -- UTC ISO
            subject_email TEXT,        -- optional (Double-Opt-In möglich)
            subject_name TEXT,         -- optional
            purposes_json TEXT NOT NULL,   -- {"newsletter": true, "tracking": false, ...}
            consent_text_id INTEGER NOT NULL,
            consent_text_version TEXT NOT NULL,
            consent_text_hash TEXT NOT NULL,
            is_granted INTEGER NOT NULL CHECK(is_granted IN (0,1)),
            revoked_at TEXT,           -- UTC ISO, falls widerrufen
            revocation_note TEXT,
            FOREIGN KEY(consent_text_id) REFERENCES consent_texts(id)
        );""")

def upsert_consent_text(version: str, language: str, title: str, body: str):
    h = hashlib.sha256(body.encode("utf-8")).hexdigest()
    with db() as con:
        con.execute("""
        INSERT INTO consent_texts(version, language, title, body, body_hash, created_at)
        VALUES (?, ?, ?, ?, ?, ?);
        """, (version, language, title, body, h, datetime.now(timezone.utc).isoformat()))
    return h

def get_latest_consent_text(language="de"):
    with db() as con:
        cur = con.execute("""
            SELECT id, version, language, title, body, body_hash, created_at
            FROM consent_texts
            WHERE language=?
            ORDER BY created_at DESC
            LIMIT 1;
        """, (language,))
        row = cur.fetchone()
    return row

def list_consent_texts():
    with db() as con:
        cur = con.execute("""
            SELECT id, version, language, title, body, body_hash, created_at
            FROM consent_texts
            ORDER BY created_at DESC;
        """)
        return cur.fetchall()

def save_consent(subject_email, subject_name, purposes: dict, ct_row):
    ct_id, ct_version, _, _, _, ct_hash, _ = ct_row
    with db() as con:
        cid = str(uuid.uuid4())
        con.execute("""
        INSERT INTO consents(id, created_at, subject_email, subject_name, purposes_json,
                             consent_text_id, consent_text_version, consent_text_hash,
                             is_granted, revoked_at, revocation_note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL);
        """, (
            cid,
            datetime.now(timezone.utc).isoformat(),
            subject_email or None,
            subject_name or None,
            json.dumps(purposes, ensure_ascii=False),
            ct_id, ct_version, ct_hash,
            1
        ))
    return cid

def list_consents(email_filter=None):
    q = """SELECT id, created_at, subject_email, subject_name, purposes_json,
                  consent_text_version, is_granted, revoked_at
           FROM consents
           WHERE 1=1 """
    params = []
    if email_filter:
        q += " AND subject_email = ?"
        params.append(email_filter)
    q += " ORDER BY datetime(created_at) DESC"
    with db() as con:
        cur = con.execute(q, params)
        rows = cur.fetchall()
    return rows

def revoke_consent(consent_id, note="Widerruf durch Betroffene*n via UI"):
    with db() as con:
        con.execute("""
            UPDATE consents
            SET is_granted = 0,
                revoked_at = ?,
                revocation_note = ?
            WHERE id = ? AND revoked_at IS NULL;
        """, (datetime.now(timezone.utc).isoformat(), note, consent_id))

# ---------- Hilfen ----------
def sha256_preview(text: str, n=10):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:n]

def purposes_default():
    return {
        "newsletter": False,
        "produktinfos": False,
        "feedback_kontakt": False,
        "datenanalyse_aggregiert": False,  # z.B. anonyme Statistiken
    }

# ---------- UI ----------
st.set_page_config(page_title="DSGVO-Einwilligungen", page_icon="✅", layout="centered")
st.title("DSGVO-Einwilligungen einholen & verwalten")

init_db()

with st.expander("ℹ️ Rechtliche Hinweise / Setup (Admin)"):
    st.markdown("""
- **Transparenz**: Du brauchst eine verlinkte Datenschutzerklärung mit allen Pflichtinfos (Art. 13 DSGVO).  
- **Granularität**: Einwilligungen **zweckbezogen** abfragen (kein Bündelzwang).  
- **Widerruf**: Muss so einfach möglich sein wie die Erteilung.  
- **Nachweis**: Wir speichern Timestamp, Version + Hash des Einwilligungstextes und die Zweck-Entscheidungen.  
- **Double-Opt-In** (optional empfohlen bei E-Mail-Zwecken): Hier als Erweiterung vorgesehen (E-Mail-Versand muss bei dir konfiguriert werden).
""")

st.header("1) Aktuell gültigen Einwilligungstext verwalten (Versionierung)")
st.caption("Lege oder aktualisiere den Text, zu dem die Betroffenen zustimmen.")

with st.form("consent_text_form"):
    col1, col2 = st.columns(2)
    with col1:
        version = st.text_input("Version", value="v1.0", help="z. B. v1.0, v1.1 …")
    with col2:
        language = st.selectbox("Sprache", options=["de", "en"], index=0)
    title = st.text_input("Titel", value="Einwilligung in die Verarbeitung personenbezogener Daten")
    default_body = (
        "Verantwortlicher: Beispiel GmbH, Musterstraße 1, 12345 Musterstadt.\n\n"
        "Zwecke:\n"
        "- Newsletter-Versand (E-Mail)\n"
        "- Zusendung von Produktinformationen (E-Mail)\n"
        "- Kontaktaufnahme für Feedback (E-Mail)\n"
        "- Aggregierte, anonymisierte Datenanalyse\n\n"
        "Hinweise: Die Einwilligung ist freiwillig und kann jederzeit mit Wirkung für die Zukunft widerrufen "
        "werden (z. B. per Link oder E-Mail an datenschutz@beispiel.de). Weitere Informationen findest du "
        "in unserer Datenschutzerklärung: https://example.org/datenschutz\n"
        "Speicherdauer: bis Widerruf oder Ablauf des Zwecks.\n"
    )
    body = st.text_area("Einwilligungstext (für Betroffene sichtbar)", height=220, value=default_body)
    submitted_ct = st.form_submit_button("Einwilligungstext speichern/aktualisieren")
    if submitted_ct:
        h = upsert_consent_text(version, language, title, body)
        st.success(f"Gespeichert. Version **{version}** (Sprache: {language}), Hash: `{h[:12]}…`")

latest = get_latest_consent_text(language="de")
if not latest:
    st.warning("Noch kein Einwilligungstext vorhanden. Bitte oben zuerst anlegen.")
    st.stop()

ct_id, ct_version, ct_lang, ct_title, ct_body, ct_hash, ct_created = latest
st.info(f"Aktive Version: **{ct_version}** (Hash `{ct_hash[:12]}…`, erstellt {ct_created})")

st.header("2) Einwilligung einholen")
st.caption("Dieses Formular entspricht der Sicht der betroffenen Person.")

with st.form("consent_collect_form"):
    st.subheader(ct_title)
    with st.expander("Einwilligungstext anzeigen"):
        st.write(ct_body)

    st.markdown("**Bitte wähle je Zweck aus (freiwillig, nicht gekoppelt):**")
    p = purposes_default()
    p["newsletter"] = st.checkbox("Newsletter per E-Mail erhalten")
    p["produktinfos"] = st.checkbox("Produktinformationen per E-Mail erhalten")
    p["feedback_kontakt"] = st.checkbox("Kontaktaufnahme für Feedback (E-Mail)")
    p["datenanalyse_aggregiert"] = st.checkbox("Aggregierte/anonymisierte Analyse deiner Daten zulassen")

    st.divider()
    subject_name = st.text_input("Dein Name (optional)")
    subject_email = st.text_input("Deine E-Mail (optional, empfohlen für Nachweis/Widerruf)")
    ack_info = st.checkbox("Ich habe die Datenschutzhinweise gelesen und verstanden.", value=False)
    age_ok = st.checkbox("Ich bin mindestens 16 Jahre alt oder habe die Einwilligung der Sorgeberechtigten.", value=False)

    submitted = st.form_submit_button("Einwilligung erteilen")
    if submitted:
        if not ack_info or not age_ok:
            st.error("Bitte bestätige Datenschutzhinweise und Altersangabe.")
        elif not any(p.values()):
            st.warning("Du hast aktuell keine Zwecke ausgewählt. Das ist erlaubt – es wird dann **keine** Einwilligung gespeichert.")
        else:
            cid = save_consent(subject_email.strip() or None, subject_name.strip() or None, p, latest)
            st.success("Danke! Deine Einwilligung wurde gespeichert.")
            st.write(f"Deine Vorgangs-ID (für Nachweis/Widerruf): `{cid}`")
            if subject_email:
                st.caption("Tipp: Mit deiner E-Mail kannst du unten deine Einwilligungen einsehen oder widerrufen.")

st.header("3) Einwilligungen einsehen & widerrufen (Selbstbedienung)")
email_filter = st.text_input("E-Mail (falls angegeben) – zeigt nur deine Einträge")
rows = list_consents(email_filter=email_filter.strip() or None)
if rows:
    df = []
    for (cid, created_at, email, name, purposes_json, ctv, is_granted, revoked_at) in rows:
        purposes = json.loads(purposes_json)
        df.append({
            "Vorgangs-ID": cid,
            "Erstellt (UTC)": created_at,
            "E-Mail": email or "",
            "Name": name or "",
            "Version": ctv,
            "Newsletter": purposes.get("newsletter", False),
            "Produktinfos": purposes.get("produktinfos", False),
            "Feedback-Kontakt": purposes.get("feedback_kontakt", False),
            "Anonymisierte Analyse": purposes.get("datenanalyse_aggregiert", False),
            "Aktiv": bool(is_granted) and not revoked_at,
            "Widerrufen am (UTC)": revoked_at or ""
        })
    df = pd.DataFrame(df)
    st.dataframe(df, use_container_width=True)
    st.download_button("CSV exportieren", df.to_csv(index=False).encode("utf-8"), "consents_export.csv", "text/csv")

    st.markdown("**Widerruf:** Wähle einen Eintrag aus und widerrufe.")
    cid_to_revoke = st.selectbox("Vorgangs-ID wählen", options=[""] + [r[0] for r in rows], index=0)
    if st.button("Ausgewählten Eintrag widerrufen", disabled=(cid_to_revoke == "")):
        revoke_consent(cid_to_revoke)
        st.success("Einwilligung widerrufen. Bitte Ansicht neu laden.")
else:
    st.caption("Keine (passenden) Einträge gefunden.")

st.header("4) Admin: Versionen & Nachweis")
with st.expander("Alle Einwilligungstexte (Versionen)"):
    versions = list_consent_texts()
    if versions:
        vdf = pd.DataFrame([{
            "ID": r[0],
            "Version": r[1],
            "Sprache": r[2],
            "Titel": r[3],
            "Hash (SHA-256)": r[5],
            "Erstellt (UTC)": r[6],
            "Inhalt-Vorschau": (r[4][:180] + "…") if len(r[4]) > 180 else r[4]
        } for r in versions])
        st.dataframe(vdf, use_container_width=True)
    else:
        st.caption("Noch keine Versionen gespeichert.")
