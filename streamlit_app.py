# edt_reports_streamlit.py
import streamlit as st
import pandas as pd
import re
import uuid
from datetime import datetime, date, time
from dateutil import parser as dtparser
import pytz

# ---------------- Utilities (same robust parser as before) ----------------

def normalize_group_label(x):
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    s = str(x).strip()
    if not s:
        return None
    m = re.search(r'G\s*\.?\s*(\d+)', s, re.I)
    if m:
        return f'G {m.group(1)}'
    m2 = re.search(r'^(?:groupe)?\s*(\d+)$', s, re.I)
    if m2:
        return f'G {m2.group(1)}'
    return s

def is_time_like(x):
    if x is None:
        return False
    if isinstance(x, (pd.Timestamp, datetime, time)):
        return True
    s = str(x).strip()
    if not s:
        return False
    if re.match(r'^\d{1,2}[:hH]\d{2}(\s*[AaPp][Mm]\.?)?$', s):
        return True
    return False

def to_time(x):
    if x is None:
        return None
    if isinstance(x, time):
        return x
    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime().time()
    if isinstance(x, datetime):
        return x.time()
    s = str(x).strip()
    if not s:
        return None
    s2 = s.replace('h', ':').replace('H', ':')
    try:
        dt = dtparser.parse(s2, dayfirst=True)
        return dt.time()
    except Exception:
        return None

def to_date(x):
    if x is None:
        return None
    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime().date()
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    if not s:
        return None
    try:
        dt = dtparser.parse(s, dayfirst=True, fuzzy=True)
        return dt.date()
    except Exception:
        return None

def find_week_rows(df):
    rows = []
    for i in range(len(df)):
        try:
            v = df.iat[i, 0]
        except Exception:
            v = None
        if isinstance(v, str) and re.match(r'^\s*S\s*\d+', v.strip(), re.I):
            rows.append(i)
    return rows

def find_slot_rows(df):
    rows = []
    for i in range(len(df)):
        try:
            v = df.iat[i, 0]
        except Exception:
            v = None
        if isinstance(v, str) and re.match(r'^\s*H\s*\d+', v.strip(), re.I):
            rows.append(i)
    return rows

def parse_sheet_to_events(xls, sheet_name):
    """
    Retourne une liste d'événements structurés extraits d'une feuille (header=None).
    Chaque élément : {
      'summary', 'teachers' (list), 'description' (str), 'start' (datetime), 'end', 'groups' (list)
    }
    """
    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
    nrows, ncols = df.shape

    s_rows = find_week_rows(df)
    h_rows = find_slot_rows(df)

    raw_events = []

    for r in h_rows:
        p_candidates = [s for s in s_rows if s <= r]
        if not p_candidates:
            continue
        p = max(p_candidates)
        date_row = p + 1
        group_row = p + 2

        date_cols = [c for c in range(ncols) if date_row < nrows and to_date(df.iat[date_row, c]) is not None]

        for c in date_cols:
            for col in (c, c + 1):
                if col >= ncols:
                    continue
                try:
                    summary = df.iat[r, col]
                except Exception:
                    summary = None
                if pd.isna(summary) or summary is None:
                    continue
                summary_str = str(summary).strip()
                if not summary_str:
                    continue

                # teacher
                teacher = None
                if (r + 2) < nrows:
                    try:
                        t = df.iat[r + 2, col]
                        if not pd.isna(t):
                            teacher = str(t).strip()
                    except Exception:
                        teacher = None

                # find first time-like cell after summary to avoid grabbing next-session summary
                stop_idx = None
                for off in range(1, 12):
                    idx = r + off
                    if idx >= nrows:
                        break
                    try:
                        if is_time_like(df.iat[idx, col]):
                            stop_idx = idx
                            break
                    except Exception:
                        continue
                if stop_idx is None:
                    stop_idx = min(r + 7, nrows)

                # description cells between summary line and first time cell (exclusive)
                desc_parts = []
                for idx in range(r + 1, stop_idx):
                    if idx >= nrows:
                        break
                    try:
                        cell = df.iat[idx, col]
                    except Exception:
                        cell = None
                    if pd.isna(cell) or cell is None:
                        continue
                    s = str(cell).strip()
                    if not s:
                        continue
                    if to_date(cell) is not None:
                        continue
                    if teacher and s == teacher:
                        continue
                    if s == summary_str:
                        continue
                    desc_parts.append(s)
                desc_text = " | ".join(dict.fromkeys(desc_parts))

                # find start/end time (scan forward)
                start_val = None
                end_val = None
                for off in range(1, 13):
                    idx = r + off
                    if idx >= nrows:
                        break
                    try:
                        v = df.iat[idx, col]
                    except Exception:
                        v = None
                    if is_time_like(v):
                        if start_val is None:
                            start_val = v
                        elif end_val is None and v != start_val:
                            end_val = v
                            break
                if start_val is None or end_val is None:
                    continue
                start_t = to_time(start_val)
                end_t = to_time(end_val)
                if start_t is None or end_t is None:
                    continue

                # day date
                date_cell = df.iat[date_row, c]
                d = to_date(date_cell)
                if d is None:
                    continue

                dtstart = datetime.combine(d, start_t)
                dtend = datetime.combine(d, end_t)

                # groups detection
                gl = None
                gl_next = None
                if group_row < nrows:
                    try:
                        gl_raw = df.iat[group_row, col]
                        gl = normalize_group_label(gl_raw)
                    except Exception:
                        gl = None
                    if (col + 1) < ncols:
                        try:
                            gl_next_raw = df.iat[group_row, col + 1]
                            gl_next = normalize_group_label(gl_next_raw)
                        except Exception:
                            gl_next = None

                is_left_col = (col == c)
                right_summary = None
                if (col + 1) < ncols:
                    try:
                        right_summary = df.iat[r, col + 1]
                    except Exception:
                        right_summary = None

                groups = set()
                if is_left_col and (pd.isna(right_summary) or right_summary is None) and gl and gl_next and gl != gl_next:
                    groups.add(gl); groups.add(gl_next)
                else:
                    if gl:
                        groups.add(gl)

                raw_events.append({
                    'summary': summary_str,
                    'teachers': set([teacher]) if teacher else set(),
                    'descriptions': set([desc_text]) if desc_text else set(),
                    'start': dtstart,
                    'end': dtend,
                    'groups': groups
                })

    # merge raw events by (summary, start, end)
    merged = {}
    for e in raw_events:
        key = (e['summary'], e['start'], e['end'])
        if key not in merged:
            merged[key] = {
                'summary': e['summary'],
                'teachers': set(),
                'descriptions': set(),
                'start': e['start'],
                'end': e['end'],
                'groups': set()
            }
        merged[key]['teachers'].update(e.get('teachers', set()))
        merged[key]['descriptions'].update(e.get('descriptions', set()))
        merged[key]['groups'].update(e.get('groups', set()))

    out = []
    for v in merged.values():
        out.append({
            'summary': v['summary'],
            'teachers': sorted([t for t in v['teachers'] if t and str(t).lower() not in ['nan','none']]),
            'description': " | ".join(sorted([d for d in v['descriptions'] if d and d.strip()])),
            'start': v['start'],
            'end': v['end'],
            'groups': sorted(list(v['groups']))
        })
    return out

# ---------------- Maquette loader (flexible) ----------------

def read_maquette(xls):
    """
    Look up sheet 'Maquette' (case-insensitive). Try to find columns:
      - matter/subject name
      - promo (P1/P2)
      - target sessions (col names: 'seances','sessions','target','cible','nb', 'heures')
    Returns DataFrame normalized with columns: ['promo','subject','target']
    If Maquette not present or malformed returns empty DataFrame.
    """
    sheet_candidates = [s for s in xls.sheet_names if s.lower() == 'maquette' or 'maquette' in s.lower()]
    if not sheet_candidates:
        return pd.DataFrame(columns=['promo','subject','target'])
    sheet = sheet_candidates[0]
    try:
        mq = pd.read_excel(xls, sheet_name=sheet)
    except Exception:
        return pd.DataFrame(columns=['promo','subject','target'])

    cols = {c.lower(): c for c in mq.columns}
    # heuristics
    subject_col = None
    promo_col = None
    target_col = None
    for k in cols:
        if any(w in k for w in ['mati', 'subject', 'module', 'ue', 'course']):
            subject_col = cols[k]; break
    for k in cols:
        if any(w in k for w in ['promo','promotion','year','p1','p2']):
            promo_col = cols[k]; break
    for k in cols:
        if any(w in k for w in ['seanc','session','target','cibl','nb','heure','hours']):
            target_col = cols[k]; break
    if subject_col is None:
        return pd.DataFrame(columns=['promo','subject','target'])

    # build normalized dataframe
    rows = []
    for _, row in mq.iterrows():
        subject = row.get(subject_col)
        if pd.isna(subject): continue
        promo = row.get(promo_col) if promo_col else None
        target = row.get(target_col) if target_col else None
        # try to coerce numeric target
        try:
            tval = float(target) if target is not None and not pd.isna(target) else None
        except Exception:
            tval = None
        promo_str = str(promo).strip() if promo and not pd.isna(promo) else ''
        rows.append({'promo': promo_str, 'subject': str(subject).strip(), 'target': tval})
    return pd.DataFrame(rows, columns=['promo','subject','target'])

# ---------------- Aggregation helpers ----------------

def build_events_index(xls, sheet_names):
    """
    Parse provided sheets and return a dict promo->events list
    promo names are sheet names (e.g. 'EDT P1','EDT P2')
    """
    out = {}
    for s in sheet_names:
        try:
            events = parse_sheet_to_events(xls, s)
            out[s] = events
        except Exception:
            out[s] = []
    return out

def count_sessions_by_subject(events):
    """Return dict subject->count of events (sessions)"""
    cnt = {}
    for ev in events:
        subj = ev['summary']
        cnt[subj] = cnt.get(subj, 0) + 1
    return cnt

def flatten_events_table(events):
    """Return DataFrame rows with columns: subject,start,end,groups,teachers,description"""
    rows = []
    for ev in events:
        rows.append({
            'subject': ev['summary'],
            'start': ev['start'],
            'end': ev['end'],
            'groups': ', '.join(ev['groups']) if ev['groups'] else '',
            'teachers': ', '.join(ev['teachers']) if ev['teachers'] else '',
            'description': ev['description'] if ev.get('description') else ''
        })
    if rows:
        return pd.DataFrame(rows)
    else:
        return pd.DataFrame(columns=['subject','start','end','groups','teachers','description'])

# ---------------- UI ----------------

st.set_page_config(page_title='EDT Reports', layout='wide')
st.title('EDT — Rapports et comparaisons')

uploaded = st.file_uploader('Charger le fichier Excel (EDT + Maquette)', type=['xlsx'])
if uploaded is None:
    st.info('Upload un fichier .xlsx pour commencer.')
    st.stop()

try:
    xls = pd.ExcelFile(uploaded)
    sheets = xls.sheet_names
except Exception as e:
    st.error('Impossible de lire le fichier Excel: ' + str(e))
    st.stop()

st.write('Feuilles trouvées :', sheets)

# parse events for P1/P2 if present
promo_sheets = [s for s in sheets if s.strip().upper() in ['EDT P1','EDT P2','P1','P2','EDT P1 ','EDT P2 ']]
# fallback: if exact names present use them
candidates = []
if 'EDT P1' in sheets: candidates.append('EDT P1')
if 'EDT P2' in sheets: candidates.append('EDT P2')
if not candidates:
    # take any sheets starting with 'EDT' or containing 'P1'/'P2'
    candidates = [s for s in sheets if 'EDT' in s.upper() or 'P1' in s.upper() or 'P2' in s.upper()]
promo_sheets = [s for s in ['EDT P1','EDT P2'] if s in sheets] or promo_sheets

events_by_promo = build_events_index(xls, promo_sheets)
maquette_df = read_maquette(xls)

# navigation
page = st.sidebar.selectbox('Page', [
    '1 — Comparaison avec Maquette',
    '2 — Récap par matière',
    '3 — Récap par enseignant',
    '4 — Récap complet textuel'
])

# ---------- Page 1: comparaison ----------
if page.startswith('1'):
    st.header('Comparaison des séances par matière vs Maquette')
    st.write('Hypothèse : on compare le **nombre de séances** extraites au **target** dans la feuille Maquette (si présente).')
    if maquette_df.empty:
        st.warning('Feuille Maquette non trouvée ou non lisible — seul le total des séances extraites sera affiché.')
    # compute counts
    results = []
    for promo, evs in events_by_promo.items():
        counts = count_sessions_by_subject(evs)
        # build DataFrame for this promo
        dfp = pd.DataFrame([(k, v) for k, v in counts.items()], columns=['subject','count'])
        dfp['promo_sheet'] = promo
        # if maquette present, find targets matching subject and promo
        if not maquette_df.empty:
            # try to match ignoring case and whitespace
            def find_target(subject):
                # try exact match
                m = maquette_df[maquette_df['subject'].str.lower().str.strip() == subject.lower().strip()]
                if not m.empty:
                    # if promo column used, prefer matching row where promo matches
                    if 'promo' in maquette_df.columns and m.shape[0] > 1:
                        mm = m[m['promo'].str.contains(promo.split()[-1]) if m['promo'].notna().any() else False]
                        if not mm.empty:
                            return mm['target'].iloc[0]
                    return m['target'].iloc[0]
                # fuzzy: contains
                m2 = maquette_df[maquette_df['subject'].str.lower().str.contains(subject.split()[0].lower())]
                if not m2.empty:
                    return m2['target'].iloc[0]
                return None
            dfp['target'] = dfp['subject'].apply(find_target)
            dfp['diff'] = dfp.apply(lambda r: (r['count'] - r['target']) if pd.notna(r['target']) else None, axis=1)
        results.append(dfp)
    if results:
        full = pd.concat(results, ignore_index=True)
        st.dataframe(full.sort_values(['promo_sheet','subject']).reset_index(drop=True))
    else:
        st.info('Aucune séance détectée dans les feuilles sélectionnées.')

# ---------- Page 2: recap by subject ----------
elif page.startswith('2'):
    st.header('Récapitulatif par matière (sélectionne une matière)')
    # compile set of subjects
    subjects = set()
    for evs in events_by_promo.values():
        for ev in evs:
            subjects.add(ev['summary'])
    subjects = sorted(list(subjects))
    if not subjects:
        st.warning('Aucune matière trouvée dans les plannings.')
    else:
        chosen = st.selectbox('Choisir une matière', options=subjects)
        st.write(f'Séances pour : **{chosen}**')
        cols = ['subject','start','end','groups','teachers','description']
        tables = {}
        for promo, evs in events_by_promo.items():
            df_ev = flatten_events_table([e for e in evs if e['summary'] == chosen])
            if df_ev.empty:
                st.subheader(promo)
                st.info('Aucune séance pour cette matière.')
            else:
                st.subheader(promo)
                # sort by date
                df_ev = df_ev.sort_values('start')
                # afficher deux tables? l'utilisateur demandait deux tableaux (un pour P1 et un pour P2) — on affiche par promo
                st.dataframe(df_ev.reset_index(drop=True))

# ---------- Page 3: recap by teacher ----------
elif page.startswith('3'):
    st.header('Récapitulatif par enseignant')
    # build list of teachers
    teachers = set()
    for evs in events_by_promo.values():
        for ev in evs:
            for t in ev['teachers']:
                if t and t.lower() not in ['nan','none']:
                    teachers.add(t)
    teachers = sorted(list(teachers))
    if not teachers:
        st.warning('Aucun enseignant détecté.')
    else:
        chosen = st.selectbox('Choisir un enseignant', options=teachers)
        st.write(f'Séances pour : **{chosen}**')
        for promo, evs in events_by_promo.items():
            df_ev = flatten_events_table([e for e in evs if chosen in e['teachers']])
            st.subheader(promo)
            if df_ev.empty:
                st.info('Aucune séance pour cet enseignant dans cette promo.')
            else:
                st.dataframe(df_ev.sort_values('start').reset_index(drop=True))

# ---------- Page 4: textual recap ----------
else:
    st.header('Récapitulatif textuel complet (par promo)')
    for promo, evs in events_by_promo.items():
        st.subheader(promo)
        if not evs:
            st.info('Aucune séance détectée.')
            continue
        # group by subject and list events
        by_subject = {}
        for e in evs:
            by_subject.setdefault(e['summary'], []).append(e)
        for subj in sorted(by_subject.keys()):
            st.markdown(f'**{subj}**')
            lst = by_subject[subj]
            # sort by date
            lst = sorted(lst, key=lambda x: x['start'])
            for e in lst:
                groups = ', '.join(e['groups']) if e['groups'] else '—'
                teachers = ', '.join(e['teachers']) if e['teachers'] else '—'
                desc = e['description'] if e.get('description') else ''
                st.write(f"- {e['start'].strftime('%Y-%m-%d %H:%M')} → {e['end'].strftime('%H:%M')} — Groupes: {groups} — Enseignant(s): {teachers} {('- '+desc) if desc else ''}")

# end
