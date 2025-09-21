import streamlit as st
import pandas as pd
import re
from datetime import datetime, date, time
from dateutil import parser as dtparser

# ---------------- Utilities ----------------

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

                # collect teachers (multiple)
                teachers = []
                if (r + 2) < nrows:
                    for off in range(2, 6):
                        if (r + off) >= nrows:
                            break
                        try:
                            t = df.iat[r + off, col]
                        except Exception:
                            t = None
                        if t and not pd.isna(t) and not is_time_like(t):
                            teachers.append(str(t).strip())
                teachers = list(dict.fromkeys(teachers))

                # find first time-like cell after summary
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

                # description cells
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
                    if s in teachers or s == summary_str:
                        continue
                    desc_parts.append(s)
                desc_text = " | ".join(dict.fromkeys(desc_parts))

                # start/end time
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

                # groups
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
                    'teachers': set(teachers),
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

# ---------------- Maquette loader ----------------

def read_maquette(xls):
    sheet_candidates = [s for s in xls.sheet_names if s.lower() == 'maquette' or 'maquette' in s.lower()]
    if not sheet_candidates:
        return pd.DataFrame(columns=['promo','subject','target'])
    sheet = sheet_candidates[0]
    try:
        mq = pd.read_excel(xls, sheet_name=sheet)
    except Exception:
        return pd.DataFrame(columns=['promo','subject','target'])

    cols = {c.lower(): c for c in mq.columns}
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

    rows = []
    for _, row in mq.iterrows():
        subject = row.get(subject_col)
        if pd.isna(subject): continue
        promo = row.get(promo_col) if promo_col else None
        target = row.get(target_col) if target_col else None
        try:
            tval = float(target) if target is not None and not pd.isna(target) else None
        except Exception:
            tval = None
        promo_str = str(promo).strip() if promo and not pd.isna(promo) else ''
        rows.append({'promo': promo_str, 'subject': str(subject).strip(), 'target': tval})
    return pd.DataFrame(rows, columns=['promo','subject','target'])

# ---------------- Aggregation helpers ----------------

def build_events_index(xls, sheet_names):
    out = {}
    for s in sheet_names:
        try:
            events = parse_sheet_to_events(xls, s)
            out[s] = events
        except Exception:
            out[s] = []
    return out

def sum_hours_by_subject(events):
    totals = {}
    for ev in events:
        if ev['start'] and ev['end']:
            delta = (ev['end'] - ev['start']).total_seconds() / 3600.0
        else:
            delta = 0
        subj = ev['summary']
        totals[subj] = totals.get(subj, 0) + delta
    return totals

def flatten_events_table(events):
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

promo_sheets = [s for s in sheets if s.strip().upper() in ['EDT P1','EDT P2','P1','P2','EDT P1 ','EDT P2 ']]
if not promo_sheets:
    promo_sheets = [s for s in sheets if 'EDT' in s.upper() or 'P1' in s.upper() or 'P2' in s.upper()]

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

# ---------- Page 1 ----------
if page.startswith('1'):
    st.header('Comparaison des heures par matière vs Maquette')
    if maquette_df.empty:
        st.warning('Feuille Maquette non trouvée ou non lisible — seul le total des heures extraites sera affiché.')
    results = []
    for promo, evs in events_by_promo.items():
        totals = sum_hours_by_subject(evs)
        dfp = pd.DataFrame([(k, v) for k, v in totals.items()], columns=['subject','hours'])
        dfp['promo_sheet'] = promo
        if not maquette_df.empty:
            def find_target(subject):
                m = maquette_df[maquette_df['subject'].str.lower().str.strip() == subject.lower().strip()]
                if not m.empty:
                    if 'promo' in maquette_df.columns and m.shape[0] > 1:
                        mm = m[m['promo'].str.contains(promo.split()[-1]) if m['promo'].notna().any() else False]
                        if not mm.empty:
                            return mm['target'].iloc[0]
                    return m['target'].iloc[0]
                m2 = maquette_df[maquette_df['subject'].str.lower().str.contains(subject.split()[0].lower())]
                if not m2.empty:
                    return m2['target'].iloc[0]
                return None
            dfp['target'] = dfp['subject'].apply(find_target)
            dfp['diff'] = dfp.apply(lambda r: (r['hours'] - r['target']) if pd.notna(r['target']) else None, axis=1)
        results.append(dfp)
    if results:
        full = pd.concat(results, ignore_index=True)
        st.dataframe(full.sort_values(['promo_sheet','subject']).reset_index(drop=True))
    else:
        st.info('Aucune séance détectée.')

# ---------- Page 2 ----------
elif page.startswith('2'):
    st.header('Récapitulatif par matière (sélectionne une matière)')
    subjects = set()
    for evs in events_by_promo.values():
        for ev in evs:
            subjects.add(ev['summary'])
    subjects = sorted(list(subjects))
    if not subjects:
        st.warning('Aucune matière trouvée.')
    else:
        chosen = st.selectbox('Choisir une matière', options=subjects)
        st.write(f'Séances pour : **{chosen}**')
        for promo, evs in events_by_promo.items():
            df_ev = flatten_events_table([e for e in evs if e['summary'] == chosen])
            st.subheader(promo)
            if df_ev.empty:
                st.info('Aucune séance pour cette matière.')
            else:
                st.dataframe(df_ev.sort_values('start').reset_index(drop=True))

# ---------- Page 3 ----------
elif page.startswith('3'):
    st.header('Récapitulatif par enseignant')
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

# ---------- Page 4 ----------
else:
    st.header('Récapitulatif textuel complet (par promo)')
    for promo, evs in events_by_promo.items():
        st.subheader(promo)
        if not evs:
            st.info('Aucune séance détectée.')
            continue
        by_subject = {}
        for e in evs:
            by_subject.setdefault(e['summary'], []).append(e)
        for subj in sorted(by_subject.keys()):
            st.markdown(f'**{subj}**')
            lst = sorted(by_subject[subj], key=lambda x: x['start'])
            for e in lst:
                groups = ', '.join(e['groups']) if e['groups'] else '—'
                teachers = ', '.join(e['teachers']) if e['teachers'] else '—'
                desc = e['description'] if e.get('description') else ''
                st.write(
                    f"- {e['start'].strftime('%Y-%m-%d %H:%M')} → {e['end'].strftime('%H:%M')} "
                    f"— Groupes: {groups} — Enseignant(s): {teachers} {('- '+desc) if desc else ''}"
                )
