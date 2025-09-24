import streamlit as st
import pandas as pd
import re
import io
from datetime import datetime, date, time
from dateutil import parser as dtparser
from openpyxl import load_workbook

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

# ---------------- Helpers Fusion ----------------

def get_merged_map_from_bytes(xls_bytes, sheet_name):
    """
    Retourne un dict {(row_zero_based, col_zero_based): (r1,c1,r2,c2)} pour les ranges fusionnées
    basé sur le sheet_name. On lit depuis bytes (io.BytesIO).
    """
    wb = load_workbook(io.BytesIO(xls_bytes), data_only=True)
    ws = wb[sheet_name]
    merged_map = {}
    for merged in ws.merged_cells.ranges:
        r1, r2 = merged.min_row, merged.max_row
        c1, c2 = merged.min_col, merged.max_col
        # convert to zero-based indices
        for r in range(r1, r2 + 1):
            for c in range(c1, c2 + 1):
                merged_map[(r - 1, c - 1)] = (r1 - 1, c1 - 1, r2 - 1, c2 - 1)
    return merged_map

# ---------------- Parsing ----------------

def parse_sheet_to_events(uploaded_file, sheet_name):
    """
    uploaded_file : object retourné par st.file_uploader (file-like)
    sheet_name : nom de la feuille
    """
    # read bytes once and reuse for both pandas and openpyxl
    if hasattr(uploaded_file, 'read'):
        xls_bytes = uploaded_file.read()
        # reset pointer for uploaded_file if needed by streamlit later (UploadedFile cannot be rewound easily),
        # but we've already consumed it; downstream code uses xls (pd.ExcelFile) created below from bytes.
    else:
        # uploaded_file might be a path string
        with open(uploaded_file, 'rb') as f:
            xls_bytes = f.read()

    # read sheet into dataframe
    try:
        df = pd.read_excel(io.BytesIO(xls_bytes), sheet_name=sheet_name, header=None, engine='openpyxl')
    except Exception as e:
        # in case of error, return empty
        return []

    nrows, ncols = df.shape

    # build merged map for this sheet
    try:
        merged_map = get_merged_map_from_bytes(xls_bytes, sheet_name)
    except Exception:
        merged_map = {}

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
                            s_t = str(t).strip()
                            if s_t:
                                teachers.append(s_t)
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
                if (col + 1) < ncols and group_row < nrows:
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
                # ---- Use merged_map to detect real Excel merge between (r,col) and (r,col+1) ----
                if is_left_col:
                    merged_here = merged_map.get((r, col))
                    merged_right = merged_map.get((r, col + 1))
                    # if both cells belong to a same merged range that spans the adjacent column,
                    # treat it as a shared course (G1+G2)
                    if merged_here is not None and merged_right is not None and merged_here == merged_right:
                        # add both group labels if present
                        if gl:
                            groups.add(gl)
                        if gl_next:
                            groups.add(gl_next)
                    else:
                        # not merged across columns: normal behavior (only left group)
                        if gl:
                            groups.add(gl)
                else:
                    # right column event (only add its group)
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
    """
    Lit la feuille Maquette et retourne un DataFrame avec colonnes :
    ['semester', 'ue', 'promo', 'subject', 'target']
    - conserve l'ordre d'apparition dans la feuille (pour l'affichage)
    """
    sheet_candidates = [s for s in xls.sheet_names if s.lower() == 'maquette' or 'maquette' in s.lower()]
    if not sheet_candidates:
        return pd.DataFrame(columns=['semester','ue','promo','subject','target'])
    sheet = sheet_candidates[0]
    try:
        mq = pd.read_excel(xls, sheet_name=sheet)
    except Exception:
        return pd.DataFrame(columns=['semester','ue','promo','subject','target'])

    # normalise noms de colonnes en minuscules pour détecter 'semester' et 'ue'
    cols = {c.lower().strip(): c for c in mq.columns}
    # detect subject column
    subject_col = None
    promo_col = None
    target_col = None
    sem_col = None
    ue_col = None

    for k in cols:
        if any(w in k for w in ['mati', 'subject', 'module', 'ue', 'course']):
            subject_col = cols[k]; break
    for k in cols:
        if any(w in k for w in ['promo','promotion','year','p1','p2']):
            promo_col = cols[k]; break
    for k in cols:
        if any(w in k for w in ['seanc','session','target','cibl','nb','heure','hours']):
            target_col = cols[k]; break
    for k in cols:
        if any(w in k for w in ['sem', 'semestre', 'semester']):
            sem_col = cols[k]; break
    for k in cols:
        if k == 'ue' or 'ue' in k:
            ue_col = cols[k]; break

    # fallback: use first / second column as semester/ue if names not found
    col_list = list(mq.columns)
    if sem_col is None and len(col_list) >= 1:
        # try to guess: if the first col doesn't look like subject, use it for semester
        # but keep safe: only set if different from subject column
        if subject_col is None or col_list[0] != subject_col:
            sem_col = col_list[0]
    if ue_col is None and len(col_list) >= 2:
        if subject_col is None or col_list[1] != subject_col:
            ue_col = col_list[1]

    # if subject not found, try first column that is not semester/ue
    if subject_col is None:
        for c in col_list:
            if c != sem_col and c != ue_col:
                subject_col = c
                break

    if subject_col is None:
        return pd.DataFrame(columns=['semester','ue','promo','subject','target'])

    rows = []
    for _, row in mq.iterrows():
        subject = row.get(subject_col)
        if pd.isna(subject): continue
        promo = row.get(promo_col) if promo_col else None
        target = row.get(target_col) if target_col else None
        sem = row.get(sem_col) if sem_col else None
        ue = row.get(ue_col) if ue_col else None
        try:
            tval = float(target) if target is not None and not pd.isna(target) else None
        except Exception:
            tval = None
        promo_str = str(promo).strip() if promo and not pd.isna(promo) else ''
        rows.append({
            'semester': str(sem).strip() if sem and not pd.isna(sem) else '',
            'ue': str(ue).strip() if ue and not pd.isna(ue) else '',
            'promo': promo_str,
            'subject': str(subject).strip(),
            'target': tval
        })
    # preserve original order (already in rows order)
    return pd.DataFrame(rows, columns=['semester','ue','promo','subject','target'])


def sum_hours_by_subject_and_group(events):
    """
    Retourne dict:
      totals[subject][group] = hours
    group labels are like 'G 1' or 'G 2' or 'G1' or 'G 2' depending de normalize_group_label
    Nous allons normaliser en 'G1','G2' (sans espace) pour facilité.
    """
    totals = {}
    for ev in events:
        if ev['start'] and ev['end']:
            delta = (ev['end'] - ev['start']).total_seconds() / 3600.0
        else:
            delta = 0.0
        subj = ev['summary']
        groups = ev.get('groups') or []
        # si pas de groupe indiqué, on peut associer à 'ALL' (ou ignorer) — ici on met 'ALL'
        if not groups:
            groups = ['ALL']
        for g in groups:
            # normalise 'G 1' -> 'G1'
            g_norm = re.sub(r'\s+', '', g).upper()
            g_norm = g_norm.replace('G.', 'G').replace('GROUP', 'G')
            if subj not in totals:
                totals[subj] = {}
            totals[subj][g_norm] = totals[subj].get(g_norm, 0.0) + delta
    return totals

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
    # create a pandas ExcelFile from uploaded bytes so read_maquette / listing work
    xls_bytes_for_listing = uploaded.read()
    xls = pd.ExcelFile(io.BytesIO(xls_bytes_for_listing), engine='openpyxl')
    sheets = xls.sheet_names
except Exception as e:
    st.error('Impossible de lire le fichier Excel: ' + str(e))
    st.stop()

st.write('Feuilles trouvées :', sheets)

promo_sheets = [s for s in sheets if s.strip().upper() in ['EDT P1','EDT P2','P1','P2','EDT P1 ','EDT P2 ']]
if not promo_sheets:
    promo_sheets = [s for s in sheets if 'EDT' in s.upper() or 'P1' in s.upper() or 'P2' in s.upper()]

promo_sheets = [s for s in ['EDT P1','EDT P2'] if s in sheets] or promo_sheets

# build events using the original uploaded file object — parse_sheet_to_events expects the uploaded file-like,
# but it already consumed uploaded.read() above, so we pass the bytes we saved to parse_sheet_to_events:
class _BytesWrapper:
    def __init__(self, b):
        self._b = b
    def read(self):
        return self._b

uploaded_bytes_wrapper = _BytesWrapper(xls_bytes_for_listing)

events_by_promo = build_events_index(uploaded_bytes_wrapper, promo_sheets)
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

    # choix promo et groupe dans la sidebar
    promo_choice = st.sidebar.selectbox('Promo à comparer', options=['P1', 'P2'])
    group_choice = st.sidebar.selectbox('Groupe à comparer', options=['G1', 'G2'])

    # trouver la feuille correspondant à la promo choisie
    # priorité à 'EDT P1' / 'EDT P2' exact, sinon cherche une feuille contenant 'P1' ou 'P2'
    target_sheet = None
    candidates = [s for s in promo_sheets if promo_choice in s.upper()]
    # prefer exact match 'EDT P1' etc
    if f'EDT {promo_choice}' in sheets:
        target_sheet = f'EDT {promo_choice}'
    elif candidates:
        target_sheet = candidates[0]
    else:
        # fallback: take any sheet that mentions P1/P2
        any_match = [s for s in sheets if promo_choice in s.upper()]
        if any_match:
            target_sheet = any_match[0]

    if not target_sheet:
        st.error(f"Aucune feuille d'emploi du temps trouvée pour {promo_choice}. Feuilles détectées: {sheets}")
        st.stop()

    st.write(f"Feuille utilisée pour la comparaison: **{target_sheet}** — Groupe: **{group_choice}**")

    events = events_by_promo.get(target_sheet, [])
    totals_by_subject = sum_hours_by_subject_and_group(events)

    # Construire le DataFrame final en respectant l'ordre de la maquette
    rows = []
    # filtre maquette pour la promo choisie (maquette peut contenir des lignes sans promo)
    if not maquette_df.empty:
        # we consider a row matches the promo if promo column contains P1/P2 OR empty
        def promo_matches(promo_cell):
            if not promo_cell or str(promo_cell).strip() == '':
                return True
            return promo_choice in str(promo_cell).upper()

        mq_rows = maquette_df[maquette_df['promo'].apply(lambda x: promo_matches(x))]
        # preserve order (already)
        for _, r in mq_rows.iterrows():
            subj = r['subject']
            sem = r.get('semester', '')
            ue = r.get('ue', '')
            target = r.get('target', None)
            # lookup actual hours: subject match try exact lower strip, then contains fallback
            actual = 0.0
            found_key = None
            # attempt direct key match in totals_by_subject
            for s_key in totals_by_subject.keys():
                if s_key.strip().lower() == subj.strip().lower():
                    found_key = s_key; break
            if not found_key:
                # fallback: contains
                for s_key in totals_by_subject.keys():
                    if subj.strip().lower() in s_key.strip().lower() or s_key.strip().lower() in subj.strip().lower():
                        found_key = s_key; break
            if found_key:
                # totals_by_subject[found_key] is dict per group
                gdict = totals_by_subject[found_key]
                # group_choice normalization e.g. 'G1' compare to keys like 'G1' or 'G 1'
                key_norm = group_choice.upper().replace(' ', '')
                actual = gdict.get(key_norm, 0.0) + gdict.get('ALL', 0.0)
            else:
                actual = 0.0

            diff = None
            if target is not None:
                diff = actual - float(target)
            status = 'OK' if (target is not None and actual >= float(target) - 1e-6) else ('MANQUANT' if target is not None else '')
            rows.append({
                'semester': sem,
                'ue': ue,
                'subject': subj,
                'target': target,
                'actual_hours': round(actual, 2),
                'diff': round(diff, 2) if diff is not None else None,
                'status': status
            })

        df_compare = pd.DataFrame(rows, columns=['semester','ue','subject','target','actual_hours','diff','status'])
        st.write('Comparatif (toutes les matières de la Maquette dans l\'ordre)')
        st.dataframe(df_compare)

        # montrer les matières trouvées dans les EDT mais non présentes dans la maquette
        present_subjects = set([k for k in totals_by_subject.keys()])
        maquette_subjects = set([s.strip().lower() for s in maquette_df['subject'].tolist() if s and str(s).strip()])
        extras = []
        for s in present_subjects:
            if s.strip().lower() not in maquette_subjects:
                # report hours for the chosen group
                gdict = totals_by_subject[s]
                actual = gdict.get(group_choice.replace(' ', ''), 0.0) + gdict.get('ALL', 0.0)
                extras.append({'subject': s, 'actual_hours': round(actual,2)})
        if extras:
            st.warning('Matières détectées dans l\'EDT mais absentes de la Maquette : vérifie le nom exact dans la Maquette.')
            st.table(pd.DataFrame(extras))
    else:
        # pas de maquette : afficher juste le total des heures par matière pour le groupe choisi
        rows = []
        for subj, gdict in totals_by_subject.items():
            actual = gdict.get(group_choice.replace(' ', ''), 0.0) + gdict.get('ALL', 0.0)
            rows.append({'subject': subj, 'actual_hours': round(actual,2)})
        if rows:
            st.dataframe(pd.DataFrame(rows).sort_values('subject').reset_index(drop=True))
        else:
            st.info('Aucune séance détectée pour cette feuille/promo.')
            
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
