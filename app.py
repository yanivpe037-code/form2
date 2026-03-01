from flask import Flask, jsonify, render_template, request
import pandas as pd
import numpy as np
import os
import pickle

app = Flask(__name__)

CACHE_FILE = os.path.join(os.path.dirname(__file__), 'form2_cache.pkl')
DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'DC23.xlsx')


def load_data():
    if os.path.exists(CACHE_FILE):
        print("Loading from cache...")
        with open(CACHE_FILE, 'rb') as f:
            form2 = pickle.load(f)
    else:
        print("Loading DC23.xlsx (first time, may take ~60 seconds)...")
        df = pd.read_excel(DATA_FILE)
        form2 = df[df['גליון'] == 'טופס 2'].copy()
        form2['ערך'] = pd.to_numeric(form2['ערך'], errors='coerce').fillna(0)
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(form2, f)
        print("Cache saved. Future loads will be fast.")

    form2['שורה'] = form2['שורה'].astype(str).str.strip()
    return form2


form2 = load_data()
MUNICIPALITIES = sorted(form2['שם_רשות'].dropna().unique().tolist())
print(f"Ready. {len(MUNICIPALITIES)} municipalities loaded.")

_data_years = sorted(form2['שנת_נתונים'].dropna().unique().astype(int).tolist())
YEAR_CUR  = _data_years[-1] if _data_years else 2023
YEAR_PREV = _data_years[-2] if len(_data_years) >= 2 else (YEAR_CUR - 1)
print(f"Data years: current={YEAR_CUR}, previous={YEAR_PREV}")

# ---------------------------------------------------------------------------
# Row structure definitions
# ---------------------------------------------------------------------------
RECEIPTS_STRUCTURE = [
    {'key': None, 'label': 'מיסים ומענקים', 'type': 'section_header', 'num': '1'},
    {'key': 'מיסים',                      'label': 'מיסים',                        'type': 'row',     'num': '11'},
    {'key': 'מיסים אחרים',                'label': 'מיסים אחרים',                  'type': 'sub_row', 'num': 'X11'},
    {'key': 'הכנסות במקום ארנונה',        'label': 'הכנסות במקום ארנונה',          'type': 'sub_row', 'num': '112'},
    {'key': 'אגרות',                      'label': 'אגרות',                        'type': 'row',     'num': '12'},
    {'key': 'הטלים',                      'label': 'הטלים',                        'type': 'row',     'num': '13'},
    {'key': 'מכסות',                      'label': 'מכסות',                        'type': 'row',     'num': '14'},
    {'key': 'הכנסות מימון',               'label': 'הכנסות מימון',                 'type': 'row',     'num': '15'},
    {'key': 'השתתפות מוסדות',             'label': 'השתתפות מוסדות',               'type': 'row',     'num': '16'},
    {'key': 'מענקים כלליים ומיוחדים',     'label': 'מענקים כלליים ומיוחדים',       'type': 'row',     'num': '17'},
    {'key': 'סהכ מיסים ומענקים - תקבולים','label': 'סה"כ מיסים ומענקים',          'type': 'subtotal','num': ''},

    {'key': None, 'label': 'שירותים מקומיים', 'type': 'section_header', 'num': '2'},
    {'key': 'תברואה',                     'label': 'תברואה',                       'type': 'row',     'num': '21'},
    {'key': 'שמירה וביטחון',              'label': 'שמירה וביטחון',                'type': 'row',     'num': '22'},
    {'key': 'תכנון ובנין עיר',            'label': 'תכנון ובנין עיר',              'type': 'row',     'num': '23'},
    {'key': 'נכסים ציבוריים',             'label': 'נכסים ציבוריים',               'type': 'row',     'num': '24'},
    {'key': 'שרותים עירוניים שונים',      'label': 'שירותים עירוניים שונים',       'type': 'row',     'num': '26'},
    {'key': 'פיתוח כלכלי',               'label': 'פיתוח כלכלי',                  'type': 'row',     'num': '27'},
    {'key': 'פיקוח עירוני',               'label': 'פיקוח עירוני',                 'type': 'row',     'num': '28'},
    {'key': 'שירותים חקלאיים',            'label': 'שירותים חקלאיים',              'type': 'row',     'num': '29'},
    {'key': 'סהכ שירותים מקומיים - תקבולים','label': 'סה"כ שירותים מקומיים',      'type': 'subtotal','num': ''},

    {'key': None, 'label': 'שירותים ממלכתיים', 'type': 'section_header', 'num': '3'},
    {'key': 'חינוך',                      'label': 'חינוך',                        'type': 'row',     'num': '31'},
    {'key': 'תרבות',                      'label': 'תרבות',                        'type': 'row',     'num': '32'},
    {'key': 'בריאות',                     'label': 'בריאות',                       'type': 'row',     'num': '33'},
    {'key': 'רווחה',                      'label': 'רווחה',                        'type': 'row',     'num': '34'},
    {'key': 'דת',                         'label': 'דת',                           'type': 'row',     'num': '35'},
    {'key': 'קליטת עלייה',                'label': 'קליטת עלייה',                  'type': 'row',     'num': '36'},
    {'key': 'איכות הסביבה',               'label': 'איכות הסביבה',                 'type': 'row',     'num': '37'},
    {'key': 'סהכ שרותים ממלכתיים - תקבולים','label': 'סה"כ שירותים ממלכתיים',    'type': 'subtotal','num': ''},

    {'key': None, 'label': 'מפעלים', 'type': 'section_header', 'num': '4'},
    {'key': 'מים',                        'label': 'מים',                          'type': 'row',     'num': '41'},
    {'key': 'בתי מטבחיים',                'label': 'בתי מטבחיים',                  'type': 'row',     'num': '42'},
    {'key': 'נכסים',                      'label': 'נכסים',                        'type': 'row',     'num': '43'},
    {'key': 'תחבורה',                     'label': 'תחבורה',                       'type': 'row',     'num': '44'},
    {'key': 'מפעלי תעסוקה',               'label': 'מפעלי תעסוקה',                 'type': 'row',     'num': '45'},
    {'key': 'חשמל',                       'label': 'חשמל',                         'type': 'row',     'num': '46'},
    {'key': 'מפעל הביוב',                 'label': 'מפעל הביוב',                   'type': 'row',     'num': '47'},
    {'key': 'מפעלים אחרים',               'label': 'מפעלים אחרים',                 'type': 'row',     'num': '48'},
    {'key': 'סהכ מפעלים - תקבולים',       'label': 'סה"כ מפעלים',                  'type': 'subtotal','num': ''},

    {'key': 'תקבולים בלתי רגילים',        'label': 'תקבולים בלתי רגילים',          'type': 'irregular','num': '5'},
    {'key': '__grand_total__',            'label': 'סה"כ תקבולים',                 'type': 'grand_total','num': ''},
]

PAYMENTS_STRUCTURE = [
    {'key': None, 'label': 'הנהלה וכלליות', 'type': 'section_header', 'num': '6'},
    {'key': 'מנהל כללי',                  'label': 'מנהל כללי',                    'type': 'row',     'num': '61'},
    {'key': 'מנהל כספי',                  'label': 'מנהל כספי',                    'type': 'row',     'num': '62'},
    {'key': 'הוצאות מימון',               'label': 'הוצאות מימון',                 'type': 'row',     'num': '63'},
    {'key': 'פרעון מלוות - למעט ביוב',    'label': 'פרעון מלוות',                  'type': 'row',     'num': '64'},
    {'key': 'סהכ הנהלה וכלליות',          'label': 'סה"כ הנהלה וכלליות',           'type': 'subtotal','num': ''},

    {'key': None, 'label': 'שירותים מקומיים', 'type': 'section_header', 'num': '7'},
    {'key': 'תברואה',                     'label': 'תברואה',                       'type': 'row',     'num': '71'},
    {'key': 'שמירה וביטחון',              'label': 'שמירה וביטחון',                'type': 'row',     'num': '72'},
    {'key': 'תכנון ובנין עיר',            'label': 'תכנון ובנין עיר',              'type': 'row',     'num': '73'},
    {'key': 'נכסים ציבוריים',             'label': 'נכסים ציבוריים',               'type': 'row',     'num': '74'},
    {'key': 'חגיגות',                     'label': 'חגיגות, מבצעים ואירועים',      'type': 'row',     'num': '75'},
    {'key': 'שרותים עירוניים שונים',      'label': 'שירותים עירוניים שונים',       'type': 'row',     'num': '76'},
    {'key': 'פיתוח כלכלי',               'label': 'פיתוח כלכלי',                  'type': 'row',     'num': '77'},
    {'key': 'פיקוח עירוני',               'label': 'פיקוח עירוני',                 'type': 'row',     'num': '78'},
    {'key': 'שירותים חקלאיים',            'label': 'שירותים חקלאיים',              'type': 'row',     'num': '79'},
    {'key': 'סהכ שירותים מקומיים - תשלומים','label': 'סה"כ שירותים מקומיים',      'type': 'subtotal','num': ''},

    {'key': None, 'label': 'שירותים ממלכתיים', 'type': 'section_header', 'num': '8'},
    {'key': 'חינוך',                      'label': 'חינוך',                        'type': 'row',     'num': '81'},
    {'key': 'תרבות',                      'label': 'תרבות',                        'type': 'row',     'num': '82'},
    {'key': 'בריאות',                     'label': 'בריאות',                       'type': 'row',     'num': '83'},
    {'key': 'רווחה',                      'label': 'רווחה',                        'type': 'row',     'num': '84'},
    {'key': 'דת',                         'label': 'דת',                           'type': 'row',     'num': '85'},
    {'key': 'קליטת עלייה',                'label': 'קליטת עלייה',                  'type': 'row',     'num': '86'},
    {'key': 'איכות הסביבה',               'label': 'איכות הסביבה',                 'type': 'row',     'num': '87'},
    {'key': 'סהכ שירותים ממלכתיים - תשלומים','label': 'סה"כ שירותים ממלכתיים',    'type': 'subtotal','num': ''},

    {'key': None, 'label': 'מפעלים', 'type': 'section_header', 'num': '9'},
    {'key': 'מים',                        'label': 'מים',                          'type': 'row',     'num': '91'},
    {'key': 'בתי מטבחיים',                'label': 'בתי מטבחיים',                  'type': 'row',     'num': '92'},
    {'key': 'נכסים',                      'label': 'נכסים',                        'type': 'row',     'num': '93'},
    {'key': 'תחבורה',                     'label': 'תחבורה',                       'type': 'row',     'num': '94'},
    {'key': 'מפעלי תעסוקה',               'label': 'מפעלי תעסוקה',                 'type': 'row',     'num': '95'},
    {'key': 'חשמל',                       'label': 'חשמל',                         'type': 'row',     'num': '96'},
    {'key': 'מפעל הביוב כולל פרעון מלוות','label': 'מפעל הביוב (כולל פרעון)',     'type': 'row',     'num': '97'},
    {'key': 'מפעלים אחרים',               'label': 'מפעלים אחרים',                 'type': 'row',     'num': '98'},
    {'key': 'סהכ מפעלים - תשלומים',       'label': 'סה"כ מפעלים',                  'type': 'subtotal','num': ''},

    {'key': 'תשלומים בלתי רגילים',        'label': 'תשלומים בלתי רגילים',          'type': 'irregular','num': '99'},
    {'key': '__grand_total__',            'label': 'סה"כ תשלומים',                 'type': 'grand_total','num': ''},
]

RECEIPTS_SUBTOTALS = [
    'סהכ מיסים ומענקים - תקבולים',
    'סהכ שירותים מקומיים - תקבולים',
    'סהכ שרותים ממלכתיים - תקבולים',
    'סהכ מפעלים - תקבולים',
    'תקבולים בלתי רגילים',
]
PAYMENTS_SUBTOTALS = [
    'סהכ הנהלה וכלליות',
    'סהכ שירותים מקומיים - תשלומים',
    'סהכ שירותים ממלכתיים - תשלומים',
    'סהכ מפעלים - תשלומים',
    'תשלומים בלתי רגילים',
]


# ---------------------------------------------------------------------------
# Cross-municipality statistics for Form 2 (precomputed at startup)
# ---------------------------------------------------------------------------
def precompute_stats():
    COLS_CUR = {
        'rec_cur': 'תקבולים - ביצוע - שנה נוכחית',
        'pay_cur': 'תשלומים - ביצוע - שנה נוכחית',
    }
    stats = {}
    for col_key, col_name in COLS_CUR.items():
        col_data = form2[form2['עמודה'] == col_name]
        if col_data.empty:
            stats[col_key] = {}
            continue
        pivot = col_data.pivot_table(
            index='שם_רשות', columns='שורה', values='ערך',
            aggfunc='sum', fill_value=0
        )
        subtotals = RECEIPTS_SUBTOTALS if col_key == 'rec_cur' else PAYMENTS_SUBTOTALS
        avail = [s for s in subtotals if s in pivot.columns]
        grand_totals = pivot[avail].sum(axis=1).replace(0, np.nan)
        pct_pivot = pivot.div(grand_totals, axis=0).mul(100)

        structure = RECEIPTS_STRUCTURE if col_key == 'rec_cur' else PAYMENTS_STRUCTURE
        col_stats = {}
        for item in structure:
            rk = item.get('key')
            if not rk or rk.startswith('__'):
                continue
            pct_s = pct_pivot[rk].fillna(0) if rk in pct_pivot.columns else pd.Series(0.0, index=pivot.index)
            col_stats[rk] = {
                'avg_pct':    round(float(pct_s.mean()),   1),
                'median_pct': round(float(pct_s.median()), 1),
                'muni_pcts':  pct_s.to_dict(),
            }
        stats[col_key] = col_stats
    return stats


def compute_rank(muni_pcts_dict, municipality):
    muni_pct = muni_pcts_dict.get(municipality, 0.0)
    return int(sum(1 for p in muni_pcts_dict.values() if p > muni_pct) + 1)


row_stats = precompute_stats()
print("Cross-municipality stats precomputed.")


# ---------------------------------------------------------------------------
# Extra sheets (generic display) — 23 sheets with categorical dtypes
# ---------------------------------------------------------------------------
EXTRA_SHEETS_CACHE = os.path.join(os.path.dirname(__file__), 'extra_sheets_cache.pkl')
extra_sheets_data = None

DISPLAY_SHEETS = [
    # ── טפסים ראשיים ──────────────────────────────────────────
    {'key': 'form2',                    'label': 'טופס 2 – תקבולים ותשלומים',       'group': 'טפסים'},
    {'key': 'דוח לתושב',               'label': 'דוח לתושב',                        'group': 'טפסים'},
    {'key': 'ספר לבן',                 'label': 'ספר לבן',                           'group': 'טפסים'},
    {'key': 'טופס 1 אקטיב',            'label': 'מאזן – נכסים (טופס 1א)',            'group': 'טפסים'},
    {'key': 'טופס 1 פאסיב',            'label': 'מאזן – התחייבויות (טופס 1פ)',       'group': 'טפסים'},
    {'key': 'טופס 3',                  'label': 'טופס 3 – מלוות',                    'group': 'טפסים'},
    {'key': 'טופס 4',                  'label': 'טופס 4 – תקציב פיתוח',              'group': 'טפסים'},
    {'key': 'נתונים כלליים',           'label': 'נתונים כלליים',                     'group': 'טפסים'},
    # ── נספחים לטופס 2 ────────────────────────────────────────
    {'key': 'נספח 1 לטופס 2',         'label': 'נספח 1 לטופס 2',                    'group': 'נספחים ט׳׳2'},
    {'key': 'נספח 1 לטופס 2 המשך',    'label': 'נספח 1 לטופס 2 (המשך)',              'group': 'נספחים ט׳׳2'},
    {'key': 'נספח 2 לטופס 2',         'label': 'נספח 2 – הוצאות פירוט',             'group': 'נספחים ט׳׳2'},
    {'key': 'נספח 3 לטופס 2',         'label': 'נספח 3 – הכנסות פירוט',             'group': 'נספחים ט׳׳2'},
    {'key': 'נספח 4 לטופס 2',         'label': 'נספח 4 – מצבת כוח אדם',             'group': 'נספחים ט׳׳2'},
    {'key': 'נספח 5 לטופס 2',         'label': 'נספח 5 – שכר',                      'group': 'נספחים ט׳׳2'},
    {'key': 'נספח 6 לטופס 2',         'label': 'נספח 6 לטופס 2',                    'group': 'נספחים ט׳׳2'},
    # ── נספחים לטופס 1 ────────────────────────────────────────
    {'key': 'נספח 2 לטופס 1',         'label': 'נספח 2 לטופס 1 – ארנונה',           'group': 'נספחים ט׳׳1'},
    {'key': 'נספח 3 לטופס 1',         'label': 'נספח 3 לטופס 1 – הלוואות',          'group': 'נספחים ט׳׳1'},
    # ── נספחים לטופס 3 ────────────────────────────────────────
    {'key': 'נספח 1 לטופס 3',         'label': 'נספח 1 לטופס 3 – פירוט מלוות',      'group': 'נספחים ט׳׳3'},
    # ── ביאורים ───────────────────────────────────────────────
    {'key': 'ביאור 3',                 'label': 'ביאור 3',                            'group': 'ביאורים'},
    {'key': 'ביאור 4',                 'label': 'ביאור 4',                            'group': 'ביאורים'},
    {'key': 'ביאור 5',                 'label': 'ביאור 5',                            'group': 'ביאורים'},
    # ── נספחים נוספים ─────────────────────────────────────────
    {'key': 'נספח א',                  'label': 'נספח א – ארנונה',                    'group': 'נספחים נוספים'},
    {'key': 'נספח ה',                  'label': 'נספח ה – סטיות תקציב',               'group': 'נספחים נוספים'},
    {'key': 'דוח תמיכות',              'label': 'דוח תמיכות',                         'group': 'נספחים נוספים'},
]

PREFERRED_COL_ORDER = [
    'תקציב שנה נוכחית',
    'ביצוע שנה נוכחית',
    'שנה נוכחית',
    'תקציב שנה קודמת',
    'ביצוע שנה קודמת',
    'שנה קודמת',
    'אחוז ביצוע מסהכ שנה נוכחית',
    'אחוז ביצוע מסהכ שנה קודמת',
]

# Keywords that identify "primary" value columns worth ranking
RANK_COL_KEYWORDS = [
    'ביצוע שנה נוכחית',
    'שנה נוכחית',
    'ביצוע - שנה נוכחית',
    'תקציב שנה נוכחית',
]


def load_extra_sheets():
    global extra_sheets_data
    if os.path.exists(EXTRA_SHEETS_CACHE):
        print("Loading extra sheets cache...")
        with open(EXTRA_SHEETS_CACHE, 'rb') as f:
            df = pickle.load(f)
        df['שורה'] = df['שורה'].astype(str).str.strip()
        extra_sheets_data = df
        sheets_n = df['גליון'].nunique()
        print(f"Extra sheets loaded: {len(df):,} rows, {sheets_n} sheets")
    else:
        print("Warning: extra_sheets_cache.pkl not found. Extra sheets unavailable.")
        extra_sheets_data = None


load_extra_sheets()


# ---------------------------------------------------------------------------
# Generic cross-municipality stats (precomputed at startup)
# ---------------------------------------------------------------------------
generic_stats = {}   # {sheet: {col: {row_label: {p25,p50,p75,n,muni_pctrank}}}}


def precompute_generic_stats():
    """For each sheet × primary column × row label, compute percentile distribution
    across all 258 municipalities. Uses most-recent שנת_נתונים only."""
    if extra_sheets_data is None:
        return {}

    print("Precomputing generic sheet statistics...")
    result = {}

    sheets = extra_sheets_data['גליון'].cat.categories.tolist() \
        if hasattr(extra_sheets_data['גליון'], 'cat') \
        else extra_sheets_data['גליון'].unique().tolist()

    for sheet in sheets:
        sheet_df = extra_sheets_data[extra_sheets_data['גליון'] == sheet]
        max_year = int(sheet_df['שנת_נתונים'].max())
        cur_df = sheet_df[sheet_df['שנת_נתונים'] == max_year]

        avail_cols = cur_df['עמודה'].dropna().unique().tolist()
        primary = [c for c in avail_cols
                   if any(kw in str(c) for kw in RANK_COL_KEYWORDS)
                   and 'אחוז' not in str(c)]
        if not primary and avail_cols:
            primary = avail_cols[:1]

        result[sheet] = {}
        for col in primary:
            col_df = cur_df[cur_df['עמודה'] == col]
            try:
                pivot = col_df.pivot_table(
                    index='שם_רשות', columns='שורה',
                    values='ערך', aggfunc='sum'
                )
            except Exception:
                continue

            result[sheet][col] = {}
            for row_label in pivot.columns:
                series = pivot[row_label].dropna()
                if len(series) < 10:
                    continue
                pct_ranks = series.rank(ascending=True, pct=True).mul(100).round(1)
                result[sheet][col][str(row_label)] = {
                    'p25': round(float(series.quantile(0.25)), 1),
                    'p50': round(float(series.quantile(0.50)), 1),
                    'p75': round(float(series.quantile(0.75)), 1),
                    'n':   int(len(series)),
                    'muni_pctrank': {str(m): float(r) for m, r in pct_ranks.items()},
                }

    print(f"Generic stats done: {len(result)} sheets indexed.")
    return result


generic_stats = precompute_generic_stats()


# ---------------------------------------------------------------------------
# Analyst KPIs precomputation (Form 2 based)
# ---------------------------------------------------------------------------
analyst_kpis = None   # DataFrame, index = municipality


def precompute_analyst_kpis():
    """Compute financial health KPIs for every municipality from Form 2 data."""
    print("Precomputing analyst KPIs...")

    def grand_total(col_name, subtotals):
        sub = form2[form2['עמודה'] == col_name]
        if sub.empty:
            return pd.Series(dtype=float)
        piv = sub.pivot_table(
            index='שם_רשות', columns='שורה',
            values='ערך', aggfunc='sum', fill_value=0
        )
        avail = [s for s in subtotals if s in piv.columns]
        return piv[avail].sum(axis=1)

    def row_values(col_name, row_name):
        sub = form2[(form2['עמודה'] == col_name) & (form2['שורה'] == row_name)]
        return sub.groupby('שם_רשות')['ערך'].sum()

    rec_cur  = grand_total('תקבולים - ביצוע - שנה נוכחית',  RECEIPTS_SUBTOTALS)
    rec_prev = grand_total('תקבולים - ביצוע - שנה קודמת',   RECEIPTS_SUBTOTALS)
    pay_cur  = grand_total('תשלומים - ביצוע - שנה נוכחית',  PAYMENTS_SUBTOTALS)
    pay_bud  = grand_total('תשלומים - תקציב - שנה נוכחית',  PAYMENTS_SUBTOTALS)
    rec_bud  = grand_total('תקבולים - תקציב - שנה נוכחית',  RECEIPTS_SUBTOTALS)

    gov_grants = row_values('תקבולים - ביצוע - שנה נוכחית', 'מענקים כלליים ומיוחדים')
    fin_cost   = row_values('תשלומים - ביצוע - שנה נוכחית', 'הוצאות מימון')
    loan_rep   = row_values('תשלומים - ביצוע - שנה נוכחית', 'פרעון מלוות - למעט ביוב')
    admin_cost = row_values('תשלומים - ביצוע - שנה נוכחית', 'סהכ הנהלה וכלליות')
    edu_pay    = row_values('תשלומים - ביצוע - שנה נוכחית', 'חינוך')
    welfare    = row_values('תשלומים - ביצוע - שנה נוכחית', 'רווחה')
    sanitation = row_values('תשלומים - ביצוע - שנה נוכחית', 'תברואה')

    df = pd.DataFrame({
        'rec_cur':  rec_cur,
        'rec_prev': rec_prev,
        'pay_cur':  pay_cur,
        'pay_bud':  pay_bud,
        'rec_bud':  rec_bud,
    }).fillna(0)

    safe = lambda s: s.reindex(df.index).fillna(0)

    df['surplus']           = df['rec_cur'] - df['pay_cur']
    df['surplus_pct']       = (df['surplus']       / df['rec_cur'].replace(0, np.nan) * 100)
    df['budget_exec_rate']  = (df['pay_cur']        / df['pay_bud'].replace(0, np.nan) * 100)
    df['gov_dep_pct']       = (safe(gov_grants)     / df['rec_cur'].replace(0, np.nan) * 100)
    df['fin_cost_pct']      = (safe(fin_cost)       / df['pay_cur'].replace(0, np.nan) * 100)
    df['loan_rep_pct']      = (safe(loan_rep)       / df['pay_cur'].replace(0, np.nan) * 100)
    df['admin_pct']         = (safe(admin_cost)     / df['pay_cur'].replace(0, np.nan) * 100)
    df['edu_pct']           = (safe(edu_pay)        / df['pay_cur'].replace(0, np.nan) * 100)
    df['welfare_pct']       = (safe(welfare)        / df['pay_cur'].replace(0, np.nan) * 100)
    df['sanitation_pct']    = (safe(sanitation)     / df['pay_cur'].replace(0, np.nan) * 100)
    df['growth_pct']        = ((df['rec_cur'] - df['rec_prev']) / df['rec_prev'].replace(0, np.nan) * 100)

    # Percentile ranks (ascending: higher value = higher percentile)
    kpi_cols = [
        'surplus_pct', 'budget_exec_rate', 'gov_dep_pct',
        'fin_cost_pct', 'loan_rep_pct', 'admin_pct',
        'edu_pct', 'welfare_pct', 'sanitation_pct', 'growth_pct',
    ]
    for col in kpi_cols:
        df[f'{col}_pctrank'] = df[col].rank(ascending=True, pct=True).mul(100).round(1)

    print(f"Analyst KPIs ready: {len(df)} municipalities.")
    return df.round(2)


analyst_kpis = precompute_analyst_kpis()


# ---------------------------------------------------------------------------
# Red flag rules
# ---------------------------------------------------------------------------
RED_FLAG_RULES = [
    {
        'id': 'deficit',
        'category': 'גרעון תפעולי',
        'condition': lambda k: k.get('surplus', 0) < 0,
        'severity': 'critical',
        'title': 'גרעון תפעולי שוטף',
        'detail': lambda k: (
            f"גרעון של {abs(k['surplus']):,.0f} אלפי ש\"כ "
            f"({abs(k.get('surplus_pct', 0)):.1f}% מסה\"כ תקבולים) | "
            f"דירוג: {k.get('surplus_pct_pctrank', 0):.0f}% מהרשויות"
        ),
    },
    {
        'id': 'low_surplus',
        'category': 'גרעון תפעולי',
        'condition': lambda k: 0 <= k.get('surplus_pct', 99) < 2.0,
        'severity': 'warning',
        'title': 'עודף תפעולי נמוך מאוד',
        'detail': lambda k: (
            f"עודף של {k.get('surplus_pct', 0):.1f}% בלבד — בסף הסיכון | "
            f"חציון ארצי: ~9%"
        ),
    },
    {
        'id': 'gov_dep_critical',
        'category': 'תלות ממשלתית',
        'condition': lambda k: k.get('gov_dep_pct', 0) > 30,
        'severity': 'critical',
        'title': 'תלות קריטית בתקצוב ממשלתי',
        'detail': lambda k: (
            f"מענקים כלליים: {k.get('gov_dep_pct', 0):.1f}% מסה\"כ תקבולים | "
            f"P90 ארצי: 25.9% | דירוג: {k.get('gov_dep_pct_pctrank', 0):.0f}%"
        ),
    },
    {
        'id': 'gov_dep_warning',
        'category': 'תלות ממשלתית',
        'condition': lambda k: 20 < k.get('gov_dep_pct', 0) <= 30,
        'severity': 'warning',
        'title': 'תלות גבוהה בתקצוב ממשלתי',
        'detail': lambda k: (
            f"מענקים כלליים: {k.get('gov_dep_pct', 0):.1f}% | חציון ארצי: 12.3%"
        ),
    },
    {
        'id': 'budget_overrun',
        'category': 'ביצוע תקציב',
        'condition': lambda k: k.get('budget_exec_rate', 100) > 115,
        'severity': 'warning',
        'title': 'חריגה מהותית מהתקציב',
        'detail': lambda k: (
            f"ביצוע תשלומים: {k.get('budget_exec_rate', 0):.1f}% מהתקציב | "
            f"חציון ארצי: 103%"
        ),
    },
    {
        'id': 'budget_low',
        'category': 'ביצוע תקציב',
        'condition': lambda k: k.get('budget_exec_rate', 100) < 80,
        'severity': 'warning',
        'title': 'ביצוע תקציב נמוך מאוד',
        'detail': lambda k: (
            f"ביצוע תשלומים: {k.get('budget_exec_rate', 0):.1f}% בלבד מהתקציב | "
            f"עשוי להצביע על תכנון לקוי"
        ),
    },
    {
        'id': 'admin_critical',
        'category': 'עלויות הנהלה',
        'condition': lambda k: k.get('admin_pct', 0) > 20,
        'severity': 'critical',
        'title': 'נטל הנהלה קריטי',
        'detail': lambda k: (
            f"עלויות הנהלה: {k.get('admin_pct', 0):.1f}% מסה\"כ תשלומים | "
            f"P90 ארצי: 14.3% | דירוג: {k.get('admin_pct_pctrank', 0):.0f}%"
        ),
    },
    {
        'id': 'admin_warning',
        'category': 'עלויות הנהלה',
        'condition': lambda k: 14 < k.get('admin_pct', 0) <= 20,
        'severity': 'warning',
        'title': 'עלויות הנהלה גבוהות',
        'detail': lambda k: (
            f"הנהלה: {k.get('admin_pct', 0):.1f}% | חציון ארצי: 9.7% | "
            f"P75: 12.1%"
        ),
    },
    {
        'id': 'high_fin_cost',
        'category': 'נטל חוב',
        'condition': lambda k: k.get('fin_cost_pct', 0) > 1.5,
        'severity': 'warning',
        'title': 'עלויות מימון גבוהות',
        'detail': lambda k: (
            f"הוצאות מימון: {k.get('fin_cost_pct', 0):.1f}% מסה\"כ תשלומים | "
            f"P90 ארצי: 0.9%"
        ),
    },
    {
        'id': 'high_loan_rep',
        'category': 'נטל חוב',
        'condition': lambda k: k.get('loan_rep_pct', 0) > 5,
        'severity': 'warning',
        'title': 'פרעון מלוות גבוה',
        'detail': lambda k: (
            f"פרעון מלוות: {k.get('loan_rep_pct', 0):.1f}% מסה\"כ תשלומים | "
            f"P90 ארצי: 4.2%"
        ),
    },
    {
        'id': 'negative_growth',
        'category': 'מגמת הכנסות',
        'condition': lambda k: k.get('growth_pct', 0) < -5,
        'severity': 'warning',
        'title': 'ירידה משמעותית בהכנסות',
        'detail': lambda k: (
            f"שינוי בתקבולים: {k.get('growth_pct', 0):.1f}% לעומת שנה קודמת"
        ),
    },
    {
        'id': 'low_edu',
        'category': 'שירותים חברתיים',
        'condition': lambda k: 0 < k.get('edu_pct', 99) < 5,
        'severity': 'note',
        'title': 'הוצאה נמוכה על חינוך',
        'detail': lambda k: (
            f"חינוך: {k.get('edu_pct', 0):.1f}% מסה\"כ תשלומים | "
            f"חציון ארצי: ~18%"
        ),
    },
]


def evaluate_red_flags(kpi_dict):
    severity_order = {'critical': 0, 'warning': 1, 'note': 2}
    flags = []
    for rule in RED_FLAG_RULES:
        try:
            if rule['condition'](kpi_dict):
                flags.append({
                    'id':       rule['id'],
                    'category': rule['category'],
                    'severity': rule['severity'],
                    'title':    rule['title'],
                    'detail':   rule['detail'](kpi_dict),
                })
        except Exception:
            pass
    flags.sort(key=lambda f: severity_order.get(f['severity'], 9))
    return flags


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/municipalities')
def get_municipalities():
    return jsonify(MUNICIPALITIES)


@app.route('/api/form2/<path:municipality>')
def get_form2_data(municipality):
    muni_data = form2[form2['שם_רשות'] == municipality]

    COL_NAMES = {
        'rec_cur':  'תקבולים - ביצוע - שנה נוכחית',
        'rec_prev': 'תקבולים - ביצוע - שנה קודמת',
        'pay_cur':  'תשלומים - ביצוע - שנה נוכחית',
        'pay_prev': 'תשלומים - ביצוע - שנה קודמת',
    }

    data = {}
    for key, col_name in COL_NAMES.items():
        col_data = muni_data[muni_data['עמודה'] == col_name]
        data[key] = {str(r['שורה']): float(r['ערך']) for _, r in col_data.iterrows()}

    totals = {
        'rec_cur':  sum(data['rec_cur'].get(k, 0)  for k in RECEIPTS_SUBTOTALS),
        'rec_prev': sum(data['rec_prev'].get(k, 0) for k in RECEIPTS_SUBTOTALS),
        'pay_cur':  sum(data['pay_cur'].get(k, 0)  for k in PAYMENTS_SUBTOTALS),
        'pay_prev': sum(data['pay_prev'].get(k, 0) for k in PAYMENTS_SUBTOTALS),
    }

    def build_rows(structure, side):
        rows = []
        total_cur  = totals['rec_cur']  if side == 'receipts' else totals['pay_cur']
        total_prev = totals['rec_prev'] if side == 'receipts' else totals['pay_prev']
        cur_key    = 'rec_cur'          if side == 'receipts' else 'pay_cur'
        prev_key   = 'rec_prev'         if side == 'receipts' else 'pay_prev'
        stats_map  = row_stats.get(cur_key, {})

        for item in structure:
            row = {'label': item['label'], 'type': item['type'], 'num': item.get('num', '')}
            row['key'] = item.get('key')
            if item['type'] in ('row', 'sub_row', 'subtotal', 'irregular'):
                key = item['key']
                cur_val  = data[cur_key].get(key, 0)
                prev_val = data[prev_key].get(key, 0)
                row['cur_val']    = cur_val
                row['prev_val']   = prev_val
                row['cur_pct']    = round(cur_val  / total_cur  * 100, 1) if total_cur  else None
                row['prev_pct']   = round(prev_val / total_prev * 100, 1) if total_prev else None
                st = stats_map.get(key, {})
                row['avg_pct']    = st.get('avg_pct')
                row['median_pct'] = st.get('median_pct')
                muni_pcts = st.get('muni_pcts', {})
                row['rank']       = compute_rank(muni_pcts, municipality) if muni_pcts else None
                row['n_munis']    = len(muni_pcts)
            elif item['type'] == 'grand_total':
                row['cur_val']    = total_cur
                row['prev_val']   = total_prev
                row['cur_pct']    = 100.0
                row['prev_pct']   = 100.0
                row['avg_pct']    = None
                row['median_pct'] = None
                row['rank']       = None
                row['n_munis']    = None
            rows.append(row)
        return rows

    rec_cur  = totals['rec_cur']
    rec_prev = totals['rec_prev']
    pay_cur  = totals['pay_cur']
    pay_prev = totals['pay_prev']

    return jsonify({
        'municipality': municipality,
        'year_cur':     YEAR_CUR,
        'year_prev':    YEAR_PREV,
        'receipts':     build_rows(RECEIPTS_STRUCTURE, 'receipts'),
        'payments':     build_rows(PAYMENTS_STRUCTURE, 'payments'),
        'balance': {
            'rec_cur':      rec_cur,
            'rec_prev':     rec_prev,
            'pay_cur':      pay_cur,
            'pay_prev':     pay_prev,
            'surplus_cur':  rec_cur  - pay_cur,
            'surplus_prev': rec_prev - pay_prev,
        },
    })


@app.route('/api/topbottom/<side>/<path:row_key>')
def get_topbottom(side, row_key):
    side_key  = 'rec_cur' if side == 'receipts' else 'pay_cur'
    stats_map = row_stats.get(side_key, {})
    st        = stats_map.get(row_key, {})
    muni_pcts = st.get('muni_pcts', {})

    if not muni_pcts:
        return jsonify({'row_key': row_key, 'top5': [], 'bottom5': [], 'total': 0, 'n_zero': 0})

    all_pairs  = [(m, round(float(p), 1)) for m, p in muni_pcts.items()]
    zeros      = sorted([m for m, p in all_pairs if p == 0])
    sorted_desc = sorted(all_pairs, key=lambda x: -x[1])
    top5        = [{'muni': m, 'pct': p} for m, p in sorted_desc[:5]]
    non_zero    = [(m, p) for m, p in all_pairs if p > 0]
    sorted_asc  = sorted(non_zero, key=lambda x: x[1])
    bottom5     = [{'muni': m, 'pct': p} for m, p in sorted_asc[:5]]

    return jsonify({'row_key': row_key, 'top5': top5, 'bottom5': bottom5,
                    'total': len(all_pairs), 'n_zero': len(zeros), 'zeros': zeros})


@app.route('/api/sheets')
def get_sheets():
    return jsonify(DISPLAY_SHEETS)


@app.route('/api/sheet_data')
def get_sheet_data():
    sheet_name   = request.args.get('sheet', '')
    municipality = request.args.get('municipality', '')
    analyst_mode = request.args.get('analyst', '0') == '1'

    if not sheet_name or not municipality:
        return jsonify({'error': 'Missing parameters', 'rows': [], 'columns': []})

    if extra_sheets_data is None:
        return jsonify({'error': 'Extra sheets not available', 'rows': [], 'columns': [],
                        'year_cur': YEAR_CUR, 'year_prev': YEAR_PREV})

    muni_df = extra_sheets_data[
        (extra_sheets_data['גליון'] == sheet_name) &
        (extra_sheets_data['שם_רשות'] == municipality)
    ]

    if muni_df.empty:
        return jsonify({'rows': [], 'columns': [], 'year_cur': YEAR_CUR, 'year_prev': YEAR_PREV,
                        'municipality': municipality, 'sheet': sheet_name})

    all_cols = muni_df['עמודה'].dropna().unique().tolist()
    ordered_cols = [c for c in PREFERRED_COL_ORDER if c in all_cols]
    ordered_cols += [c for c in all_cols if c not in ordered_cols]

    lookup = {}
    for _, r in muni_df.iterrows():
        key = (str(r['שורה']).strip(), str(r['עמודה']))
        v = r['ערך']
        lookup[key] = float(v) if pd.notna(v) else None

    row_order = (muni_df[['קוד', 'שורה']]
                 .drop_duplicates()
                 .sort_values('קוד')
                 .reset_index(drop=True))

    sheet_gs = generic_stats.get(sheet_name, {}) if analyst_mode else {}

    rows = []
    seen = set()
    for _, rr in row_order.iterrows():
        label = str(rr['שורה']).strip()
        if label in seen:
            continue
        seen.add(label)
        row = {
            'label': label,
            'code':  float(rr['קוד']) if pd.notna(rr['קוד']) else None,
        }
        for col in ordered_cols:
            row[col] = lookup.get((label, col))

        # Inject percentile ranks for analyst mode
        if analyst_mode and sheet_gs:
            row['_ranks'] = {}
            for col in ordered_cols:
                col_stats = sheet_gs.get(col, {})
                label_stats = col_stats.get(label)
                if label_stats and municipality in label_stats.get('muni_pctrank', {}):
                    row['_ranks'][col] = {
                        'pctrank': label_stats['muni_pctrank'][municipality],
                        'p25':     label_stats['p25'],
                        'p50':     label_stats['p50'],
                        'p75':     label_stats['p75'],
                        'n':       label_stats['n'],
                    }

        rows.append(row)

    return jsonify({
        'rows':         rows,
        'columns':      ordered_cols,
        'year_cur':     YEAR_CUR,
        'year_prev':    YEAR_PREV,
        'municipality': municipality,
        'sheet':        sheet_name,
        'analyst_mode': analyst_mode,
    })


@app.route('/api/analyst/<path:municipality>')
def get_analyst(municipality):
    """Comprehensive financial health analysis vs all 258 municipalities."""
    if analyst_kpis is None or municipality not in analyst_kpis.index:
        return jsonify({'error': 'Municipality not found or data unavailable'}), 404

    row = analyst_kpis.loc[municipality].to_dict()
    row['municipality'] = municipality
    row['year_cur']     = YEAR_CUR
    row['year_prev']    = YEAR_PREV
    row['n_munis']      = len(analyst_kpis)

    flags = evaluate_red_flags(row)

    # National medians (pre-computed from actual data)
    MEDIANS = {
        'surplus_pct':      9.1,
        'budget_exec_rate': 103.1,
        'gov_dep_pct':      12.4,
        'fin_cost_pct':     0.4,
        'loan_rep_pct':     2.2,
        'admin_pct':        9.7,
        'edu_pct':          18.0,
        'welfare_pct':      8.5,
        'sanitation_pct':   7.2,
        'growth_pct':       5.0,
    }

    kpi_cards = [
        {
            'id':               'surplus_pct',
            'label':            'עודף / גרעון תפעולי',
            'value':            round(row.get('surplus_pct', 0), 1),
            'unit':             '%',
            'pctrank':          row.get('surplus_pct_pctrank'),
            'p50':              MEDIANS['surplus_pct'],
            'higher_is_better': True,
        },
        {
            'id':               'budget_exec_rate',
            'label':            'שיעור ביצוע תקציב',
            'value':            round(row.get('budget_exec_rate', 0), 1),
            'unit':             '%',
            'pctrank':          row.get('budget_exec_rate_pctrank'),
            'p50':              MEDIANS['budget_exec_rate'],
            'higher_is_better': None,   # closer to 100% is ideal
        },
        {
            'id':               'gov_dep_pct',
            'label':            'תלות ממשלתית',
            'value':            round(row.get('gov_dep_pct', 0), 1),
            'unit':             '%',
            'pctrank':          row.get('gov_dep_pct_pctrank'),
            'p50':              MEDIANS['gov_dep_pct'],
            'higher_is_better': False,
        },
        {
            'id':               'admin_pct',
            'label':            'עלויות הנהלה',
            'value':            round(row.get('admin_pct', 0), 1),
            'unit':             '%',
            'pctrank':          row.get('admin_pct_pctrank'),
            'p50':              MEDIANS['admin_pct'],
            'higher_is_better': False,
        },
        {
            'id':               'fin_cost_pct',
            'label':            'עלויות מימון (חוב)',
            'value':            round(row.get('fin_cost_pct', 0), 1),
            'unit':             '%',
            'pctrank':          row.get('fin_cost_pct_pctrank'),
            'p50':              MEDIANS['fin_cost_pct'],
            'higher_is_better': False,
        },
        {
            'id':               'loan_rep_pct',
            'label':            'פרעון מלוות',
            'value':            round(row.get('loan_rep_pct', 0), 1),
            'unit':             '%',
            'pctrank':          row.get('loan_rep_pct_pctrank'),
            'p50':              MEDIANS['loan_rep_pct'],
            'higher_is_better': False,
        },
        {
            'id':               'edu_pct',
            'label':            'הוצאה על חינוך',
            'value':            round(row.get('edu_pct', 0), 1),
            'unit':             '%',
            'pctrank':          row.get('edu_pct_pctrank'),
            'p50':              MEDIANS['edu_pct'],
            'higher_is_better': True,
        },
        {
            'id':               'welfare_pct',
            'label':            'הוצאה על רווחה',
            'value':            round(row.get('welfare_pct', 0), 1),
            'unit':             '%',
            'pctrank':          row.get('welfare_pct_pctrank'),
            'p50':              MEDIANS['welfare_pct'],
            'higher_is_better': True,
        },
        {
            'id':               'growth_pct',
            'label':            'צמיחת הכנסות שנ"ש',
            'value':            round(row.get('growth_pct', 0), 1),
            'unit':             '%',
            'pctrank':          row.get('growth_pct_pctrank'),
            'p50':              MEDIANS['growth_pct'],
            'higher_is_better': True,
        },
    ]

    # Compute simple health score (0-100)
    score_inputs = []
    for kpi in kpi_cards:
        pr = kpi.get('pctrank')
        hib = kpi.get('higher_is_better')
        if pr is None or hib is None:
            continue
        eff = pr if hib else (100 - pr)
        score_inputs.append(eff)
    health_score = round(sum(score_inputs) / len(score_inputs)) if score_inputs else 50

    return jsonify({
        'municipality': municipality,
        'year_cur':     YEAR_CUR,
        'n_munis':      len(analyst_kpis),
        'health_score': health_score,
        'kpis':         kpi_cards,
        'red_flags':    flags,
        'totals': {
            'rec_cur': int(row.get('rec_cur', 0)),
            'pay_cur': int(row.get('pay_cur', 0)),
            'surplus': int(row.get('surplus', 0)),
        },
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5003))
    app.run(host='0.0.0.0', debug=False, port=port)
