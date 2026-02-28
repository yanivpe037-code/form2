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

    # Strip whitespace from row names (source data has trailing spaces e.g. 'סהכ הנהלה וכלליות ')
    form2['שורה'] = form2['שורה'].astype(str).str.strip()
    return form2


form2 = load_data()
MUNICIPALITIES = sorted(form2['שם_רשות'].dropna().unique().tolist())
print(f"Ready. {len(MUNICIPALITIES)} municipalities loaded.")

# Detect actual current/previous years from data
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
    'סהכ שירותים ממלכתיים - תשלומים',  # data spells שירותים (with yod) for payments
    'סהכ מפעלים - תשלומים',
    'תשלומים בלתי רגילים',
]


# ---------------------------------------------------------------------------
# Cross-municipality statistics (precomputed at startup)
# ---------------------------------------------------------------------------
def precompute_stats():
    """For each row key, precompute avg%, median%, and per-municipality % across all municipalities.
    Computed for current year only (used for ממוצע, חציון, דירוג columns)."""
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

        # Pivot: municipality (rows) × row_name (cols) → value
        pivot = col_data.pivot_table(
            index='שם_רשות', columns='שורה', values='ערך',
            aggfunc='sum', fill_value=0
        )

        # Grand total per municipality (sum of section subtotals)
        subtotals = RECEIPTS_SUBTOTALS if col_key == 'rec_cur' else PAYMENTS_SUBTOTALS
        avail = [s for s in subtotals if s in pivot.columns]
        grand_totals = pivot[avail].sum(axis=1).replace(0, np.nan)

        # % pivot: value / grand_total * 100
        pct_pivot = pivot.div(grand_totals, axis=0).mul(100)

        structure = RECEIPTS_STRUCTURE if col_key == 'rec_cur' else PAYMENTS_STRUCTURE
        col_stats = {}
        for item in structure:
            rk = item.get('key')
            if not rk or rk.startswith('__'):
                continue
            if rk in pct_pivot.columns:
                pct_s = pct_pivot[rk].fillna(0)
            else:
                pct_s = pd.Series(0.0, index=pivot.index)

            col_stats[rk] = {
                'avg_pct':    round(float(pct_s.mean()),   1),
                'median_pct': round(float(pct_s.median()), 1),
                'muni_pcts':  pct_s.to_dict(),   # {municipality: pct}
            }
        stats[col_key] = col_stats

    return stats


def compute_rank(muni_pcts_dict, municipality):
    """Return rank of municipality by %, 1 = highest. Ties share the same rank."""
    muni_pct = muni_pcts_dict.get(municipality, 0.0)
    return int(sum(1 for p in muni_pcts_dict.values() if p > muni_pct) + 1)


row_stats = precompute_stats()
print("Cross-municipality stats precomputed.")


# ---------------------------------------------------------------------------
# Extra sheets (generic display)
# ---------------------------------------------------------------------------
EXTRA_SHEETS_CACHE = os.path.join(os.path.dirname(__file__), 'extra_sheets_cache.pkl')
extra_sheets_data = None

DISPLAY_SHEETS = [
    {'key': 'form2',              'label': 'טופס 2 – תקבולים ותשלומים'},
    {'key': 'דוח לתושב',          'label': 'דוח לתושב'},
    {'key': 'ספר לבן',            'label': 'ספר לבן'},
    {'key': 'טופס 1 אקטיב',       'label': 'מאזן – נכסים (טופס 1א)'},
    {'key': 'טופס 1 פאסיב',       'label': 'מאזן – התחייבויות (טופס 1פ)'},
    {'key': 'נספח 2 לטופס 2',     'label': 'נספח 2 – הוצאות פירוט'},
    {'key': 'נספח 3 לטופס 2',     'label': 'נספח 3 – הכנסות פירוט'},
    {'key': 'נספח 5 לטופס 2',     'label': 'נספח 5 – שכר'},
    {'key': 'טופס 3',             'label': 'טופס 3 – מלוות'},
    {'key': 'טופס 4',             'label': 'טופס 4 – תקציב פיתוח'},
    {'key': 'דוח תמיכות',         'label': 'דוח תמיכות'},
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


def load_extra_sheets():
    global extra_sheets_data
    if os.path.exists(EXTRA_SHEETS_CACHE):
        print("Loading extra sheets cache...")
        with open(EXTRA_SHEETS_CACHE, 'rb') as f:
            df = pickle.load(f)
        # Keep ALL years — "נוכחית" columns live in YEAR_CUR report,
        # "קודמת" columns live in YEAR_PREV report; they don't overlap.
        df['שורה'] = df['שורה'].astype(str).str.strip()
        extra_sheets_data = df
        print(f"Extra sheets loaded: {len(df)} rows, {len(df['גליון'].unique())} sheets")
    else:
        print("Warning: extra_sheets_cache.pkl not found. Extra sheets unavailable.")
        extra_sheets_data = None


load_extra_sheets()


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

    # Compute grand totals from section subtotals
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
            row = {
                'label': item['label'],
                'type':  item['type'],
                'num':   item.get('num', ''),
            }
            row['key'] = item.get('key')        # needed by frontend for top/bottom drill-down
            if item['type'] in ('row', 'sub_row', 'subtotal', 'irregular'):
                key = item['key']
                cur_val  = data[cur_key].get(key, 0)
                prev_val = data[prev_key].get(key, 0)
                row['cur_val']    = cur_val
                row['prev_val']   = prev_val
                row['cur_pct']    = round(cur_val  / total_cur  * 100, 1) if total_cur  else None
                row['prev_pct']   = round(prev_val / total_prev * 100, 1) if total_prev else None
                # Cross-municipality stats (current year only)
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
    """Return top-5 and bottom-5 municipalities by % for a given row key."""
    side_key  = 'rec_cur' if side == 'receipts' else 'pay_cur'
    stats_map = row_stats.get(side_key, {})
    st        = stats_map.get(row_key, {})
    muni_pcts = st.get('muni_pcts', {})

    if not muni_pcts:
        return jsonify({'row_key': row_key, 'top5': [], 'bottom5': [],
                        'total': 0, 'n_zero': 0})

    all_pairs = [(m, round(float(p), 1)) for m, p in muni_pcts.items()]
    zeros     = sorted([m for m, p in all_pairs if p == 0])
    n_zero    = len(zeros)

    # Top 5: highest values (include zeros only if everything is zero)
    sorted_desc = sorted(all_pairs, key=lambda x: -x[1])
    top5 = [{'muni': m, 'pct': p} for m, p in sorted_desc[:5]]

    # Bottom 5: lowest non-zero values
    non_zero   = [(m, p) for m, p in all_pairs if p > 0]
    sorted_asc = sorted(non_zero, key=lambda x: x[1])
    bottom5    = [{'muni': m, 'pct': p} for m, p in sorted_asc[:5]]

    return jsonify({'row_key': row_key, 'top5': top5, 'bottom5': bottom5,
                    'total': len(all_pairs), 'n_zero': n_zero, 'zeros': zeros})


@app.route('/api/sheets')
def get_sheets():
    return jsonify(DISPLAY_SHEETS)


@app.route('/api/sheet_data')
def get_sheet_data():
    sheet_name   = request.args.get('sheet', '')
    municipality = request.args.get('municipality', '')

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

    # All unique columns present for this municipality in this sheet
    all_cols = muni_df['עמודה'].dropna().unique().tolist()

    # Order by preferred list first, then any remaining columns
    ordered_cols = [c for c in PREFERRED_COL_ORDER if c in all_cols]
    ordered_cols += [c for c in all_cols if c not in ordered_cols]

    # Build (שורה, עמודה) → ערך lookup
    lookup = {}
    for _, r in muni_df.iterrows():
        key = (str(r['שורה']).strip(), str(r['עמודה']))
        v = r['ערך']
        lookup[key] = float(v) if pd.notna(v) else None

    # Ordered rows by קוד (unique labels)
    row_order = (muni_df[['קוד', 'שורה']]
                 .drop_duplicates()
                 .sort_values('קוד')
                 .reset_index(drop=True))

    rows = []
    seen = set()
    for _, rr in row_order.iterrows():
        label = str(rr['שורה']).strip()
        if label in seen:
            continue
        seen.add(label)
        row = {
            'label': label,
            'code': float(rr['קוד']) if pd.notna(rr['קוד']) else None,
        }
        for col in ordered_cols:
            row[col] = lookup.get((label, col))
        rows.append(row)

    return jsonify({
        'rows': rows,
        'columns': ordered_cols,
        'year_cur': YEAR_CUR,
        'year_prev': YEAR_PREV,
        'municipality': municipality,
        'sheet': sheet_name,
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5003))
    app.run(host='0.0.0.0', debug=False, port=port)
