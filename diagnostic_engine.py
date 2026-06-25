#!/usr/bin/env python3
"""
Structural Diagnostic Engine
=============================
Explains movements in the VAT Flash Estimate Diffusion Index using
granular I2I (industry-to-industry) payment flow data.

Usage:
    python diagnostic_engine.py
    python diagnostic_engine.py --month "Dec 2025" --category "Services (G-T)"
    python diagnostic_engine.py --month "Sep 2025" --category "Manufacturing (C)"
"""

import argparse
import sys
import re
from pathlib import Path

import pandas as pd
import numpy as np
from scipy import stats

# ─── PATHS ────────────────────────────────────────────────────────────────────

ROOT     = Path(__file__).parent
VAT_FILE = ROOT / 'Data' / 'vatflashdataset290526.xlsx'
I2I_FILE = ROOT / 'Data' / 'cache' / 'agg_sic2_monthly.pkl'

# ─── SIC-2 NAME DICTIONARY ────────────────────────────────────────────────────

SIC2_NAME = {
    1:  'Crop & animal production',        2:  'Forestry & logging',
    3:  'Fishing & aquaculture',
    5:  'Coal mining',                     6:  'Crude petroleum & gas',
    7:  'Metal ore mining',                8:  'Other mining & quarrying',
    9:  'Mining support services',
    10: 'Food manufacturing',              11: 'Beverage manufacturing',
    12: 'Tobacco products',               13: 'Textiles',
    14: 'Wearing apparel',                15: 'Leather products',
    16: 'Wood & wood products',           17: 'Paper & paper products',
    18: 'Printing & recorded media',      19: 'Coke & petroleum refining',
    20: 'Chemicals',                      21: 'Pharmaceuticals',
    22: 'Rubber & plastics',              23: 'Non-metallic mineral products',
    24: 'Basic metals',                   25: 'Fabricated metal products',
    26: 'Computer & electronic products', 27: 'Electrical equipment',
    28: 'Machinery & equipment',          29: 'Motor vehicles & trailers',
    30: 'Other transport equipment',      31: 'Furniture',
    32: 'Other manufacturing',            33: 'Repair & installation of machinery',
    35: 'Electricity, gas & steam supply',
    36: 'Water collection & supply',      37: 'Sewerage',
    38: 'Waste management',               39: 'Remediation activities',
    41: 'Construction of buildings',      42: 'Civil engineering',
    43: 'Specialised construction',
    45: 'Motor vehicle trade & repair',   46: 'Wholesale trade',
    47: 'Retail trade',
    49: 'Land transport',                 50: 'Water transport',
    51: 'Air transport',                  52: 'Warehousing & transport support',
    53: 'Postal & courier services',
    55: 'Accommodation',                  56: 'Food & beverage services',
    58: 'Publishing',                     59: 'Film, TV & music production',
    60: 'Broadcasting',                   61: 'Telecommunications',
    62: 'Computer programming',           63: 'Information services',
    64: 'Financial services',             65: 'Insurance & pensions',
    66: 'Auxiliary financial activities',
    68: 'Real estate activities',
    69: 'Legal & accounting',             70: 'Management consultancy',
    71: 'Architecture & engineering',     72: 'Scientific R&D',
    73: 'Advertising & market research',  74: 'Other professional activities',
    75: 'Veterinary activities',
    77: 'Rental & leasing',               78: 'Employment activities',
    79: 'Travel agencies & tour ops',     80: 'Security & investigation',
    81: 'Building & landscape services',  82: 'Office admin & support',
    84: 'Public administration & defence',
    85: 'Education',
    86: 'Human health activities',        87: 'Residential care',
    88: 'Social work (without accommodation)',
    90: 'Creative arts & entertainment',  91: 'Libraries, archives & museums',
    92: 'Gambling & betting',             93: 'Sports & recreation',
    94: 'Membership organisations',       95: 'Repair of computers & goods',
    96: 'Other personal services',
    97: 'Household employers',            98: 'Undifferentiated household services',
}

# ─── SECTION → SIC-2 RANGES ───────────────────────────────────────────────────

SECTION_SIC_MAP = {
    'A': list(range(1,   4)),   # Farming, forestry, fishing
    'B': list(range(5,  10)),   # Mining & quarrying
    'C': list(range(10, 34)),   # Manufacturing
    'D': [35],                  # Electricity, gas & steam
    'E': list(range(36, 40)),   # Water, sewerage, waste
    'F': list(range(41, 44)),   # Construction
    'G': list(range(45, 48)),   # Wholesale & retail
    'H': list(range(49, 54)),   # Transport & storage
    'I': list(range(55, 57)),   # Accommodation & food
    'J': list(range(58, 64)),   # IT & communications
    'K': list(range(64, 67)),   # Finance & insurance
    'L': [68],                  # Real estate
    'M': list(range(69, 76)),   # Professional services
    'N': list(range(77, 83)),   # Admin & support
    'O': [84],                  # Public administration
    'P': [85],                  # Education
    'Q': list(range(86, 89)),   # Health & social work
    'R': list(range(90, 94)),   # Arts & recreation
    'S': list(range(94, 97)),   # Other services
    'T': list(range(97, 99)),   # Household employers
}

# Composite VAT groupings
COMPOSITE_SECTIONS = {
    'Production (B-E)': ['B', 'C', 'D', 'E'],
    'Services (G-T)':   ['G', 'H', 'I', 'J', 'K', 'L', 'M', 'N',
                         'O', 'P', 'Q', 'R', 'S', 'T'],
}

# ─── EXACT VAT COLUMN NAMES (from the Excel file) ─────────────────────────────

VAT_COLUMNS = [
    'Total (A-T)',
    'Production (B-E)',
    'Services (G-T)',
    'Agriculture, forestry and fishing (A)',
    'Mining and quarrying (B)',
    'Manufacturing (C)',
    'Electricity, gas, steam and air (D)',
    'Water supply, sewerage, etc (E)',
    'Construction (F)',
    'Wholesale and retail; repair of motor vehicles (G)',
    'Transport and storage (H)',
    'Accommodation and food services (I)',
    'Information and communication (J)',
    'Financial and insurance activities (K)',
    'Real estate activities (L)',
    'Professional, scientific and technical activities (M)',
    'Administrative and support activities (N)',
    'Public administration and defence (O)',
    'Education (P)',
    'Human health and social work (Q)',
    'Arts, entertainmant and recreation (R)',
    'Other service activities (S)',
]

# ─── DATA LOADERS ─────────────────────────────────────────────────────────────

def load_vat():
    """Load VAT diffusion index; [c] / non-numeric → NaN; date as index."""
    raw  = pd.read_excel(VAT_FILE, sheet_name='2.Index Turnover MoM NSA', header=None)
    hdrs = raw.iloc[0].tolist()
    data = raw.iloc[1:].copy()
    data.columns = hdrs
    data = data.rename(columns={'Date': 'date'})
    data['date'] = pd.to_datetime(data['date'])
    data = data.set_index('date')
    for col in data.columns:
        data[col] = pd.to_numeric(data[col], errors='coerce')
    return data


def load_i2i():
    """Load SIC-2 monthly cache; exclude SIC 0 (unclassified placeholder)."""
    s2 = pd.read_pickle(I2I_FILE)
    s2 = s2[(s2['payer_sic'] != 0) & (s2['payee_sic'] != 0)]
    return s2


# ─── INPUT PARSING ────────────────────────────────────────────────────────────

_MONTH_LOOKUP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'june': 6,
    'july': 7, 'august': 8, 'september': 9, 'october': 10,
    'november': 11, 'december': 12,
}

def parse_month(s):
    """Accept 'Dec 2025', 'December 2025', '2025-12' → pd.Timestamp(day=1)."""
    s = s.strip()
    try:
        return pd.Timestamp(s).replace(day=1)
    except Exception:
        pass
    parts = s.replace(',', ' ').split()
    if len(parts) == 2:
        a, b = parts
        if a.lower() in _MONTH_LOOKUP and b.isdigit():
            return pd.Timestamp(year=int(b), month=_MONTH_LOOKUP[a.lower()], day=1)
        if b.lower() in _MONTH_LOOKUP and a.isdigit():
            return pd.Timestamp(year=int(a), month=_MONTH_LOOKUP[b.lower()], day=1)
    raise ValueError(f"Cannot parse month: '{s}'. Try 'Dec 2025' or '2025-12'.")


_CATEGORY_KEYWORDS = {
    'total':         'Total (A-T)',
    'all':           'Total (A-T)',
    'a-t':           'Total (A-T)',
    'production':    'Production (B-E)',
    'b-e':           'Production (B-E)',
    'services':      'Services (G-T)',
    'g-t':           'Services (G-T)',
    'agriculture':   'Agriculture, forestry and fishing (A)',
    'farming':       'Agriculture, forestry and fishing (A)',
    'forestry':      'Agriculture, forestry and fishing (A)',
    'fishing':       'Agriculture, forestry and fishing (A)',
    'mining':        'Mining and quarrying (B)',
    'quarrying':     'Mining and quarrying (B)',
    'manufacturing': 'Manufacturing (C)',
    'electricity':   'Electricity, gas, steam and air (D)',
    'gas':           'Electricity, gas, steam and air (D)',
    'water':         'Water supply, sewerage, etc (E)',
    'sewerage':      'Water supply, sewerage, etc (E)',
    'construction':  'Construction (F)',
    'building':      'Construction (F)',
    'wholesale':     'Wholesale and retail; repair of motor vehicles (G)',
    'retail':        'Wholesale and retail; repair of motor vehicles (G)',
    'transport':     'Transport and storage (H)',
    'logistics':     'Transport and storage (H)',
    'accommodation': 'Accommodation and food services (I)',
    'hospitality':   'Accommodation and food services (I)',
    'food services': 'Accommodation and food services (I)',
    'information':   'Information and communication (J)',
    'communication': 'Information and communication (J)',
    'tech':          'Information and communication (J)',
    'it':            'Information and communication (J)',
    'finance':       'Financial and insurance activities (K)',
    'financial':     'Financial and insurance activities (K)',
    'insurance':     'Financial and insurance activities (K)',
    'real estate':   'Real estate activities (L)',
    'property':      'Real estate activities (L)',
    'professional':  'Professional, scientific and technical activities (M)',
    'scientific':    'Professional, scientific and technical activities (M)',
    'admin':         'Administrative and support activities (N)',
    'administrative':'Administrative and support activities (N)',
    'government':    'Public administration and defence (O)',
    'public admin':  'Public administration and defence (O)',
    'defence':       'Public administration and defence (O)',
    'education':     'Education (P)',
    'health':        'Human health and social work (Q)',
    'social work':   'Human health and social work (Q)',
    'arts':          'Arts, entertainmant and recreation (R)',
    'entertainment': 'Arts, entertainmant and recreation (R)',
    'recreation':    'Arts, entertainmant and recreation (R)',
    'other services':'Other service activities (S)',
}

_LETTER_MAP = {
    'a': 'Agriculture, forestry and fishing (A)',
    'b': 'Mining and quarrying (B)',
    'c': 'Manufacturing (C)',
    'd': 'Electricity, gas, steam and air (D)',
    'e': 'Water supply, sewerage, etc (E)',
    'f': 'Construction (F)',
    'g': 'Wholesale and retail; repair of motor vehicles (G)',
    'h': 'Transport and storage (H)',
    'i': 'Accommodation and food services (I)',
    'j': 'Information and communication (J)',
    'k': 'Financial and insurance activities (K)',
    'l': 'Real estate activities (L)',
    'm': 'Professional, scientific and technical activities (M)',
    'n': 'Administrative and support activities (N)',
    'o': 'Public administration and defence (O)',
    'p': 'Education (P)',
    'q': 'Human health and social work (Q)',
    'r': 'Arts, entertainmant and recreation (R)',
    's': 'Other service activities (S)',
}


def resolve_category(user_str):
    """Match user input to an exact VAT column name."""
    s = user_str.strip()

    # Exact match
    for col in VAT_COLUMNS:
        if col.lower() == s.lower():
            return col

    # Single letter
    if s.lower() in _LETTER_MAP:
        return _LETTER_MAP[s.lower()]

    # Keyword match
    sl = s.lower()
    for kw, col in _CATEGORY_KEYWORDS.items():
        if sl == kw or sl.startswith(kw) or kw in sl:
            return col

    # Substring match against full column names
    matches = [col for col in VAT_COLUMNS if sl in col.lower()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        print(f"\n  Ambiguous: '{s}' matches multiple categories:")
        for i, m in enumerate(matches, 1):
            print(f"    {i}. {m}")
        while True:
            choice = input("  Select number: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(matches):
                return matches[int(choice) - 1]

    # No match — show full list
    print(f"\n  No match for '{s}'. Available categories:")
    for i, col in enumerate(VAT_COLUMNS, 1):
        print(f"    {i:2d}. {col}")
    while True:
        choice = input("  Select number: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(VAT_COLUMNS):
            return VAT_COLUMNS[int(choice) - 1]


def resolve_sic_codes(col_name):
    """Return list of SIC-2 ints for a VAT column; None means Total (A-T)."""
    if col_name == 'Total (A-T)':
        return None
    if col_name in COMPOSITE_SECTIONS:
        sics = []
        for sec in COMPOSITE_SECTIONS[col_name]:
            sics.extend(SECTION_SIC_MAP[sec])
        return sorted(set(sics))
    # Single section — extract letter in parens at end of name
    m = re.search(r'\(([A-Z])\)\s*$', col_name)
    if m:
        return SECTION_SIC_MAP.get(m.group(1), [])
    return []


# ─── FORMATTING HELPERS ───────────────────────────────────────────────────────

def _box(lines, width=76):
    """Wrap lines in a Unicode box."""
    inner = width - 4
    out   = ['╔' + '═' * (width - 2) + '╗']
    for line in lines:
        if line == '':
            out.append('║' + ' ' * (width - 2) + '║')
        else:
            # Handle multi-line entries
            for sub in str(line).split('\n'):
                padded = f'║  {sub}'
                out.append(padded + ' ' * max(0, width - len(padded) - 1) + '║')
    out.append('╚' + '═' * (width - 2) + '╝')
    return '\n'.join(out)


def _fmt_sign(n, fmt='+,.0f'):
    """Format a number with explicit +/- sign."""
    if pd.isna(n) or np.isinf(n):
        return 'n/a'
    return f'{n:{fmt}}'


def _fmt_pct(n):
    if pd.isna(n) or np.isinf(n):
        return 'n/a'
    return f'{n:+.1f}%'


def _table(df):
    """Render a DataFrame as a clean fixed-width text table (no external deps)."""
    cols  = df.columns.tolist()
    str_d = {c: df[c].astype(str).tolist() for c in cols}
    widths = [max(len(str(c)), max((len(v) for v in str_d[c]), default=0)) + 1
              for c in cols]
    sep  = '  '.join('─' * w for w in widths)
    hdr  = '  '.join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
    rows = []
    for row_i in range(len(df)):
        rows.append('  '.join(str_d[c][row_i].ljust(widths[i])
                               for i, c in enumerate(cols)))
    return '\n'.join([hdr, sep] + rows)


def _section_header(title, char='─', width=76):
    print(f'\n{char * width}')
    print(f'  {title}')
    print(f'{char * width}')


# ─── STEP 1: VAT SIGNAL BOX ───────────────────────────────────────────────────

def step1_vat_signal(vat_df, col_name, target_date):
    month_str = target_date.strftime('%B %Y')
    prev_str  = (target_date - pd.DateOffset(months=1)).strftime('%B %Y')

    val = None
    if target_date in vat_df.index:
        raw_val = vat_df.loc[target_date, col_name]
        val = raw_val if not pd.isna(raw_val) else None

    if val is None:
        val_line   = 'Value    : [c]  — SUPPRESSED (insufficient reporters)'
        interp     = 'Data suppressed by ONS; not enough firms to disclose safely.'
        color_hint = ''
    else:
        pct        = abs(val) * 100
        trend      = 'growing' if val >= 0 else 'declining'
        val_line   = f'Value    : {val:+.4f}  ({val*100:+.2f} percentage points)'
        interp     = (f'{pct:.1f}% more firms reported {trend} turnover\n'
                      f'             in {month_str} vs {prev_str}.')
        color_hint = '▲ EXPANSION' if val > 0 else ('▼ CONTRACTION' if val < 0 else '■ FLAT')

    lines = [
        f'VAT Flash Estimate — {month_str}',
        f'Category : {col_name}',
        f'Sheet    : 2. Index Turnover MoM NSA (Not Seasonally Adjusted)',
        '',
        val_line,
        f'Reading  : {interp}',
        '',
        f'Signal   : {color_hint}',
        '',
        'WHAT THIS METRIC MEASURES:',
        '  Diffusion index — counts firms, not £ amounts.',
        '  +0.05 = 5% more firms grew turnover than declined.',
        '  Zero  = equal numbers growing and declining.',
        '  [c]   = suppressed; fewer than the ONS minimum threshold.',
    ]

    _section_header('STEP 1 — VAT FLASH ESTIMATE SIGNAL', char='═')
    print()
    print(_box(lines))


# ─── STEP 2: SIC CODE MAPPING ─────────────────────────────────────────────────

def step2_sic_mapping(col_name, sic_list):
    _section_header('STEP 2 — SIC-2 CODE MAPPING')
    print(f'\n  Category mapped: {col_name}')

    if sic_list is None:
        print('  Scope: Total (A-T) — all SIC-2 codes in the dataset.')
        return

    print(f'  Total SIC-2 codes in scope: {len(sic_list)}\n')
    for letter, sec_sics in SECTION_SIC_MAP.items():
        overlap = [s for s in sec_sics if s in sic_list]
        if not overlap:
            continue
        names = [f'{s}={SIC2_NAME.get(s,"?")[:20]}' for s in overlap]
        print(f'  Section {letter}: ' + ' | '.join(names))
    print()


# ─── STEP 3: TOP MOVERS (non-Total categories) ───────────────────────────────

def step3_top_movers(s2, sic_list, target_date, col_name):
    _section_header('STEP 3 — TOP SIC-2 INBOUND MOVERS  (Revenue / Turnover Proxy)')

    tgt_m  = target_date.strftime('%b %Y')
    prev_d = target_date - pd.DateOffset(months=1)
    prv_m  = prev_d.strftime('%b %Y')

    print(f'\n  Logic:')
    print(f'    INFLOWS  (payee side)  = money received = turnover/revenue ← matches VAT')
    print(f'    TRANSACTIONS (count)   = firm-breadth proxy ← mirrors VAT diffusion logic')
    print(f'    MoM comparison: {tgt_m} vs {prv_m}\n')

    def agg_inflows(date, sic_filter):
        df = s2[s2['date'] == date]
        if sic_filter is not None:
            df = df[df['payee_sic'].isin(sic_filter)]
        return (df.groupby('payee_sic')[['value', 'transactions']]
                  .sum()
                  .rename(columns={'value': 'val', 'transactions': 'tx'}))

    cur   = agg_inflows(target_date, sic_list)
    prev  = agg_inflows(prev_d,      sic_list)
    m     = cur.join(prev, how='outer', lsuffix='_cur', rsuffix='_prv').fillna(0)
    m['delta_tx']      = m['tx_cur']  - m['tx_prv']
    m['pct_tx']        = 100 * m['delta_tx']  / m['tx_prv'].replace(0, np.nan)
    m['delta_val']     = m['val_cur'] - m['val_prv']
    m['pct_val']       = 100 * m['delta_val'] / m['val_prv'].replace(0, np.nan)
    m['abs_delta_tx']  = m['delta_tx'].abs()
    m = m.sort_values('abs_delta_tx', ascending=False).reset_index()
    m.rename(columns={'payee_sic': 'SIC'}, inplace=True)
    m['Name'] = m['SIC'].map(SIC2_NAME).fillna('Unknown')

    top = m.head(10)
    display_df = pd.DataFrame({
        'SIC':             top['SIC'],
        'Industry':        top['Name'].str[:28],
        f'Tx {tgt_m}':     top['tx_cur'].map(lambda x: f'{x:>12,.0f}'),
        f'Tx {prv_m}':     top['tx_prv'].map(lambda x: f'{x:>12,.0f}'),
        'Δ Transactions':  top['delta_tx'].map(lambda x: _fmt_sign(x, '+,.0f')),
        '% Δ Tx':          top['pct_tx'].map(_fmt_pct),
        f'£bn {tgt_m}':    top['val_cur'].map(lambda x: f'£{x/1e9:>6.2f}bn'),
        'Δ £bn':           top['delta_val'].map(lambda x: f'{"+" if x>=0 else ""}{x/1e9:.2f}bn'),
        '% Δ £':           top['pct_val'].map(_fmt_pct),
    })

    print('  ── INBOUND (Revenue side) — Top 10 by absolute change in transactions ──\n')
    print('  ' + _table(display_df).replace('\n', '\n  '))

    # Category-level totals
    total_cur_tx  = cur['tx'].sum()
    total_prv_tx  = prev['tx'].sum() if len(prev) else 0
    total_cur_val = cur['val'].sum()
    total_prv_val = prev['val'].sum() if len(prev) else 0
    d_tx  = total_cur_tx  - total_prv_tx
    d_val = total_cur_val - total_prv_val

    print(f'\n  ── CATEGORY TOTAL (all SIC-2 codes in {col_name}) ──')
    print(f'     Transactions: {total_cur_tx:>14,.0f}  ({_fmt_sign(d_tx,"+,.0f")} MoM,  {_fmt_pct(100*d_tx/total_prv_tx if total_prv_tx else np.nan)})')
    print(f'     Value:        £{total_cur_val/1e9:>10.2f}bn  ({("+" if d_val>=0 else "")}{d_val/1e9:.2f}bn MoM,  {_fmt_pct(100*d_val/total_prv_val if total_prv_val else np.nan)})')

    # Outflows (supply chain)
    print(f'\n  ── OUTBOUND (Supply-chain / expenditure side) — Top 5 ──\n')

    def agg_outflows(date, sic_filter):
        df = s2[s2['date'] == date]
        if sic_filter is not None:
            df = df[df['payer_sic'].isin(sic_filter)]
        return (df.groupby('payer_sic')[['value', 'transactions']]
                  .sum()
                  .rename(columns={'value': 'val', 'transactions': 'tx'}))

    cur_o  = agg_outflows(target_date, sic_list)
    prev_o = agg_outflows(prev_d,      sic_list)
    mo     = cur_o.join(prev_o, how='outer', lsuffix='_cur', rsuffix='_prv').fillna(0)
    mo['delta_tx'] = mo['tx_cur'] - mo['tx_prv']
    mo['abs']      = mo['delta_tx'].abs()
    mo = mo.sort_values('abs', ascending=False).head(5).reset_index()
    mo.rename(columns={'payer_sic': 'SIC'}, inplace=True)
    mo['Name'] = mo['SIC'].map(SIC2_NAME).fillna('Unknown')

    out_df = pd.DataFrame({
        'SIC':            mo['SIC'],
        'Industry':       mo['Name'].str[:28],
        f'Tx {tgt_m}':    mo['tx_cur'].map(lambda x: f'{x:>12,.0f}'),
        'Δ Transactions': mo['delta_tx'].map(lambda x: _fmt_sign(x, '+,.0f')),
        '% Δ Tx':         (100 * mo['delta_tx'] / mo['tx_prv'].replace(0, np.nan)).map(_fmt_pct),
        'Δ £bn':          (mo['val_cur'] - mo['val_prv']).map(lambda x: f'{"+" if x>=0 else ""}{x/1e9:.2f}bn'),
    })
    print('  ' + _table(out_df).replace('\n', '\n  '))
    print()


# ─── STEP 4: TOTAL (A-T) EDGE CASE ───────────────────────────────────────────

def step4_total_movers(s2, target_date):
    _section_header('STEP 4 — TOTAL ECONOMY: LARGEST POSITIVE & NEGATIVE MOVERS')

    tgt_m  = target_date.strftime('%b %Y')
    prev_d = target_date - pd.DateOffset(months=1)
    prv_m  = prev_d.strftime('%b %Y')

    print(f'\n  Scope: All SIC-2 codes (Total A-T)')
    print(f'  Comparison: {tgt_m} vs {prv_m}\n')

    def agg(date):
        return (s2[s2['date'] == date]
                .groupby('payee_sic')[['value', 'transactions']]
                .sum()
                .rename(columns={'value': 'val', 'transactions': 'tx'}))

    cur   = agg(target_date)
    prev  = agg(prev_d)
    m     = cur.join(prev, how='outer', lsuffix='_cur', rsuffix='_prv').fillna(0)
    m['delta_tx']  = m['tx_cur']  - m['tx_prv']
    m['pct_tx']    = 100 * m['delta_tx']  / m['tx_prv'].replace(0, np.nan)
    m['delta_val'] = m['val_cur'] - m['val_prv']
    m = m.reset_index().rename(columns={'payee_sic': 'SIC'})
    m['Name'] = m['SIC'].map(SIC2_NAME).fillna('Unknown')

    for label, asc in [
        ('TOP 5 POSITIVE MOVERS — Inbound Transactions (Expanding)', False),
        ('TOP 5 NEGATIVE MOVERS — Inbound Transactions (Contracting)', True),
    ]:
        sub = m.sort_values('delta_tx', ascending=asc).head(5)
        df  = pd.DataFrame({
            'SIC':            sub['SIC'],
            'Industry':       sub['Name'].str[:30],
            f'Tx {tgt_m}':    sub['tx_cur'].map(lambda x: f'{x:>12,.0f}'),
            'Δ Transactions': sub['delta_tx'].map(lambda x: _fmt_sign(x, '+,.0f')),
            '% Δ Tx':         sub['pct_tx'].map(_fmt_pct),
            'Δ £bn':          sub['delta_val'].map(lambda x: f'{"+" if x>=0 else ""}{x/1e9:.2f}bn'),
        })
        print(f'  ── {label} ──\n')
        print('  ' + _table(df).replace('\n', '\n  '))
        print()


# ─── CORRELATION ANALYSIS ─────────────────────────────────────────────────────

def bonus_correlation(vat_df, s2, col_name, sic_list):
    _section_header('CORRELATION ANALYSIS — Proving the I2I ↔ VAT Relationship', char='═')

    print(f'\n  Hypothesis: Monthly growth in I2I INBOUND TRANSACTIONS predicts the')
    print(f'  VAT Diffusion Index for the same category.')
    print(f'  Category : {col_name}')
    print(f'  Period   : Jan 2019 – Dec 2025 (up to 84 months)')
    print(f'\n  Lags tested: 0 = same month; 1 = I2I leads VAT by 1 month; etc.\n')

    # Build I2I monthly inbound transaction MoM% change
    df = s2.copy()
    if sic_list is not None:
        df = df[df['payee_sic'].isin(sic_list)]

    monthly = (df.groupby('date')['transactions']
                 .sum()
                 .sort_index())
    monthly_pct = monthly.pct_change() * 100   # MoM% change in transaction volume

    vat_series = vat_df[col_name].dropna()

    rows = []
    for lag in range(4):
        i2i_lagged = monthly_pct.shift(lag)
        combined   = pd.DataFrame({
            'i2i': i2i_lagged,
            'vat': vat_series,
        }).dropna()

        if len(combined) < 12:
            rows.append({
                'Lag':         f'{lag}m' if lag else '0 (same)',
                'I2I leads by': f'{lag} month(s)' if lag else 'no lag',
                'N (months)':  len(combined),
                'Pearson r':   'n/a',
                'p-value':     'n/a',
                'Spearman ρ':  'n/a',
                'p-value ':    'n/a',
                'Strength':    'insufficient data',
            })
            continue

        pr, pp = stats.pearsonr( combined['i2i'], combined['vat'])
        sr, sp = stats.spearmanr(combined['i2i'], combined['vat'])

        def p_str(p): return '<0.001' if p < 0.001 else f'{p:.3f}'
        def strength(r):
            r = abs(r)
            if r >= 0.70: return 'Strong'
            if r >= 0.50: return 'Moderate'
            if r >= 0.30: return 'Weak'
            return 'Negligible'

        rows.append({
            'Lag':          f'{lag}m' if lag else '0 (same)',
            'I2I leads by': f'{lag} month(s)' if lag else 'no lag',
            'N (months)':   len(combined),
            'Pearson r':    f'{pr:+.3f}',
            'p-value':      p_str(pp),
            'Spearman ρ':   f'{sr:+.3f}',
            'p-value ':     p_str(sp),
            'Strength':     strength(pr),
        })

    corr_df = pd.DataFrame(rows)
    print('  ' + _table(corr_df).replace('\n', '\n  '))

    # Interpretation
    numeric = [r for r in rows if r['Pearson r'] not in ('n/a', 'insufficient data')]
    if numeric:
        best = max(numeric, key=lambda r: abs(float(r['Pearson r'].replace('+', ''))))
        pr   = float(best['Pearson r'].replace('+', ''))
        print(f'\n  ── INTERPRETATION ──')
        print(f'  Best correlation : Lag {best["Lag"]}, Pearson r = {best["Pearson r"]}  ({best["Strength"]})')

        if abs(pr) >= 0.5:
            print(f'  ✓ I2I transaction growth has a {best["Strength"].lower()} systematic')
            print(f'    relationship with the VAT Diffusion Index for "{col_name}".')
            if best['Lag'] != '0 (same)':
                lag_n = best['Lag'].replace('m', '')
                print(f'  ✓ I2I data leads the VAT index by {lag_n} month(s) — potential early-warning signal.')
        else:
            print(f'  △ The relationship is {best["Strength"].lower()} for this category.')
            print(f'    Possible reasons: sector uses cash, long invoice cycles, or VAT data is suppressed.')

    # Additional context
    print(f'\n  ── NOTES ──')
    print(f'  • COVID months (Mar–Jun 2020) are included and may amplify correlations.')
    print(f'  • [c]-suppressed VAT months are excluded from the correlation automatically.')
    print(f'  • Transaction counts mirror the VAT diffusion index logic (counting firms,')
    print(f'    not £ values), making them a closer proxy than payment £ volumes.')
    print()


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Structural Diagnostic Engine — I2I Payment Flows vs VAT Flash Estimates',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python diagnostic_engine.py --month "Dec 2025" --category "Services (G-T)"\n'
            '  python diagnostic_engine.py --month "Sep 2025" --category "Manufacturing (C)"\n'
            '  python diagnostic_engine.py --month "Apr 2020" --category "Total (A-T)"\n'
        )
    )
    parser.add_argument('--month',    type=str, help='Target month, e.g. "Dec 2025"')
    parser.add_argument('--category', type=str, help='VAT category or section letter, e.g. "C" or "Services (G-T)"')
    args = parser.parse_args()

    print('\n' + '═' * 76)
    print('  STRUCTURAL DIAGNOSTIC ENGINE')
    print('  Explains VAT Flash Estimate movements using I2I Payment Flow data')
    print('  Source: ONS/Pay.UK/Vocalink I2I + HMRC VAT Flash Estimates')
    print('═' * 76)

    # ── User inputs
    month_str    = args.month    or input('\n  Target month   (e.g. Dec 2025): ').strip()
    category_str = args.category or input('  VAT category   (e.g. Services (G-T), or letter C): ').strip()

    try:
        target_date = parse_month(month_str)
    except ValueError as e:
        print(f'\n  Error: {e}')
        sys.exit(1)

    col_name = resolve_category(category_str)
    sic_list = resolve_sic_codes(col_name)

    print(f'\n  Category resolved : {col_name}')
    print(f'  Target month      : {target_date.strftime("%B %Y")}')
    print(f'  SIC-2 codes       : {"All (Total A-T)" if sic_list is None else len(sic_list)}')

    # ── Load data
    print('\n  Loading data...')
    try:
        vat_df = load_vat()
    except FileNotFoundError:
        print(f'  Error: VAT file not found at {VAT_FILE}')
        sys.exit(1)
    try:
        s2 = load_i2i()
    except FileNotFoundError:
        print(f'  Error: I2I cache not found at {I2I_FILE}')
        print(f'  Run the notebook first to build the cache.')
        sys.exit(1)

    print(f'  VAT data loaded   : {len(vat_df)} months')
    print(f'  I2I cache loaded  : {len(s2):,} rows')

    # ── Validate date is in I2I data
    available_dates = s2['date'].sort_values().unique()
    if target_date not in available_dates:
        first = pd.Timestamp(available_dates[0]).strftime('%b %Y')
        last  = pd.Timestamp(available_dates[-1]).strftime('%b %Y')
        print(f'\n  Error: {target_date.strftime("%B %Y")} not found in I2I data.')
        print(f'  Available range: {first} → {last}')
        sys.exit(1)

    # Check prev month exists (for MoM calculation)
    prev_date = target_date - pd.DateOffset(months=1)
    if prev_date not in available_dates:
        print(f'\n  Warning: Prior month ({prev_date.strftime("%b %Y")}) not in I2I cache.')
        print(f'  MoM deltas will show as 0 for the prior period.')

    # ── Run all steps
    step1_vat_signal(vat_df, col_name, target_date)
    step2_sic_mapping(col_name, sic_list)

    if col_name == 'Total (A-T)':
        step4_total_movers(s2, target_date)
    else:
        step3_top_movers(s2, sic_list, target_date, col_name)

    bonus_correlation(vat_df, s2, col_name, sic_list)

    print('═' * 76)
    print('  Analysis complete.')
    print('═' * 76)
    print()


if __name__ == '__main__':
    main()
