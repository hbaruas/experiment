"""
rebuild_cache.py — standalone Phase 0 cache builder
Run with: /Users/saurabhkumar/miniforge3/envs/newenv/bin/python3 rebuild_cache.py
Reads 2.1GB CSV in chunks and writes six .pkl files to Data/cache/
Estimated time: ~5 minutes.
"""
import pandas as pd
import numpy as np
from pathlib import Path
import time

ROOT     = Path(__file__).parent
DATA_DIR = ROOT / "Data"
CACHE    = DATA_DIR / "cache"
RAW_CSV  = DATA_DIR / "sic2_region_2019_2025_nomis-1.csv"
CACHE.mkdir(parents=True, exist_ok=True)

COLS = {
    'Payer (2-digit SIC)'      : 'payer_sic',
    'Payer Region (ITL-1)'     : 'payer_region',
    'Payee (2-digit SIC)'      : 'payee_sic',
    'Payee Region (ITL-1)'     : 'payee_region',
    'Date'                     : 'date',
    'Value (£)'                : 'value',
    'Number of transactions'   : 'transactions',
}

SIC2_SECTION = {
    1:'A', 2:'A', 3:'A',
    5:'B', 6:'B', 7:'B', 8:'B', 9:'B',
    10:'C', 11:'C', 12:'C', 13:'C', 14:'C', 15:'C', 16:'C', 17:'C',
    18:'C', 19:'C', 20:'C', 21:'C', 22:'C', 23:'C', 24:'C', 25:'C',
    26:'C', 27:'C', 28:'C', 29:'C', 30:'C', 31:'C', 32:'C', 33:'C',
    35:'D', 36:'E', 37:'E', 38:'E', 39:'E',
    41:'F', 42:'F', 43:'F',
    45:'G', 46:'G', 47:'G',
    49:'H', 50:'H', 51:'H', 52:'H', 53:'H',
    55:'I', 56:'I',
    58:'J', 59:'J', 60:'J', 61:'J', 62:'J', 63:'J',
    64:'K', 65:'K', 66:'K',
    68:'L',
    69:'M', 70:'M', 71:'M', 72:'M', 73:'M', 74:'M', 75:'M',
    77:'N', 78:'N', 79:'N', 80:'N', 81:'N', 82:'N',
    84:'O', 85:'P',
    86:'Q', 87:'Q', 88:'Q',
    90:'R', 91:'R', 92:'R', 93:'R',
    94:'S', 95:'S', 96:'S',
    97:'T', 98:'T', 99:'U',
}

BROAD_GROUP = {
    'A': 'Agriculture',
    'B': 'Production', 'C': 'Production', 'D': 'Production', 'E': 'Production',
    'F': 'Construction',
    'G': 'Services', 'H': 'Services', 'I': 'Services', 'J': 'Services',
    'K': 'Services', 'L': 'Services', 'M': 'Services', 'N': 'Services',
    'O': 'Services', 'P': 'Services', 'Q': 'Services', 'R': 'Services',
    'S': 'Services', 'T': 'Services',
}

CHUNK_SIZE = 1_000_000

CACHE_FILES = {
    'timeseries'      : CACHE / 'agg_timeseries.pkl',
    'section_monthly' : CACHE / 'agg_section_monthly.pkl',
    'sic2_monthly'    : CACHE / 'agg_sic2_monthly.pkl',
    'region_section'  : CACHE / 'agg_region_section_monthly.pkl',
    'reg_sic2_dest'   : CACHE / 'agg_reg_sic2_dest.pkl',     # NEW: payer_region+payer_sic→payee_region
    'reg_sic2_pair'   : CACHE / 'agg_reg_sic2_pair.pkl',     # NEW: payer_region+payer_sic→payee_sic
}

def all_cached():
    return all(p.exists() for p in CACHE_FILES.values())

print(f"numpy : {np.__version__}")
print(f"pandas: {pd.__version__}")
print(f"Source: {RAW_CSV}")
print(f"Cache : {CACHE}")

if all_cached():
    print("\n✓ All 6 cache files found — nothing to rebuild.")
    for name, path in CACHE_FILES.items():
        df = pd.read_pickle(path)
        print(f"  {name:20s}: {len(df):>9,} rows")
else:
    missing = [k for k, v in CACHE_FILES.items() if not v.exists()]
    print(f"\nMissing caches: {missing}")
    print("Rebuilding all caches from raw CSV...\n")

    ts_acc = []
    sec_acc = []
    sic2_acc = []
    reg_sec_acc = []
    reg_sic2_dest_acc = []
    reg_sic2_pair_acc = []

    t0 = time.time()
    reader = pd.read_csv(
        RAW_CSV,
        usecols=list(COLS.keys()),
        dtype={'Payer (2-digit SIC)': str, 'Payee (2-digit SIC)': str},
        parse_dates=['Date'],
        chunksize=CHUNK_SIZE,
    )

    for i, chunk in enumerate(reader):
        chunk = chunk.rename(columns=COLS)

        chunk['payer_sic'] = pd.to_numeric(chunk['payer_sic'], errors='coerce')
        chunk['payee_sic'] = pd.to_numeric(chunk['payee_sic'], errors='coerce')
        chunk['value']        = pd.to_numeric(chunk['value'],        errors='coerce').fillna(0)
        chunk['transactions'] = pd.to_numeric(chunk['transactions'], errors='coerce').fillna(0)

        chunk = chunk[(chunk['payer_sic'] > 0) & (chunk['payee_sic'] > 0)]
        chunk['payer_sic'] = chunk['payer_sic'].astype(int)
        chunk['payee_sic'] = chunk['payee_sic'].astype(int)

        chunk['payer_section'] = chunk['payer_sic'].map(SIC2_SECTION)
        chunk['payee_section'] = chunk['payee_sic'].map(SIC2_SECTION)
        chunk['payer_group']   = chunk['payer_section'].map(BROAD_GROUP)
        chunk['payee_group']   = chunk['payee_section'].map(BROAD_GROUP)
        chunk = chunk.dropna(subset=['payer_section', 'payee_section'])

        val  = 'value'
        txns = 'transactions'

        # 1. UK monthly totals
        ts_acc.append(chunk.groupby('date')[[val, txns]].sum())

        # 2. Section × section monthly
        sec_acc.append(
            chunk.groupby(['date', 'payer_section', 'payee_section'])[[val, txns]].sum()
        )

        # 3. SIC-2 × SIC-2 monthly (UK-wide)
        sic2_acc.append(
            chunk.groupby(['date', 'payer_sic', 'payee_sic'])[[val, txns]].sum()
        )

        # 4. Region × section monthly
        reg_sec_acc.append(
            chunk.groupby(['date', 'payer_region', 'payee_region',
                           'payer_section', 'payee_section'])[[val, txns]].sum()
        )

        # 5. payer_region × payer_sic → payee_region (for retention analysis)
        reg_sic2_dest_acc.append(
            chunk.groupby(['date', 'payer_region', 'payer_sic', 'payee_region'])[[val, txns]].sum()
        )

        # 6. payer_region × payer_sic → payee_sic (for motor vehicle case study)
        reg_sic2_pair_acc.append(
            chunk.groupby(['date', 'payer_region', 'payer_sic', 'payee_sic'])[[val, txns]].sum()
        )

        elapsed = time.time() - t0
        print(f"  Chunk {i+1:2d}: {len(chunk):>9,} rows  value=£{chunk['value'].sum()/1e9:.1f}bn  "
              f"elapsed={elapsed:.0f}s")

    print("\nConsolidating and saving cache files...")

    def consolidate(acc, group_cols):
        df = pd.concat(acc)
        return df.groupby(group_cols)[['value', 'transactions']].sum().reset_index()

    ts  = pd.concat(ts_acc).groupby('date')[['value', 'transactions']].sum().reset_index()
    sec = consolidate(sec_acc,  ['date', 'payer_section', 'payee_section'])
    s2  = consolidate(sic2_acc, ['date', 'payer_sic', 'payee_sic'])
    rs  = consolidate(reg_sec_acc, ['date', 'payer_region', 'payee_region',
                                     'payer_section', 'payee_section'])
    rd  = consolidate(reg_sic2_dest_acc, ['date', 'payer_region', 'payer_sic', 'payee_region'])
    rp  = consolidate(reg_sic2_pair_acc, ['date', 'payer_region', 'payer_sic', 'payee_sic'])

    ts.to_pickle(CACHE_FILES['timeseries'])
    sec.to_pickle(CACHE_FILES['section_monthly'])
    s2.to_pickle(CACHE_FILES['sic2_monthly'])
    rs.to_pickle(CACHE_FILES['region_section'])
    rd.to_pickle(CACHE_FILES['reg_sic2_dest'])
    rp.to_pickle(CACHE_FILES['reg_sic2_pair'])

    print()
    print("=== CACHE SUMMARY ===")
    for name, df in [('timeseries', ts), ('section_monthly', sec), ('sic2_monthly', s2),
                     ('region_section', rs), ('reg_sic2_dest', rd), ('reg_sic2_pair', rp)]:
        print(f"  {name:20s}: {len(df):>9,} rows  value=£{df['value'].sum()/1e12:.3f}tn  "
              f"dtype={df['value'].dtype}")

    print(f"\nTotal time: {(time.time()-t0)/60:.1f} minutes")
    print("Done. Restart Jupyter kernel and re-run the notebook.")
