"""
Canonical list of index / ETF symbols tracked in the `index_prices` table.

Single source of truth shared by:
  - db_scripts/initialize_data/seed_index_prices_fmp.py  (fresh-build backfill)
  - db_scripts/update_data/daily_indices_update.py       (daily updates)

Add new ETFs here so both seeding and daily updates stay in sync. Symbols added
here that should also surface on the ETFs page must be added to
`user_data/etfs.json` as well.
"""

ETF_SYMBOLS = [
    # Main indices
    'SPY', 'QQQ', 'IWM', '^VIX',
    # Commodities & rates
    'GLD', 'SLV', 'USO', 'TLT',
    # Sector ETFs - Risk On
    'XLB', 'XLC', 'XLY', 'XLE', 'XLF', 'XLV', 'XLI', 'XLK',
    # Sector ETFs - Risk Off
    'XLP', 'XLRE', 'XLU',
    # Tech / Growth
    'IGM', 'SOXX', 'IGV', 'ARTY', 'BAI', 'IBB', 'IHF', 'IHI', 'IHE',
    'IDNA', 'IEZ', 'IEO', 'FILL', 'ITA', 'IYT', 'ICOP', 'RING', 'ILIT', 'PICK', 'SLVP',
    'WOOD', 'IAI', 'IYG', 'IAK', 'IAT', 'REM', 'REZ', 'IDGT', 'ITB',
    # Thematic / sub-sector ETFs (ETFs page)
    'SMH', 'BOTZ', 'ROBT', 'CIBR', 'HACK', 'BUG', 'SKYY', 'WCLD',
    'XBI', 'LABU', 'LABD', 'FINX', 'ARKF', 'ICLN', 'TAN', 'LIT', 'IDRV',
    'ARKK', 'ARKG', 'ARKW', 'XHB', 'KRE', 'KBE', 'XRT', 'KWEB', 'CQQQ',
    # Leveraged sub-sector (3X)
    'SOXL', 'SOXS', 'TECL', 'TECS', 'FAS', 'FAZ', 'NAIL', 'DPST',
]
