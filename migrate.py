"""
migrate.py — Import your old localStorage data into Flask/SQLite
=================================================================
Updated for Partition Edition of PharmaCare Pro.

All migrated data is tagged  partition='both'
→ visible in ALL pharmacy modes (Wholesale, Retail, Hospital, etc.)

Usage:
  1. Open the old file:// app in your browser
  2. Open DevTools Console (F12)
  3. Run:  copy(localStorage.getItem('pharmacare_v2'))
  4. Paste into a file called state.json  (Ctrl+V in a text editor, save)
  5. Run:  python migrate.py

After migration:
  • All imported medicines   → visible in both Wholesale AND Retail modes
  • All imported bills       → visible based on their billStoreType tag
  • All imported credits     → visible in both Wholesale AND Retail modes
  • All imported shop credits→ visible in both Wholesale AND Retail modes

  New records you add AFTER migration will be tagged with the active
  pharmacy mode (wholesale / retail) and will only appear in that mode.
"""

import json, sys, urllib.request

STATE_FILE = 'state.json'
API_URL    = 'http://localhost:5000/api/import'

try:
    with open(STATE_FILE, 'r', encoding='utf-8') as f:
        raw = f.read().strip()
    # Handle the case where the file contains a JSON string (double-encoded)
    data = json.loads(raw)
    if isinstance(data, str):
        data = json.loads(data)
except FileNotFoundError:
    print(f"ERROR: '{STATE_FILE}' not found.")
    print("Please paste your localStorage JSON into a file called state.json")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"ERROR: Could not parse state.json — {e}")
    sys.exit(1)

print("Found data:")
print(f"  Products   : {len(data.get('products',   []))}")
print(f"  Categories : {len(data.get('categories', []))}")
print(f"  Bills      : {len(data.get('bills',      []))}")
print(f"  Stock-ins  : {len(data.get('stockIns',   []))}")
print(f"  Credits    : {len(data.get('credits',    []))}")
print(f"  ShopCredits: {len(data.get('shopCredits',[]))}")
print()
print("Note: All migrated records will be tagged  partition='both'")
print("      so they appear in ALL pharmacy modes after import.")
print()

body = json.dumps(data).encode('utf-8')
req  = urllib.request.Request(
    API_URL,
    data=body,
    headers={'Content-Type': 'application/json'},
    method='POST'
)

try:
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    print("✓ Migration successful!")
    print(f"  Server message: {result.get('message', '')}")
    print("  Counts in database:")
    for k, v in result.get('counts', {}).items():
        print(f"    {k}: {v}")
    print()
    print("  Next steps:")
    print("  • Open http://localhost:5000 in your browser")
    print("  • Go to Settings → Pharmacy Type")
    print("  • Select your pharmacy type to activate the correct partition")
    print("  • New medicines/bills added now will be partition-specific")
except urllib.error.URLError as e:
    print(f"ERROR: Could not connect to Flask server — {e}")
    print("Make sure 'python app.py' is running first.")
    sys.exit(1)
