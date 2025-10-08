import hashlib

def stable_key(row):
    s = "|".join([
        row.get('Query','') or '',
        row.get('Page','') or '',
        str(row.get('Date',''))
    ])
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

# رکورد تستی
row = {
    'Query': 'buy shoes',
    'Page': 'https://example.com/shoes',
    'Date': '2025-09-26'
}

# اجرای چندباره
key1 = stable_key(row)
key2 = stable_key(row)
key3 = stable_key(row)

print("Key1:", key1)
print("Key2:", key2)
print("Key3:", key3)

print("همه برابرند؟", key1 == key2 == key3)
