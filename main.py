# check_files.py
import os
import sys

print("=== RAILWAY FILE CHECK ===")
print(f"Python: {sys.version}")
print(f"Working dir: {os.getcwd()}")
print("\n📁 Files in directory:")

for file in os.listdir('.'):
    print(f"  {file}")

print("\n📁 Files in root (/):")
try:
    for file in os.listdir('/'):
        if '.' in file or file in ['app', 'home', 'tmp', 'usr', 'etc']:
            print(f"  /{file}")
except:
    print("  Can't list root dir")

# Check main.py exists
print(f"\n🔍 main.py exists: {os.path.exists('main.py')}")

# Write test file
with open('test_railway.txt', 'w') as f:
    f.write('Railway can write files\n')
print("📝 Created test_railway.txt")

print("\n⏳ Waiting 60 seconds...")
import time
for i in range(60):
    print(f"[{i+1}/60] Container alive")
    time.sleep(1)

print("✅ Check complete")