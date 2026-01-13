print("🚀 BSC Data Pipeline started!")
print("✅ Database connection should work")

# Простой тест
try:
    import requests
    print("✅ requests library is available")
except ImportError as e:
    print(f"❌ Error: {e}")