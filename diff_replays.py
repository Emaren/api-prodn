import sys

def read_bytes(path):
    with open(path, "rb") as f:
        return f.read()

def print_diff(a, b):
    for i in range(min(len(a), len(b))):
        if a[i] != b[i]:
            print(f"Offset 0x{i:06X}: {a[i]:02X} != {b[i]:02X}")

file1 = "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame/MP Replay v5.8 @2025.03.21 180514 (1).aoe2record"
file2 = "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame/MP Replay v5.8 @2025.03.21 190554 (1).aoe2record"

data1 = read_bytes(file1)
data2 = read_bytes(file2)

print(f"📦 File 1: {file1} ({len(data1)} bytes)")
print(f"📦 File 2: {file2} ({len(data2)} bytes)")
print_diff(data1, data2)

