import os
import zlib

FILEPATH = "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame/MP Replay v5.8 @2025.03.21 190554 (1).aoe2record"
CHUNK_OUT_DIR = "zlib_scans"
SIG = b'\x78\x9c'  # zlib header
MAX_SCAN = 5       # How many chunks to try

os.makedirs(CHUNK_OUT_DIR, exist_ok=True)

with open(FILEPATH, 'rb') as f:
    data = f.read()

count = 0
offset = 0
found = 0

while found < MAX_SCAN:
    i = data.find(SIG, offset)
    if i == -1:
        break

    for length in range(100, 50000, 100):  # Try decompressing different lengths
        try:
            chunk = data[i:i+length]
            decompressed = zlib.decompress(chunk)
            outname = f"chunk_{found}_@{i}.bin"
            with open(os.path.join(CHUNK_OUT_DIR, outname), 'wb') as out:
                out.write(decompressed)
            print(f"✅ Decompressed chunk {found} @ offset {i} (length={length}) → {outname}")
            found += 1
            break
        except Exception:
            continue
    offset = i + 1

if found == 0:
    print("❌ No decompressible zlib streams found.")
else:
    print(f"🎉 Extracted {found} zlib streams.")
