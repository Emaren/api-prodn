import os
import zlib

REPLAY_PATH = "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame/MP Replay v5.8 @2025.03.21 190554 (1).aoe2record"
KNOWN_OFFSETS = [26799, 84735, 193310, 194451, 245680]

def try_raw_deflate(file_path, offsets, chunk_size=512):
    with open(file_path, "rb") as f:
        data = f.read()

    for idx, offset in enumerate(offsets):
        chunk = data[offset:offset + chunk_size]
        try:
            decompressed = zlib.decompress(chunk, wbits=-15)
            print(f"\n✅ Raw DEFLATE success @ offset {offset} (chunk {idx+1})")
            print("-" * 60)
            preview = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in decompressed[:400])
            print(preview)
        except Exception as e:
            print(f"❌ Raw DEFLATE failed @ offset {offset}: {e}")

if __name__ == "__main__":
    if not os.path.exists(REPLAY_PATH):
        print(f"❌ File not found: {REPLAY_PATH}")
    else:
        try_raw_deflate(REPLAY_PATH, KNOWN_OFFSETS)
