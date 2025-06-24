import os
import zlib

REPLAY_PATH = "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame/MP Replay v5.8 @2025.03.21 190554 (1).aoe2record"
OFFSETS = [26799, 84735, 193310, 194451, 245680]
CHUNK_SIZE = 16384  # 16KB per try

def dump_successful_deflate_chunks(file_path, offsets):
    out_dir = "decompressed_chunks"
    os.makedirs(out_dir, exist_ok=True)

    with open(file_path, "rb") as f:
        data = f.read()

    for i, offset in enumerate(offsets):
        chunk = data[offset:offset + CHUNK_SIZE]
        try:
            decompressed = zlib.decompress(chunk, wbits=-15)
            output_path = os.path.join(out_dir, f"chunk_{i+1}_@{offset}.bin")
            with open(output_path, "wb") as out:
                out.write(decompressed)
            print(f"✅ Decompressed chunk {i+1} @ offset {offset} → {output_path}")
        except Exception as e:
            print(f"❌ Failed chunk {i+1} @ offset {offset}: {e}")

if __name__ == "__main__":
    if not os.path.exists(REPLAY_PATH):
        print(f"❌ Replay not found: {REPLAY_PATH}")
    else:
        dump_successful_deflate_chunks(REPLAY_PATH, OFFSETS)
