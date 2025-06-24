import os

# 🔁 Replace with the actual path to your HD replay
REPLAY_PATH = "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame/MP Replay v5.8 @2025.03.21 190554 (1).aoe2record"

# 🎯 Set the offset and number of bytes to read
start = 194400
length = 512

if not os.path.exists(REPLAY_PATH):
    print(f"❌ File not found: {REPLAY_PATH}")
    exit(1)

with open(REPLAY_PATH, "rb") as f:
    f.seek(start)
    chunk = f.read(length)

print(f"📄 Dumping bytes from offset {start:#x} to {start+length:#x}...\n")

for i in range(0, len(chunk), 16):
    row = chunk[i:i+16]
    hex_view = ' '.join(f'{b:02X}' for b in row)
    ascii_view = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in row)
    print(f"{start+i:08X}: {hex_view:<47}  {ascii_view}")
