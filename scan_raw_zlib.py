import zlib

def scan_zlib(file_path):
    with open(file_path, 'rb') as f:
        data = f.read()

    count = 0
    for i in range(len(data) - 2):
        if data[i] == 0x78 and data[i+1] in (0x01, 0x9C, 0xDA):
            try:
                zlib.decompress(data[i:])
                print(f"✅ Found zlib stream at offset {hex(i)}")
                count += 1
                if count > 5:
                    break
            except zlib.error:
                continue

    if count == 0:
        print("❌ No valid zlib streams found.")

scan_zlib("/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame/MP Replay v5.8 @2025.03.21 190554 (1).aoe2record")

