import struct
import csv

FILEPATH = "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame/MP Replay v5.8 @2025.03.21 190554 (1).aoe2record"
OFFSET = 0x2F760
BLOCK_SIZE = 32
NUM_BLOCKS = 50
CSV_OUTPUT = "parsed_blocks.csv"

def parse_block(data):
    try:
        return {
            "offset": f"0x{OFFSET + i*BLOCK_SIZE:06X}",
            "int1": struct.unpack("<I", data[0:4])[0],
            "int2": struct.unpack("<I", data[4:8])[0],
            "float1": struct.unpack("<f", data[8:12])[0],
            "float2": struct.unpack("<f", data[12:16])[0],
            "byte1": data[16],
            "byte2": data[17],
            "int3": struct.unpack("<I", data[20:24])[0],
            "int4": struct.unpack("<I", data[24:28])[0],
            "float3": struct.unpack("<f", data[28:32])[0],
        }
    except Exception as e:
        return {"offset": f"0x{OFFSET + i*BLOCK_SIZE:06X}", "error": str(e)}

rows = []
with open(FILEPATH, "rb") as f:
    f.seek(OFFSET)
    for i in range(NUM_BLOCKS):
        block = f.read(BLOCK_SIZE)
        if len(block) < BLOCK_SIZE:
            break
        rows.append(parse_block(block))

with open(CSV_OUTPUT, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Exported {len(rows)} blocks to {CSV_OUTPUT}")
