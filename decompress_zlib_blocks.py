import zlib

def try_decompress_at_offset(blob, offset):
    for wbits in [15, -15]:  # 15 = standard zlib, -15 = raw deflate
        try:
            chunk = blob[offset:]
            result = zlib.decompress(chunk, wbits=wbits)
            print(f"✅ Success at offset {offset} (0x{offset:X}) - wbits={wbits} - Length: {len(result)} bytes")
            print(result[:200])
            print("-----")
            return  # Exit on first success
        except Exception as e:
            print(f"❌ Offset {offset} (0x{offset:X}) wbits={wbits} failed: {e}")

            continue


with open("aoe2_chunk.bin", "rb") as f:
    blob = f.read()

# Typical zlib FLG second bytes
valid_second_bytes = [0x9C, 0xDA, 0x01, 0x5E]

for i in range(len(blob) - 2):
    if blob[i] == 0x78 and blob[i + 1] in valid_second_bytes:
        try_decompress_at_offset(blob, i)
