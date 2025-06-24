from collections import defaultdict

with open('xor_out.bin', 'rb') as f:
    data = f.read()

block_counts = defaultdict(int)

# Scan for all 8-byte sequences
for i in range(len(data) - 8):
    block = data[i:i+8]
    block_counts[block] += 1

# Print most frequent sequences
sorted_blocks = sorted(block_counts.items(), key=lambda x: x[1], reverse=True)
for block, count in sorted_blocks[:20]:
    print(f"{block.hex()} — {count} times")
