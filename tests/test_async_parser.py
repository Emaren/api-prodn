import asyncio
import sys
import os

# Add the root of the project to the module search path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.replay_parser import parse_replay_full, hash_replay_file

REPLAY_PATH = "/Users/tonyblum/Library/Application Support/CrossOver/Bottles/Steam/drive_c/Program Files (x86)/Steam/steamapps/common/Age2HD/SaveGame/MP Replay v5.8 @2025.03.19 174212 (1).aoe2record"  # 👈 Replace with real path

async def test_parse():
    print("🔄 Hashing replay...")
    replay_hash = await hash_replay_file(REPLAY_PATH)
    print(f"✅ SHA256: {replay_hash}")

    print("🔄 Parsing replay...")
    result = await parse_replay_full(REPLAY_PATH)

    if result:
        print("✅ Parsed successfully:")
        print(result)
    else:
        print("❌ Parsing failed.")

if __name__ == "__main__":
    asyncio.run(test_parse())
