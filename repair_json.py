import json
import re

PROCESSED_REPLAYS_FILE = "processed_replays.json"
FIXED_REPLAYS_FILE = "processed_replays_fixed.json"

def fix_json_structure(data_str):
    """Attempts to fix JSON structure issues and recover valid portions."""
    
    # Step 1: Remove null bytes and non-printable characters
    data_str = data_str.replace("\x00", "")

    # Step 2: Fix concatenated keys (e.g., `"terrain_id""elevation"` → `"terrain_id": 0, "elevation": 0`)
    data_str = re.sub(r'("?\w+"?)("?\w+"?)\s*:', r'\1": null, "\2":', data_str)

    # Step 3: Ensure all keys are wrapped in double quotes
    data_str = re.sub(r'([{,])\s*([^"\s:{}]+)\s*:', r'\1 "\2":', data_str)

    # Step 4: Remove trailing commas inside lists and objects
    data_str = re.sub(r',\s*([\]}])', r'\1', data_str)

    # Step 5: Fix missing commas between objects
    data_str = re.sub(r'(\})\s*({)', r'\1,\2', data_str)

    return data_str

def extract_valid_json(file_path):
    """Reads JSON line-by-line and extracts as many valid objects as possible."""
    valid_json_parts = []
    current_json = ""
    brace_count = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            current_json += line.strip()

            # Count braces to track JSON structure
            brace_count += line.count("{") - line.count("}")

            if brace_count == 0 and current_json:
                try:
                    parsed_obj = json.loads(current_json)
                    valid_json_parts.append(parsed_obj)
                    current_json = ""  # Reset buffer after successful parse
                except json.JSONDecodeError:
                    continue  # Keep appending lines until a valid JSON object is formed

    return valid_json_parts

def repair_json(file_path):
    """Attempts to fix JSON and extract recoverable data."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_data = f.read()

        # Apply structural fixes
        cleaned_data = fix_json_structure(raw_data)

        # Extract valid JSON portions
        valid_data = extract_valid_json(PROCESSED_REPLAYS_FILE)

        if not valid_data:
            print("⚠️ No valid JSON data could be recovered.")
            return {"error": "Severe corruption, no valid data extracted."}

        with open(FIXED_REPLAYS_FILE, "w", encoding="utf-8") as f:
            json.dump(valid_data, f, indent=4)

        print(f"✅ JSON successfully repaired and saved as: {FIXED_REPLAYS_FILE}")
        return valid_data

    except Exception as e:
        print(f"❌ Error processing JSON: {e}")
        return {"error": "Failed to repair JSON."}

# Run the JSON repair
fixed_data = repair_json(PROCESSED_REPLAYS_FILE)

# Output summary
if isinstance(fixed_data, list) and fixed_data:
    print(f"🔍 Extracted {len(fixed_data)} valid JSON objects.")
else:
    print("⚠️ No valid JSON data could be recovered.")
