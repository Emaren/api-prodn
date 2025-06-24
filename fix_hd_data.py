# fix_hd_data.py

CIV_MAP = {
    1: "Britons", 2: "Franks", 3: "Goths", 4: "Teutons", 5: "Japanese",
    6: "Chinese", 7: "Byzantines", 8: "Persians", 9: "Saracens", 10: "Turks",
    11: "Vikings", 12: "Mongols", 13: "Celts", 14: "Spanish", 15: "Aztecs",
    16: "Mayans", 17: "Huns", 18: "Koreans", 19: "Italians", 20: "Indians",
    21: "Incas", 22: "Magyars", 23: "Slavs"
    # Extend as needed...
}

def enrich_players(players):
    for p in players:
        civ_num = p.get("civilization")
        if isinstance(civ_num, int):
            p["civilization_name"] = CIV_MAP.get(civ_num, f"Unknown ({civ_num})")

        # Fallback scores to zero if None
        for score_field in [
            "military_score", "economy_score", "technology_score", "society_score",
            "units_killed", "fastest_castle_age"
        ]:
            if p.get(score_field) is None:
                p[score_field] = 0

    return players
