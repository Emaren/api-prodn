# routes/bets.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter()
bets = {}

class Bet(BaseModel):
    match_id: str
    player_1: str
    player_2: str
    amount: float
    accepted: bool = False
    winner: str = None

@router.post("/bets/create")
def create_bet(bet: Bet):
    if bet.match_id in bets:
        raise HTTPException(status_code=400, detail="Match ID already exists.")
    bets[bet.match_id] = bet.dict()
    return {"message": "Bet created!", "bet_id": bet.match_id}

@router.post("/bets/accept/{match_id}")
def accept_bet(match_id: str):
    if match_id not in bets:
        raise HTTPException(status_code=404, detail="Bet not found.")
    bets[match_id]["accepted"] = True
    return {"message": "Bet accepted!", "bet": bets[match_id]}

@router.get("/bets/pending")
def get_pending_bets():
    return [bet for bet in bets.values() if not bet["accepted"]]

@router.post("/replay/upload/{match_id}")
async def upload_replay(match_id: str, request: Request):
    data = await request.json()
    winner = data.get("winner")
    if match_id not in bets:
        raise HTTPException(status_code=404, detail="Bet not found.")
    if bets[match_id]["winner"]:
        raise HTTPException(status_code=400, detail="Bet already settled.")
    bets[match_id]["winner"] = winner
    return {"message": f"Bet settled! {winner} won the bet."}
