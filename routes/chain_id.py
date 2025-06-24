from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(prefix="/api/chain-id")

@router.get("")
async def get_chain_id():
    return JSONResponse(content={"chainId": "wolo"})
