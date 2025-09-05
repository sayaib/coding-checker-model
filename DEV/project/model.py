from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import asyncio
import uvicorn
from loguru import logger
import time
from pydantic import BaseModel
from typing import *
#######################################


class data_model(BaseModel):
    input_dir_path: Optional[str] = None
    folder_name: Optional[str] = None


class rule_check_model(BaseModel):
    folder_name: str  # e.g., "Coding Checker_Rule26NG_250703-1756818977690"
    input_list: List[Any]