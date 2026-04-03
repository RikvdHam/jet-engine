from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates


# Determine templates folder relative to this file
BASE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = BASE_DIR.parent / "templates"


router = APIRouter()
templates = Jinja2Templates(directory=TEMPLATES_DIR)


@router.get("/upload", response_class=HTMLResponse)
async def upload(request: Request):
    return templates.TemplateResponse(
        "upload.html",
        {
            "request": request,
            "active_page": "upload"
         }
    )


@router.get("/column-mapping", response_class=HTMLResponse)
async def column_mapping(request: Request):
    return templates.TemplateResponse(
        "mapping.html",
        {
            "request": request,
            "active_page": "column-mapping"
         }
    )


@router.get("/validate", response_class=HTMLResponse)
async def validate(request: Request):
    return templates.TemplateResponse(
        "validate.html",
        {
            "request": request,
            "active_page": "validate"
         }
    )


@router.get("/workspace", response_class=HTMLResponse)
async def workspace(request: Request):
    return templates.TemplateResponse(
        "workspace.html",
        {
            "request": request,
            "active_page": "workspace"
         }
    )
