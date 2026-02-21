from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="app/ui/templates")


@router.get("/", response_class=HTMLResponse)
def trader(request: Request) -> HTMLResponse:
    """Render the Trading Assistant app."""
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"title": "Assemblief Trader"},
    )
