from typing import Any
from pydantic import BaseModel


class SerieIndicador(BaseModel):
    fecha: str
    valor: float


class IndicadorResponse(BaseModel):
    version: str
    autor: str
    codigo: str
    nombre: str
    unidad_medida: str
    serie: list[SerieIndicador]
