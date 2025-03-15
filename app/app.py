import pandas as pd
import pymongo
from pymongo.database import Collection
from datetime import datetime

CATEGORIAS_VISITADAS = "D|N|X"

def __obtener_datos(coleccion: Collection, desde: datetime):
    todos = coleccion.aggregate([
        {"$match": {"fecha_transaccion": {"$gt": desde}}}
    ])
    todos = list(todos)
    todos = pd.DataFrame(todos, columns=["nombre",
            "fecha_transaccion",
            "monto",
            "recurrente",
            "genero",
            "producto",
            "categoria_favorita",
            "categorias_mas_visitadas"
            ])
    return todos

def __filtrar_datos(datos: pd.DataFrame):
    datos = datos[datos['categorias_mas_visitadas'].str.contains(CATEGORIAS_VISITADAS)]
    return datos

def __transformar_datos(datos: pd.DataFrame):
    datos['recurrente'] = datos['recurrente'].astype(int)
    datos = datos.drop('fecha_transaccion', axis=1)
    return datos

def __enviar(coleccion: Collection, datos: pd.DataFrame):
    lista = []
    for i in datos.index:
        lista.append(datos.loc[i].to_dict())
    coleccion.insert_many(lista)

if __name__ == "__main__":
    cliente = pymongo.MongoClient("mongodb://localhost:27017")
    db = cliente.informes
    ventas = db.ventas
    analisis = db.analisis
    datos = __obtener_datos(ventas, datetime(2024, 11, 1))
    datos = __filtrar_datos(datos)
    datos = __transformar_datos(datos)
    __enviar(analisis, datos )

