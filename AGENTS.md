# Guía para Agentes

Este proyecto contiene **DBMSKuna**, un sistema de gestión de bases de datos con múltiples estructuras de índice y una API en FastAPI. Este archivo explica la estructura del repositorio y cómo ejecutar pruebas o la aplicación.

## Estructura principal
- `core/` : lógica del motor y manejo de tablas.
- `indexes/` : implementaciones de índices (B+Tree, AVL, Hash, RTree, etc.).
- `parser/` : analizador de un lenguaje SQL simplificado.
- `api/` : servicio REST utilizando FastAPI.
- `test/` : pruebas y archivos CSV de ejemplo.

## Instalación
1. Clonar el repositorio y situarse en la raíz.
2. Instalar dependencias con:
   ```bash
   pip install -r requirements.txt
   ```
   El paquete `rtree` requiere que el sistema tenga instalada la librería `libspatialindex`.

## Uso
- Para iniciar la API ejecutar:
  ```bash
  uvicorn api.main:app --reload
  ```
  También puede ejecutarse con:
  ```bash
  python api/main.py
  ```

## Pruebas
Las pruebas se encuentran en la carpeta `test/`.
La mayoría se ejecuta con:
```bash
pytest
```
Sin embargo, algunos archivos de prueba se ejecutan directamente con
`python3`, por ejemplo:
```bash
python3 test/test_inverted_index.py
```
Algunas pruebas utilizan los CSV proporcionados en `test/`.

## Directrices para Codex
- Mantener el estilo PEP8 al modificar o añadir código.
- Utilizar imports relativos según el patrón actual de los módulos.
- No generar archivos fuera de los que permite `.gitignore`.
- Trabajar siempre sobre la rama principal del repositorio (sin crear ramas nuevas).
