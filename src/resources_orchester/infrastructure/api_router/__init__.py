# Este paquete contiene Data Transfer Objects (DTOs). 
# Sirven para estandarizar la comunicación de datos entre las capas, 
# especialmente entre el Adaptador FastAPI y los Casos de Uso en la capa Application.

from typing import Optional

class CorpusAnalysisRequest:
    """Modelo DTO que define los parámetros necesarios para iniciar un análisis de corpus."""
    def __init__(self, input_path: str, mode: str = "default"):
        self.input_path = input_path
        # 'mode' puede ser 'full', 'update', 'deep', etc.
        self.mode = mode 

class GraphifyReportSummary:
    """Modelo DTO para encapsular un resumen de resultados que debe devolverse al usuario."""
    def __init__(self, message: str, report_data: dict):
        self.message = message
        # Contiene el resumen clave del análisis (e.g., nodos count, edges count)
        self.report_data = report_data

class GraphifyAnalysisResult:
    """Modelo DTO que encapsula la respuesta completa y detallada del pipeline."""
    def __init__(self, nodes_count: int, edges_count: int, input_tokens: int, output_tokens: int):
        self.nodes_count = nodes_count
        self.edges_count = edges_count
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens

# Nota: Siempre es útil usar bibliotecas de tipado como Pydantic en FastAPI para validar estos DTOs
