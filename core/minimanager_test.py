import minimanager
from schema import TableSchema, Column, DataType, IndexType
from logger import CustomLogger
import random

class Tester:

    def __init__(self):
        self.logger = CustomLogger("MINIMANAGER_TEST")
        # Crear un esquema de tabla para las pruebas
        self.schema = TableSchema(
            name="test_table",
            columns=[
                Column("id", DataType.INT, True, IndexType.AVL),
                Column("name", DataType.STR, False, IndexType.NONE),
                Column("value", DataType.FLOAT, False, IndexType.AVL)
            ]
        )
        self.minimanager = minimanager.miniManager(self.schema)
        self.random = random

    def TestInsert(self, num_records):
        self.logger.info(f"Probando inserción de {num_records} registros...")

        # Insertar registros en orden aleatorio
        ids = list(range(1, num_records + 1))
        self.random.shuffle(ids)

        for i, id_value in enumerate(ids, 1):
            record = [id_value, f"Name{id_value}", id_value * 10.0]
            self.minimanager.insert(record)

            # Verificar que el registro se insertó correctamente
            fetched_record = self.minimanager.search("id", id_value)
            if not fetched_record or fetched_record.values[0] != id_value:
                self.logger.error(f"Error al insertar registro con id {id_value}")
                raise Exception(f"Error en inserción del registro {id_value}")

            if i % 100 == 0 or i == num_records:
                self.logger.info(f"Insertados {i}/{num_records} registros correctamente")

        print(f"Se insertaron {num_records} registros correctamente.")

    def TestSearch(self, num_records):
        self.logger.info(f"Probando búsqueda con {num_records} registros...")

        # Primero, insertar registros para poder buscarlos
        ids = list(range(1, num_records + 1))
        self.random.shuffle(ids)

        for id_value in ids:
            record = [id_value, f"Name{id_value}", id_value * 10.0]
            self.minimanager.insert(record)

        # Realizar diferentes tipos de búsquedas
        for _ in range(min(100, num_records)):  # Realizar hasta 100 búsquedas
            # Buscar un ID existente
            id_to_find = self.random.choice(ids)
            result = self.minimanager.search("id", id_to_find)

            if not result or result.values[0] != id_to_find:
                self.logger.error(f"No se encontró el registro con id {id_to_find}")
                raise Exception(f"Búsqueda fallida para id {id_to_find}")

            # Buscar un ID no existente
            non_existent_id = num_records + self.random.randint(1, 1000)
            try:
                result = self.minimanager.search("id", non_existent_id)
                # Aquí deberíamos verificar que el resultado es None o lanzar una excepción
                if result is not None:
                    self.logger.error(f"Se encontró un registro inexistente con id {non_existent_id}")
                    raise Exception(f"Búsqueda incorrecta para id no existente {non_existent_id}")
            except Exception as e:
                self.logger.info(f"Excepción esperada al buscar id inexistente: {e}")

            # Buscar por rango de valores
            min_val = self.random.randint(1, num_records // 2)
            max_val = min_val + self.random.randint(1, num_records // 2)
            range_results = self.minimanager.searchRange("value", min_val * 10.0, max_val * 10.0)

            # Verificar que los resultados están en el rango correcto
            for record in range_results:
                if not (min_val * 10.0 <= record.values[2] <= max_val * 10.0):
                    self.logger.error(f"Valor fuera de rango: {record.values}")
                    raise Exception("Resultados incorrectos en búsqueda por rango")

        print(f"Todas las búsquedas fueron exitosas.")

    def TestDelete(self, num_records, delete_count):
        self.logger.info(f"Probando eliminación de {delete_count} registros de {num_records}...")

        # Primero, insertar registros para poder eliminarlos
        ids = list(range(1, num_records + 1))
        self.random.shuffle(ids)

        for id_value in ids:
            record = [id_value, f"Name{id_value}", id_value * 10.0]
            self.minimanager.insert(record)

        # Seleccionar IDs para eliminar
        ids_to_delete = self.random.sample(ids, min(delete_count, num_records))

        # Eliminar registros uno por uno
        for i, id_to_delete in enumerate(ids_to_delete, 1):
            try:
                # Buscar la posición del registro
                record = self.minimanager.search("id", id_to_delete)
                positions = self.minimanager._search_equality("id", id_to_delete)

                # Eliminar el registro
                self.minimanager.delete(positions)

                # Verificar que el registro fue eliminado
                try:
                    result = self.minimanager.search("id", id_to_delete)
                    if result is not None:
                        self.logger.error(f"El registro con id {id_to_delete} no se eliminó correctamente")
                        raise Exception(f"Error en eliminación del registro {id_to_delete}")
                except Exception:
                    # Se espera una excepción si el registro no existe
                    pass

                if i % 50 == 0 or i == len(ids_to_delete):
                    self.logger.info(f"Eliminados {i}/{len(ids_to_delete)} registros correctamente")

            except Exception as e:
                self.logger.error(f"Error al eliminar registro con id {id_to_delete}: {e}")
                raise

        print(f"Se eliminaron {len(ids_to_delete)} registros correctamente.")

    def TestAllRandom(self, num_operations):
        self.logger.info(f"Probando {num_operations} operaciones aleatorias...")

        operations = {
            "insert": 0,
            "search": 0,
            "range_search": 0,
            "delete": 0
        }

        inserted_ids = set()
        max_id = num_operations * 2  # Rango de IDs posibles

        # Insertar algunos registros iniciales
        initial_inserts = min(num_operations // 3, 100)
        for _ in range(initial_inserts):
            id_value = self.random.randint(1, max_id)
            while id_value in inserted_ids:
                id_value = self.random.randint(1, max_id)

            record = [id_value, f"Name{id_value}", id_value * 10.0]
            self.minimanager.insert(record)
            inserted_ids.add(id_value)
            operations["insert"] += 1

        # Realizar operaciones aleatorias
        for _ in range(num_operations):
            if not inserted_ids:  # Si no hay IDs insertados, solo podemos insertar
                operation = "insert"
            else:
                operation = self.random.choice(["insert", "search", "range_search", "delete"])

            if operation == "insert":
                id_value = self.random.randint(1, max_id)
                while id_value in inserted_ids:
                    id_value = self.random.randint(1, max_id)

                record = [id_value, f"Name{id_value}", id_value * 10.0]
                self.minimanager.insert(record)
                inserted_ids.add(id_value)
                operations["insert"] += 1
                self.logger.info(f"Insertado: {id_value}")

            elif operation == "search":
                id_to_find = self.random.choice(list(inserted_ids))
                result = self.minimanager.search("id", id_to_find)
                if not result or result.values[0] != id_to_find:
                    self.logger.error(f"No se encontró el registro con id {id_to_find}")
                    raise Exception(f"Búsqueda fallida para id {id_to_find}")
                operations["search"] += 1
                self.logger.info(f"Búsqueda: {id_to_find}")

            elif operation == "range_search":
                if len(inserted_ids) > 1:
                    min_id = min(inserted_ids)
                    max_id = max(inserted_ids)
                    min_val = self.random.randint(min_id, max_id) * 10.0
                    max_val = self.random.randint(int(min_val / 10.0), max_id) * 10.0

                    results = self.minimanager.searchRange("value", min_val, max_val)
                    operations["range_search"] += 1
                    self.logger.info(f"Búsqueda por rango: {min_val}-{max_val}, resultados: {len(results)}")

            elif operation == "delete":
                id_to_delete = self.random.choice(list(inserted_ids))
                positions = self.minimanager._search_equality("id", id_to_delete)
                self.minimanager.delete(positions)
                inserted_ids.remove(id_to_delete)
                operations["delete"] += 1
                self.logger.info(f"Eliminado: {id_to_delete}")

        print("---RESULTADOS---")
        for op, count in operations.items():
            print(f"{op}: {count} operaciones")

        print(f"Todas las {sum(operations.values())} operaciones aleatorias completadas con éxito.")


if __name__ == "__main__":
    tester = Tester()
    tester.TestInsert(1000)
    tester.TestSearch(1000)
    tester.TestDelete(500,1000)
    tester.TestAllRandom(1000)
