
/*CREATE TABLE ventas (
    id INT PRIMARY KEY INDEX BTREE,
    producto VARCHAR(50) INDEX HASH,
    cantidad INT,
    precio FLOAT,
    fecha VARCHAR(20) INDEX AVL
);

CREATE TABLE ventas (
    id INT PRIMARY KEY INDEX BTREE,
    producto VARCHAR(30) INDEX HASH,
    cantidad INT INDEX BTREE,
    precio FLOAT INDEX AVL,
    fecha VARCHAR(20) INDEX AVL
);

INSERT INTO ventas VALUES (1, 'Notebook', 2, 1200.50, '2023-11-15');
INSERT INTO ventas VALUES (2, 'Mouse', 5, 25.00, '2024-01-10');
INSERT INTO ventas VALUES (3, 'Teclado', 3, 45.00, '2024-02-05');

SELECT producto, cantidad, precio FROM ventas
WHERE (precio > 100.0 AND cantidad >= 2)
  OR (fecha BETWEEN '2024-01-01' AND '2024-12-31')
  AND NOT producto = 'Mouse';

CREATE INDEX idx_fecha ON ventas USING RTREE(fecha);
DROP INDEX idx_fecha ON ventas;
DELETE FROM ventas WHERE fecha < '2023-01-01';*/

--DROP TABLE alumnos;

/*
CREATE TABLE alumnos (
  codigo INT PRIMARY KEY INDEX HASH,
  nombre VARCHAR(20) INDEX BTREE,
  ciclo INT INDEX BTREE
);


CREATE TABLE test (
  col1 INT PRIMARY KEY INDEX BTREE,
  col2 FLOAT
);
*/

/*
CREATE TABLE test2 (
  col1 VARCHAR(20) PRIMARY KEY INDEX HASH,
  col2 INT INDEX AVL
);
*/

SELECT * FROM test2 WHERE col1 = 'hola'
