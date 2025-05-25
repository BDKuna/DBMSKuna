
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

/* 
-- 1) Redefinimos test2 con una columna espacial "coord"
    DROP TABLE IF EXISTS test2;

CREATE TABLE test2 (
  col1 VARCHAR(20) PRIMARY KEY INDEX HASH,
  col2 INT INDEX AVL,
  coord POINT guarda '(x,y)' INDEX RTREE
);

-- 2) Insertamos algunos puntos (x,y)
INSERT INTO test2 VALUES
  ('A', 10, (1.0, 2.0)),
  ('B', 20, (3.5, 1.5)),
  ('C', 30, (5.0, 5.0)),
  ('D', 40, (2.2, 3.8)),
  ('E', 50, (4.4, 0.9));

-- 3) Crea el índice
CREATE INDEX idx_test2_coord
  ON test2 USING RTREE (coord);

-- 4) Consulta por rango:  
SELECT col1, col2, coord
FROM test2
WHERE coord WITHIN RECTANGLE (1.0, 1.0, 4.0, 4.0)
OR
coord WITHIN CIRCLE (1.0, 1.0, 4.0)

-- 5) Consulta k-NN: 
SELECT col1, col2, coord
FROM test2
WHERE coord KNN (3.0, 2.0, 3)

DROP INDEX idx_test2_coord ON test2;
*/

DROP TABLE test5;

CREATE TABLE test5 (
  col1 VARCHAR(20) PRIMARY KEY INDEX HASH,
  col2 INT INDEX AVL,
  coord POINT INDEX RTREE
);

INSERT INTO test5 VALUES ('A', 10, (1.0, 2.0));

INSERT INTO lugares VALUES
  (1, (12.046374, 77.042793), 'Plaza Mayor');
INSERT INTO lugares VALUES
  (2, (12.043180, 77.028240), 'Miraflores');
INSERT INTO lugares VALUES
  (3, (12.120000, 77.030000), 'Barranco');

SELECT * FROM lugares
WHERE ubicacion WITHIN RECTANGLE (12.0, 77.0, 13.0, 78.0);

SELECT * FROM lugares
WHERE ubicacion WITHIN CIRCLE (12.05, 77.03, 0.02);

SELECT id, nombre FROM lugares
WHERE ubicacion KNN (12.05, 77.04, 2);

INSERT INTO test5 VALUES ('B', 20, (3.5, 1.5));

INSERT INTO test5 VALUES ('C', 30, (5.0, 5.0));

INSERT INTO test5 VALUES ('D', 40, (2.2, 3.8));

INSERT INTO test5 VALUES ('E', 50, (4.4, 0.9));

/*
SELECT col1, col2, coord
FROM test5
WHERE coord WITHIN RECTANGLE (4.0, 4.0, 6.0, 6.0)
OR
coord WITHIN CIRCLE (2.0, 2.0, 2.0);
*/

SELECT col1, col2, coord
FROM test5
WHERE coord KNN (3.0, 2.0, 1);
