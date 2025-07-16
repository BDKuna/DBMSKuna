
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
DROP TABLE IF EXISTS text_test;

CREATE TABLE text_test (
  id INT PRIMARY KEY INDEX HASH,
  titulo VARCHAR(20),
  contenido TEXT
);

INSERT INTO text_test VALUES (1, 'texto_1', 'Normal Attack
Performs up to 5 consecutive spear strikes.');
INSERT INTO text_test VALUES (2, 'texto_2', 'Charged Attack
Consumes a certain amount of Stamina to lunge forward, dealing damage to opponents along the way.
');
INSERT INTO text_test VALUES (3, 'texto_3', 'Plunging Attack
Plunges from mid-air to strike the ground below, damaging opponents along the path and dealing AoE DMG upon impact.');
INSERT INTO text_test VALUES (4, 'texto_4', 'Summons Guoba, who will continuously breathe fire at opponents, dealing AoE Pyro DMG.');
INSERT INTO text_test VALUES (5, 'texto_5', 'Displaying her mastery over both fire and polearms, Xiangling sends a Pyronado whirling around her.
The Pyronado will move with your character for the ability s duration, dealing Pyro DMG to all opponents in its path.');
INSERT INTO text_test VALUES (6, 'texto_6', 'Increases the flame range of Guoba by 20%.');
INSERT INTO text_test VALUES (7, 'texto_7', 'When Guoba Attack s effect ends, Guoba leaves a chili pepper on the spot where it disappeared. Picking up a chili pepper increases ATK by 10% for 10s.');
INSERT INTO text_test VALUES (8, 'texto_8', 'When Xiangling cooks an ATK-boosting dish perfectly, she has a 12% chance to receive double the product.');

CREATE INDEX idx_gist ON text_test USING GIST (contenido);

SELECT * FROM text_test WHERE contenido @@ 'Guoba Pyro DMG';