{ pkgs ? import <nixpkgs> {} }:

# Trae todos los atributos de pkgs al scope
with pkgs;

mkShell {
  buildInputs = [
    python3
    python3Packages.rtree
    python3Packages.psycopg2
    libspatialindex    # la C‐lib de SpatialIndex
    postgresql         # cliente de Postgres (libpq)
    zlib               # para libz.so.1
  ];

  shellHook = ''
    # Ahora libspatialindex, postgresql y zlib están en scope gracias al with pkgs
    export LD_LIBRARY_PATH=${libspatialindex}/lib:${postgresql}/lib:${zlib}/lib:$LD_LIBRARY_PATH
  '';
}

