{ pkgs ? import <nixpkgs> {} }:

with pkgs;

mkShell {
  buildInputs = [
    python3
    # instala numpy y pandas directamente en el entorno de python
    (python3.withPackages (ps: with ps; [ numpy pandas rtree psycopg2 ]))
    libspatialindex
    postgresql
    zlib
    gcc
  ];

  shellHook = ''
    # Aquí $LD_LIBRARY_PATH se expandirá al inicio de la sesión,
    # y lib lo interpola Nix al generar el entorno.
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${gcc}/lib:${libspatialindex}/lib:${postgresql}/lib:${zlib}/lib
  '';
}

