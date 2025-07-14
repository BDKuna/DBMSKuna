{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    python3                     # tu intérprete Python
    python3Packages.rtree       # binding Python de libspatialindex
    gcc                         # aporta libstdc++.so.6 al entorno
    libspatialindex             # la librería C++ de spatial index :contentReference[oaicite:0]{index=0}
  ];

  # Opcional, para forzar que LD_LIBRARY_PATH vea la .so de spatialindex
  shellHook = ''
    export LD_LIBRARY_PATH=${pkgs.libspatialindex}/lib:$LD_LIBRARY_PATH
  '';
}

