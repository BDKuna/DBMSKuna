{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.python3
    pkgs.libspatialindex
    pkgs.gcc
    pkgs.libGL
    pkgs.libGLU
    #pkgs.qt5
  ];

  shellHook = ''
    export LD_LIBRARY_PATH=${pkgs.stdenv.cc.cc.lib}/lib/:${pkgs.libGL}/lib/:${pkgs.libGLU}/lib/
    export QT_PLUGIN_PATH=${pkgs.qt5.qtbase}/${pkgs.qt5.qtbase.qtPluginPrefix}
  '';
}

