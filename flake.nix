# Host tooling extracted from Nova b69a1e5765ecc65ac3b00acdfe2f16e2f46a5dda.
# Keep flake.lock aligned with the consuming applications' tested input pins.
{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { nixpkgs, fenix, ... }: let
    systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
  in {
    devShells = nixpkgs.lib.genAttrs systems (system: let
      pkgs = import nixpkgs { inherit system; };
      # Nova's Fenix stable host toolchain; entering the shell needs no rustup.
      host = fenix.packages.${system}.stable;
      hostToolchain = host.withComponents [
        "cargo"
        "rustc"
        "clippy"
        "rustfmt"
        "rust-src"
      ];
    in {
      default = pkgs.mkShell {
        nativeBuildInputs = [
          hostToolchain
          pkgs.pkg-config
          # aws-lc supports a CMake fallback; bundled WebP uses stdenv's C/C++.
          pkgs.cmake
        ];
        buildInputs = [ pkgs.fontconfig pkgs.freetype ];
        LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [ pkgs.fontconfig pkgs.freetype ];
      };
    });
  };
}
