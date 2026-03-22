{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    nci.url = "github:yusdacra/nix-cargo-integration";
    nci.inputs.nixpkgs.follows = "nixpkgs";
    parts.url = "github:hercules-ci/flake-parts";
    parts.inputs.nixpkgs-lib.follows = "nixpkgs";
  };

  outputs = inputs @ {
    parts,
    nci,
    ...
  }:
    parts.lib.mkFlake {inherit inputs;} {
      systems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
      imports = [
        nci.flakeModule
        parts.flakeModules.easyOverlay
      ];
      perSystem = {
        config,
        pkgs,
        ...
      }: let
        outputs = config.nci.outputs;
        oxenBuildInputs = with pkgs;
          [
            pkg-config
            mold
            clang
            llvmPackages.libclang.lib
            openssl
          ]
          ++ lib.optionals (pkgs.stdenv.isDarwin) [
            darwin.IOKit
            darwin.apple_sdk.frameworks.SystemConfiguration
          ];
        commonDrvConfig = {
          deps.stdenv = pkgs.clangStdenv;
          mkDerivation = {
            buildInputs = oxenBuildInputs;
          };
          env = {
            LIBCLANG_PATH = pkgs.lib.makeLibraryPath [pkgs.llvmPackages.libclang.lib];
            RUST_BACKTRACE = 1;
            RUST_LOG = "info";
            RUSTFLAGS = "-C target-cpu=native";
          };
        };
      in {
        nci.projects."Oxen" = {
          path = ./.;
          export = true;
        };
        nci.crates = {
          "oxen-cli" = {
            depsDrvConfig = commonDrvConfig;
            drvConfig = commonDrvConfig;
          };
          "oxen-server" = {
            depsDrvConfig = commonDrvConfig;
            drvConfig = commonDrvConfig;
            profiles = {
              dev.runTests = false;
              release.runTests = false;
            };
          };
          "liboxen" = {
            depsDrvConfig = commonDrvConfig;
            drvConfig = commonDrvConfig;
            profiles = {
              dev.runTests = false;
              release.runTests = false;
            };
          };
        };

        # nix fmt
        formatter = pkgs.alejandra;

        # export the project devshell as the default devshell
        devShells = {
          # nix develop -c $SHELL
          default = outputs."Oxen".devShell.overrideAttrs (old: {
            buildInputs = old.buildInputs ++ oxenBuildInputs;

            packages = [
              pkgs.rust-analyzer
            ];

            LIBCLANG_PATH = pkgs.lib.makeLibraryPath [pkgs.llvmPackages.libclang.lib];
            RUST_BACKTRACE = 1;
            RUST_LOG = "info";
            RUSTFLAGS = "-C target-cpu=native";
          });
        };

        # export the release package of the crate as default package
        packages = rec {
          # nix build .#oxen-cli
          oxen-cli = outputs."oxen-cli".packages.release;
          # nix build .#oxen-server
          oxen-server = outputs."oxen-server".packages.release;
          # nix build .#liboxen
          liboxen = outputs."liboxen".packages.release;
          # nix build .#oxen-cli-oci
          oci-oxen-cli = pkgs.dockerTools.buildImage {
            name = "oxenai/oxen-cli";
            tag = oxen-cli.version;
            created = "now";
            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [oxen-cli];
              pathsToLink = ["${oxen-cli}/bin"];
            };
            config = {
              Cmd = ["${oxen-cli}/bin/oxen"];
            };
          };
          # nix build .#oxen-server-oci
          oci-oxen-server = pkgs.dockerTools.buildImage {
            name = "oxenai/oxen-server";
            tag = oxen-cli.version;
            created = "now";
            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [oxen-server];
              pathsToLink = ["${oxen-server}/bin"];
            };
            config = {
              Cmd = ["${oxen-server}/bin/oxen-server"];
            };
          };
          # nix build
          default = oxen-cli;
        };

        # Overlay that can be imported so you can access the packages using oxen.overlay
        overlayAttrs = {
          inherit (config.packages) oxen-cli oxen-server liboxen oci-oxen-cli oci-oxen-server;
        };
      };
    };
}
