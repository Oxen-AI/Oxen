#!/usr/bin/env bash
#
# install-pre-reqs.sh
#
# Installs all development prerequisites for the Oxen project.
# Works on macOS and Linux. Idempotent: safe to run multiple times.
#
set -euo pipefail

###############################################################################
# Helpers
###############################################################################

info()  { printf "\033[1;34m[INFO]\033[0m  %s\n" "$*"; }
ok()    { printf "\033[1;32m[OK]\033[0m    %s\n" "$*"; }
warn()  { printf "\033[1;33m[WARN]\033[0m  %s\n" "$*"; }

command_exists() { command -v "$1" &>/dev/null; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

###############################################################################
# Load pinned tool versions
###############################################################################

TOOL_VERSIONS_FILE="$REPO_ROOT/tool-versions.env"
if [ ! -f "$TOOL_VERSIONS_FILE" ]; then
    echo "ERROR: $TOOL_VERSIONS_FILE not found"; exit 1
fi
# shellcheck disable=SC1090,SC1091
. "$TOOL_VERSIONS_FILE"

OS="$(uname -s)"
case "$OS" in
    Darwin) PLATFORM="macos" ;;
    Linux)  PLATFORM="linux" ;;
    *)      echo "Unsupported OS: $OS"; exit 1 ;;
esac

info "Detected platform: $PLATFORM"

# Track which tools were freshly installed so we can print shell setup hints.
INSTALLED_CARGO=false
INSTALLED_BREW=false
INSTALLED_UV=false

###############################################################################
# 1. System packages (compilers, libraries needed by Rust crates)
###############################################################################

info "Checking system packages..."

if [ "$PLATFORM" = "macos" ]; then
    # Xcode Command Line Tools provide clang, libclang, pkg-config, etc.
    if ! xcode-select -p &>/dev/null; then
        info "Installing Xcode Command Line Tools..."
        xcode-select --install
        echo "Please complete the Xcode CLI Tools installation, then re-run this script."
        exit 1
    else
        ok "Xcode Command Line Tools already installed"
    fi

    # Homebrew
    if ! command_exists brew; then
        info "Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        INSTALLED_BREW=true
        # Make brew available for the rest of this script
        if [ -x /opt/homebrew/bin/brew ]; then
            eval "$(/opt/homebrew/bin/brew shellenv)"
        elif [ -x /usr/local/bin/brew ]; then
            eval "$(/usr/local/bin/brew shellenv)"
        fi
    else
        ok "Homebrew already installed"
    fi

elif [ "$PLATFORM" = "linux" ]; then
    if ! command_exists apt-get || ! command_exists dpkg; then
        warn "Unsupported Linux distro for automatic package install (requires apt + dpkg)."
        exit 1
    fi

    # Build essentials, OpenSSL, libclang, pkg-config
    NEEDED_PKGS=()
    dpkg -s build-essential     &>/dev/null || NEEDED_PKGS+=(build-essential)
    dpkg -s cmake               &>/dev/null || NEEDED_PKGS+=(cmake)
    dpkg -s pkg-config          &>/dev/null || NEEDED_PKGS+=(pkg-config)
    dpkg -s openssl             &>/dev/null || NEEDED_PKGS+=(openssl)
    dpkg -s libssl-dev          &>/dev/null || NEEDED_PKGS+=(libssl-dev)
    dpkg -s clang               &>/dev/null || NEEDED_PKGS+=(clang)
    dpkg -s libclang-dev        &>/dev/null || NEEDED_PKGS+=(libclang-dev)
    dpkg -s curl                &>/dev/null || NEEDED_PKGS+=(curl)
    dpkg -s libavcodec-dev      &>/dev/null || NEEDED_PKGS+=(libavcodec-dev)
    dpkg -s libavformat-dev     &>/dev/null || NEEDED_PKGS+=(libavformat-dev)
    dpkg -s libavutil-dev       &>/dev/null || NEEDED_PKGS+=(libavutil-dev)
    dpkg -s libavfilter-dev     &>/dev/null || NEEDED_PKGS+=(libavfilter-dev)
    dpkg -s libavdevice-dev     &>/dev/null || NEEDED_PKGS+=(libavdevice-dev)
    dpkg -s libjpeg-turbo-progs &>/dev/null || NEEDED_PKGS+=(libjpeg-turbo-progs)

    if [ ${#NEEDED_PKGS[@]} -gt 0 ]; then
        info "Installing system packages: ${NEEDED_PKGS[*]}"
        sudo apt-get update -y
        sudo apt-get install -y "${NEEDED_PKGS[@]}"
    else
        ok "All system packages already installed"
    fi
fi

###############################################################################
# 2. Rust toolchain (rustup + cargo)
###############################################################################

info "Checking Rust toolchain..."

if ! command_exists rustup; then
    info "Installing rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    INSTALLED_CARGO=true
else
    ok "rustup already installed"
fi

# Ensure cargo is on PATH for the rest of this script, whether rustup was
# just installed or was already present but not sourced (e.g. non-interactive shell).
CARGO_ENV="${CARGO_HOME:-$HOME/.cargo}/env"
if [ -f "$CARGO_ENV" ]; then
    # shellcheck source=/dev/null
    . "$CARGO_ENV"
fi

# Ensure the project's required toolchain is installed.
# rust-toolchain.toml in oxen-rust/ will be picked up automatically by cargo,
# but we can pre-install it so the first build doesn't stall.
RUST_TOOLCHAIN_FILE="$REPO_ROOT/oxen-rust/rust-toolchain.toml"

if [ -f "$RUST_TOOLCHAIN_FILE" ]; then
    REQUIRED_CHANNEL=$(grep '^channel' "$RUST_TOOLCHAIN_FILE" | sed 's/.*= *"\(.*\)"/\1/')
    if [ -n "$REQUIRED_CHANNEL" ]; then
        if rustup toolchain list | grep -q "^${REQUIRED_CHANNEL}"; then
            ok "Rust toolchain $REQUIRED_CHANNEL already installed"
        else
            info "Installing Rust toolchain $REQUIRED_CHANNEL..."
            rustup toolchain install "$REQUIRED_CHANNEL"
        fi
    fi
fi

###############################################################################
# 3. Cargo tools
###############################################################################

info "Checking cargo tools..."

installed_cargo_version() {
    # Extracts the installed version of a crate from `cargo install --list`.
    # Output format: "crate-name v1.2.3:"
    local crate="$1"
    cargo install --list 2>/dev/null | grep "^${crate} v" | sed 's/.*v\([^ :]*\).*/\1/'
}

install_cargo_tool() {
    local cmd="$1"
    local crate="$2"
    local version="$3"
    local extra_flags="${4:-}"

    # '---' means "no pinned version; install latest".
    local pinned=true
    if [ "$version" = "---" ]; then
        pinned=false
    fi

    if command_exists "$cmd"; then
        if $pinned; then
            local installed
            installed="$(installed_cargo_version "$crate")"
            if [ "$installed" = "$version" ]; then
                ok "$cmd already installed at $version"
            else
                warn "$cmd version mismatch: installed=$installed, want=$version. Reinstalling..."
                # shellcheck disable=SC2086
                cargo install $extra_flags "$crate" --version "$version" --force
            fi
        else
            ok "$cmd already installed (latest)"
        fi
    else
        if $pinned; then
            info "Installing $crate@$version..."
            # shellcheck disable=SC2086
            cargo install $extra_flags "$crate" --version "$version"
        else
            info "Installing $crate (latest)..."
            # shellcheck disable=SC2086
            cargo install $extra_flags "$crate"
        fi
    fi
}

install_cargo_tool bacon          bacon          "$BACON_VERSION"
install_cargo_tool cargo-machete  cargo-machete  "$CARGO_MACHETE_VERSION"
install_cargo_tool cargo-llvm-cov cargo-llvm-cov "$CARGO_LLVM_COV_VERSION"
install_cargo_tool cargo-sort     cargo-sort     "$CARGO_SORT_VERSION"
install_cargo_tool cargo-nextest  cargo-nextest  "$CARGO_NEXTEST_VERSION" "--locked"

###############################################################################
# 4. uv (Python package/project manager)
###############################################################################

info "Checking uv..."

if command_exists uv; then
    ok "uv already installed"
else
    info "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    INSTALLED_UV=true
    # Make uv available for the rest of this script
    UV_BIN_DIR="${XDG_BIN_HOME:-$HOME/.local/bin}"
    if [ -d "$UV_BIN_DIR" ]; then
        export PATH="$UV_BIN_DIR:$PATH"
    fi
fi

###############################################################################
# 5. shellcheck (shell script linter)
###############################################################################

install_shellcheck() {
    local version="$1"

    if [ "$PLATFORM" = "macos" ]; then
        info "Installing shellcheck ${version} via Homebrew..."
        # Unlink any existing shellcheck to avoid conflicts
        brew unlink shellcheck &>/dev/null || true
        brew install "shellcheck@${version}"
        brew link --overwrite "shellcheck@${version}"
    elif [ "$PLATFORM" = "linux" ]; then
        info "Installing shellcheck ${version} via apt..."
        sudo apt-get update -y
        # Use --allow-downgrades to force the pinned version even if a newer one is installed
        sudo apt-get install -y --allow-downgrades "shellcheck=${version}*"
    fi
}

info "Checking shellcheck..."

NEED_SHELLCHECK=false
if command_exists shellcheck; then
    INSTALLED_SC_VERSION="$(shellcheck --version | grep '^version:' | awk '{print $2}')"
    if [ "$INSTALLED_SC_VERSION" = "$SHELLCHECK_VERSION" ]; then
        ok "shellcheck already installed at $SHELLCHECK_VERSION"
    else
        warn "shellcheck version mismatch: installed=$INSTALLED_SC_VERSION, want=$SHELLCHECK_VERSION"
        NEED_SHELLCHECK=true
    fi
else
    NEED_SHELLCHECK=true
fi

if $NEED_SHELLCHECK; then
    install_shellcheck "$SHELLCHECK_VERSION"
    ok "shellcheck ${SHELLCHECK_VERSION} installed"
fi

###############################################################################
# 6. pre-commit (installed as a uv tool)
###############################################################################

info "Checking pre-commit..."

if command_exists pre-commit; then
    ok "pre-commit already installed"
else
    info "Installing pre-commit via uv..."
    uv tool install pre-commit
fi

###############################################################################
# 7. Install repo pre-commit hooks
###############################################################################

if [ -f "$REPO_ROOT/.pre-commit-config.yaml" ]; then
    info "Installing pre-commit hooks..."
    (cd "$REPO_ROOT" && pre-commit install)
    ok "Pre-commit hooks installed"
fi

###############################################################################
# Done
###############################################################################

echo ""
info "All prerequisites installed successfully!"
info "You can now build the project:"
info "  cd oxen-rust && cargo build --workspace"
info "  cd oxen-python && uv sync --verbose"

# Print shell configuration hints for any tools we freshly installed.
if $INSTALLED_CARGO || $INSTALLED_BREW || $INSTALLED_UV; then
    echo ""
    warn "One or more tools were freshly installed. To make them available in"
    warn "new shell sessions, add the following to your ~/.bashrc or ~/.zshrc:"
    echo ""
fi

if $INSTALLED_CARGO; then
    echo '  # Rust / Cargo'
    # shellcheck disable=SC2016
    echo '  . "$HOME/.cargo/env"'
    echo ""
fi

if $INSTALLED_BREW; then
    if [ -x /opt/homebrew/bin/brew ]; then
        BREW_PATH="/opt/homebrew/bin/brew"
    else
        BREW_PATH="/usr/local/bin/brew"
    fi
    echo '  # Homebrew'
    echo "  eval \"\$(${BREW_PATH} shellenv)\""
    echo ""
fi

if $INSTALLED_UV; then
    echo '  # uv'
    echo "  export PATH=\"${UV_BIN_DIR:-$HOME/.local/bin}:\$PATH\""
    echo ""
fi
