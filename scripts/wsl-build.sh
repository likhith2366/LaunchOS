#!/usr/bin/env bash
# Inner loop for Windows.
#
# Vapor targets Linux and macOS, so the build runs in WSL2. It also runs in a
# WSL-local directory rather than over /mnt/a: SwiftPM does a lot of small-file IO and
# git work, and on the Windows drive mount that is slow enough to matter (a dependency
# resolve that takes seconds natively takes many minutes over the mount).
#
# Source of truth stays on the Windows side; this script mirrors it into WSL and builds.
#
#   scripts/wsl-build.sh                 # build everything, debug
#   scripts/wsl-build.sh -c release      # build everything, release
#   scripts/wsl-build.sh -t              # build and run tests
set -euo pipefail

export PATH="$HOME/.local/share/swiftly/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
# The Swift toolchain links against libncurses.so.6, which Ubuntu 24.04 does not ship by
# default (it has the wide-character libncursesw instead). scripts/wsl-deps.sh unpacks
# the real libncurses6 into ~/.local/syslibs without needing root; point the loader at it.
if [[ -d "$HOME/.local/syslibs/usr/lib/x86_64-linux-gnu" ]]; then
  export LD_LIBRARY_PATH="$HOME/.local/syslibs/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
fi

WIN_ROOT="${LAUNCHOS_WIN_ROOT:-/mnt/a/Projects/LaunchOs}"
WSL_ROOT="${LAUNCHOS_WSL_ROOT:-$HOME/launchos}"
CONFIG=debug
RUN_TESTS=0
EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -c|--configuration) CONFIG="$2"; shift 2 ;;
    -t|--test) RUN_TESTS=1; shift ;;
    --) shift; EXTRA+=("$@"); break ;;
    *) EXTRA+=("$1"); shift ;;
  esac
done

mkdir -p "$WSL_ROOT"
rsync -a --delete \
  "$WIN_ROOT/Package.swift" \
  "$WIN_ROOT/Sources" \
  "$WIN_ROOT/Tests" \
  "$WSL_ROOT/"
# Package.resolved is mirrored back so the pinned versions are visible on the Windows
# side and land in version control with the rest of the project.
cd "$WSL_ROOT"

if [[ -f "$WIN_ROOT/Package.resolved" ]]; then
  cp "$WIN_ROOT/Package.resolved" ./Package.resolved
fi

echo "==> swift build -c $CONFIG ${EXTRA[*]:-}"
swift build -c "$CONFIG" ${EXTRA[@]+"${EXTRA[@]}"}

if [[ -f ./Package.resolved ]]; then
  cp ./Package.resolved "$WIN_ROOT/Package.resolved"
fi

if [[ "$RUN_TESTS" == "1" ]]; then
  echo "==> swift test -c $CONFIG"
  swift test -c "$CONFIG"
fi

echo "==> binaries in $WSL_ROOT/.build/$CONFIG"
