#!/usr/bin/env bash
# One-time WSL setup: Swift toolchain plus the one system library Ubuntu 24.04 is
# missing, both installed without root.
#
# Ubuntu 24.04 ships libncursesw (wide) but not libncurses.so.6, which the Swift
# toolchain links against. If you have sudo, `sudo apt-get install -y libncurses6` is
# the normal fix and this script's unpack step is unnecessary. Everything here works
# without root, which is what makes it usable on a locked-down machine.
set -euo pipefail

export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
SYSLIBS="$HOME/.local/syslibs"

if ! [[ -x "$HOME/.local/share/swiftly/bin/swift" ]]; then
  echo "==> installing Swift via swiftly"
  cd "$HOME"
  curl -sSLO "https://download.swift.org/swiftly/linux/swiftly-$(uname -m).tar.gz"
  tar zxf "swiftly-$(uname -m).tar.gz"
  ./swiftly init --quiet-shell-followup --assume-yes
fi

# libncurses6/libtinfo6: the toolchain binaries link against them.
# zlib1g/zlib1g-dev: AsyncHTTPClient links -lz, and Ubuntu ships no libz.so without
# the -dev package.
if ! [[ -f "$SYSLIBS/usr/lib/x86_64-linux-gnu/libncurses.so.6" ]] \
  || ! [[ -f "$SYSLIBS/usr/lib/x86_64-linux-gnu/libz.so" ]]; then
  echo "==> unpacking libncurses6, libtinfo6, zlib1g, zlib1g-dev into $SYSLIBS (no root needed)"
  mkdir -p "$SYSLIBS/debs"
  cd "$SYSLIBS/debs"
  apt-get download libncurses6 libtinfo6 zlib1g zlib1g-dev
  for deb in ./*.deb; do dpkg -x "$deb" "$SYSLIBS"; done
fi

# Put the libraries inside the toolchain's own RUNPATH ($ORIGIN/../lib/swift/linux)
# rather than relying on LD_LIBRARY_PATH. SwiftPM's build system spawns swiftc for the
# link steps in a sanitized environment, so an exported LD_LIBRARY_PATH reaches the
# compile but not the link, and the build fails halfway through.
TOOLCHAIN="$(dirname "$(dirname "$(readlink -f "$HOME/.local/share/swiftly/bin/swiftc")")")"
RUNPATH_DIR="$TOOLCHAIN/lib/swift/linux"
# The same directory is already on the link search path, so libz.so lands here too.
if [[ -d "$RUNPATH_DIR" ]]; then
  LIBS="$SYSLIBS/usr/lib/x86_64-linux-gnu"
  if ! [[ -e "$RUNPATH_DIR/libncurses.so.6" ]]; then
    echo "==> placing libncurses/libtinfo in the toolchain runpath"
    cp -n "$LIBS/libncurses.so.6.4" "$LIBS/libtinfo.so.6.4" "$RUNPATH_DIR/"
    ln -sf libncurses.so.6.4 "$RUNPATH_DIR/libncurses.so.6"
    ln -sf libtinfo.so.6.4 "$RUNPATH_DIR/libtinfo.so.6"
  fi
  if ! [[ -e "$RUNPATH_DIR/libz.so" ]]; then
    echo "==> placing libz in the toolchain link path"
    cp -n "$LIBS"/libz.so.1.* "$RUNPATH_DIR/"
    zsofile="$(cd "$RUNPATH_DIR" && ls libz.so.1.* | head -1)"
    ln -sf "$zsofile" "$RUNPATH_DIR/libz.so.1"
    ln -sf "$zsofile" "$RUNPATH_DIR/libz.so"
  fi
fi

export PATH="$HOME/.local/share/swiftly/bin:$PATH"
swift --version
echo "==> ok"
