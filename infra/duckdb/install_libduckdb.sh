#!/bin/sh
# Install the pinned DuckDB shared library on Linux (ADR 0024), for the Airflow image and CI.
#
# dbt v2 loads a system libduckdb when it finds one and otherwise downloads its own driver, which
# for dbt 2.0.12 is DuckDB 1.5.4. A warehouse file written by DuckDB 2.0 cannot be opened by 1.5,
# so every engine that touches the warehouse runs the same DuckDB: the Python package
# (duckdb==2.0.0 in pyproject.toml), Homebrew's duckdb on macOS, and this library on Linux.
#
# To upgrade: take the version and the sha256 digests of duckdb-shared-libs-linux-*.tar.gz from
# https://github.com/duckdb/duckdb/releases (the asset list shows each digest).
# Needs root (it writes to /usr/local/lib).
set -eu

VERSION=2.0.0
DEST=/usr/local/lib

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) arch=amd64; sum=993ad89e7dfca66cd6da994c2cad090c2fc160ddf9f550498a098ab4bb13cfb1 ;;
  Linux-aarch64 | Linux-arm64) arch=arm64; sum=c60eecf91143ca6bbac55c4f4d40dc22520488b0ac257c86e0fe9a2b451cf2b7 ;;
  Darwin-*) echo "macOS: install DuckDB $VERSION with Homebrew (brew install duckdb)" >&2; exit 1 ;;
  *) echo "unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

tmp="$(mktemp -d)"
trap 'rm -r "$tmp"' EXIT
archive="duckdb-shared-libs-linux-$arch.tar.gz"
curl -fsSL "https://github.com/duckdb/duckdb/releases/download/v$VERSION/$archive" -o "$tmp/$archive"
actual="$(sha256sum "$tmp/$archive" | awk '{print $1}')"
[ "$actual" = "$sum" ] || { echo "checksum mismatch for $archive" >&2; exit 1; }

# Python's tarfile, because the Airflow image has no gzip.
python3 -c 'import sys, tarfile; tarfile.open(sys.argv[1]).extract("libduckdb.so", sys.argv[2], filter="data")' \
  "$tmp/$archive" "$tmp"
install -m 755 "$tmp/libduckdb.so" "$DEST/libduckdb.so"
ldconfig
echo "Installed libduckdb $VERSION to $DEST/libduckdb.so"
