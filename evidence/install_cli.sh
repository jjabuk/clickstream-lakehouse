#!/bin/sh
# Install the pinned Evidence CLI (ADR 0018) to ~/.local/bin, the same steps as the vendor's
# install.sh (https://evidence.studio/install.sh) minus sudo and the install telemetry ping.
# To upgrade: take the version and checksums from
# https://gaamozau3jchzs3r.public.blob.vercel-storage.com/cli/version.json
set -eu

VERSION=0.12.2
BASE=https://gaamozau3jchzs3r.public.blob.vercel-storage.com/cli
DEST="${EVIDENCE_INSTALL_DIR:-$HOME/.local/bin}"

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) platform=darwin-arm64; sum=627cfc542d3cd67edfc1c8d62e06de32102516795378f5a0c3b5104a9cbe7ee4 ;;
  Darwin-x86_64) platform=darwin-x64; sum=bab1203a7ac480d6a3d6748b0e4a2b106697e9be396a4c11ada6f91dce27089f ;;
  Linux-x86_64) platform=linux-x64; sum=a064f83381917372056e2e2ad432d3e068e9b2f747cd48a2cee307363a01b992 ;;
  Linux-aarch64 | Linux-arm64) platform=linux-arm64; sum=ca5e6f671156c0b1f7e25831937fa607f0a4ab5bdc28d194e8f02140bf365ca4 ;;
  *) echo "unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

if [ -x "$DEST/evidence" ] && "$DEST/evidence" version 2>/dev/null | grep -q "$VERSION"; then
  echo "Evidence CLI $VERSION already in $DEST"; exit 0
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
curl -fsSL "$BASE/v$VERSION/evidence-$platform" -o "$tmp"
actual="$( (shasum -a 256 "$tmp" 2>/dev/null || sha256sum "$tmp") | awk '{print $1}')"
[ "$actual" = "$sum" ] || { echo "checksum mismatch for evidence-$platform" >&2; exit 1; }

mkdir -p "$DEST"
install -m 755 "$tmp" "$DEST/evidence"
echo "Installed Evidence CLI $VERSION to $DEST/evidence"
