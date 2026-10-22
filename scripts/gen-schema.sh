#!/usr/bin/env bash
# Regenerates Sources/LaunchCore/Schema.swift from schema.sql so the schema has one
# source of truth and migrate() needs no resource bundle at runtime.
set -euo pipefail
cd "$(dirname "$0")/.."
src=Sources/LaunchCore/schema.sql
if grep -qF '\' "$src" || grep -qF '"""' "$src"; then
  echo "schema.sql contains a backslash or a triple quote; the raw embedding below would break" >&2
  exit 1
fi
{
  printf '// Generated from Sources/LaunchCore/schema.sql by scripts/gen-schema.sh. Do not edit.\n\nlet schemaSQL = """\n'
  cat "$src"
  printf '"""\n'
} > Sources/LaunchCore/Schema.swift
echo "wrote Sources/LaunchCore/Schema.swift ($(wc -l < Sources/LaunchCore/Schema.swift) lines)"
