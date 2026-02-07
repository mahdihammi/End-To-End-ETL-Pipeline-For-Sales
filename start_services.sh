#!/bin/bash

set -e

echo "Setting up permissions for Astro project..."

# Option 1: World writable (quick and easy)
chmod -R 777 include

# Option 2: Set to Astro user (more secure, requires sudo)
# sudo chown -R 50000:50000 include/data include/duckdb logs
# chmod -R 775 include/data include/duckdb logs

echo "✓ Done!"