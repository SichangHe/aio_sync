#!/bin/bash
set -euo pipefail
ruff check --fix src tests
basedpyright src tests
