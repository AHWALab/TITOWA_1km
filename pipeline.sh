#!/usr/bin/bash
set -euo pipefail
shopt -s nullglob

# Generic hourly runner for TITOV2 HighRes.
# - Waits 5 minutes (to allow data arrival) then runs orchestrator
# - Keeps outputs untouched; only logs the run with a timestamped logfile

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$PROJECT_ROOT/data/logs"
mkdir -p "$LOG_DIR"
RUN_TS=$(date -u +%Y%m%dT%H%M%S)
LOG_FILE="$LOG_DIR/tito_hourly_${RUN_TS}.log"

exec > >(stdbuf -oL -eL tee -a "$LOG_FILE") 2>&1

echo "==== TITO hourly run started at $(date -u --iso-8601=seconds) ===="
echo "Log file: $LOG_FILE"

# Locate Conda (cron-safe)
CONDA_BASE=""
for cand in "$HOME/miniconda3" "$HOME/anaconda3" "$HOME/mambaforge" "/opt/conda"; do
  if [ -d "$cand" ]; then
    CONDA_BASE="$cand"
    break
  fi
done

if [ -n "$CONDA_BASE" ] && [ -f "$CONDA_BASE/etc/profile.d/conda.sh" ]; then
  # shellcheck disable=SC1090
  source "$CONDA_BASE/etc/profile.d/conda.sh"
elif [ -n "$CONDA_BASE" ] && [ -x "$CONDA_BASE/bin/conda" ]; then
  export PATH="$CONDA_BASE/bin:$PATH"
  eval "$("$CONDA_BASE/bin/conda" shell.bash hook 2>/dev/null)" || true
fi

# Fallback to user's bashrc if conda not yet on PATH
if ! command -v conda >/dev/null 2>&1; then
  if [ -f "$HOME/.bashrc" ]; then
    # shellcheck disable=SC1090
    source "$HOME/.bashrc"
  fi
  if command -v conda >/dev/null 2>&1; then
    eval "$(conda shell.bash hook 2>/dev/null)" || true
  fi
fi

if ! command -v conda >/dev/null 2>&1; then
  echo "conda not found; please install or adjust PATH."
  exit 1
fi

echo "Activating conda env 'tito_env'..."
set +u
conda activate tito_env
set -u

echo "Running TITOV2 orchestrator..."
cd "$PROJECT_ROOT"
PYTHONUNBUFFERED=1 python orchestrator.py westafrica1km_config.py
echo "Orchestrator complete."

set +u
conda deactivate || true
set -u

echo "==== TITO hourly run finished at $(date -u --iso-8601=seconds) ===="