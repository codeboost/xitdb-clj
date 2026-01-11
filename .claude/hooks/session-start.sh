#!/bin/bash
set -euo pipefail

# Only run in Claude Code remote environments
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"

# Install Clojure CLI tools if not present
if ! command -v clojure &> /dev/null; then
  echo "Installing Clojure CLI tools..."

  CLOJURE_PREFIX="$HOME/.local"
  mkdir -p "$CLOJURE_PREFIX/bin" "$CLOJURE_PREFIX/lib"

  # Download and run installer with local prefix
  curl -fsSL https://github.com/clojure/brew-install/releases/latest/download/linux-install.sh -o /tmp/linux-install.sh
  chmod +x /tmp/linux-install.sh
  /tmp/linux-install.sh --prefix "$CLOJURE_PREFIX"
  rm /tmp/linux-install.sh

  # Add to PATH for this session
  export PATH="$CLOJURE_PREFIX/bin:$PATH"

  # Persist PATH for future commands in this session
  if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
    echo "export PATH=\"$CLOJURE_PREFIX/bin:\$PATH\"" >> "$CLAUDE_ENV_FILE"
  fi
fi

# Download project dependencies
echo "Downloading project dependencies..."
clojure -P

echo "Session setup complete."
