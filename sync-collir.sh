#!/bin/bash
# Sync Brix code to Collir and rebuild
set -e

echo "=== Brix Sync → Collir ==="
LOCAL_VERSION=$(grep 'version' pyproject.toml | head -1 | cut -d'"' -f2)
REMOTE_VERSION_PRE=$(ssh collir "docker exec brix-mcp brix --version 2>/dev/null" | awk '{print $NF}')
echo "Local: $LOCAL_VERSION → Remote: $REMOTE_VERSION_PRE"

# 0. Pre-flight check: Welche Migrationen sind neu?
echo "→ pre-flight check..."
LOCAL_MIGRATION=$(PYTHONPATH=src python3 -c "
from brix.migrations import MIGRATIONS
print(max(m['version'] for m in MIGRATIONS))
")
REMOTE_MIGRATION_PRE=$(ssh collir "docker exec brix-mcp python3 -c \"
import sqlite3
db = sqlite3.connect('/root/.brix/brix.db')
print(db.execute('SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1').fetchone()[0])
\"")

echo "  Migrationen: Remote v$REMOTE_MIGRATION_PRE → Local v$LOCAL_MIGRATION"

if [ "$LOCAL_MIGRATION" -gt "$REMOTE_MIGRATION_PRE" ]; then
  # Zeige was die neuen Migrationen machen
  echo ""
  echo "  Neue Migrationen die auf Collir laufen werden:"
  PYTHONPATH=src python3 -c "
from brix.migrations import MIGRATIONS
remote = $REMOTE_MIGRATION_PRE
new = [m for m in MIGRATIONS if m['version'] > remote]
risky_keywords = ['DROP', 'DELETE', 'RENAME', 'ALTER TABLE.*DROP', 'UPDATE.*SET']
import re
for m in sorted(new, key=lambda x: x['version']):
    sql = m.get('sql', '')
    fn = m.get('up_fn', '')
    risk = ''
    for kw in ['DROP', 'DELETE FROM', 'RENAME', 'UPDATE']:
        if kw in sql.upper():
            risk = ' ⚠ DESTRUCTIVE'
            break
    if fn:
        print(f\"    v{m['version']}: [Python] {fn}{risk}\")
    else:
        # Erste Zeile der SQL als Zusammenfassung
        first_line = sql.strip().split('\n')[0][:80]
        print(f\"    v{m['version']}: {first_line}{risk}\")
"
  echo ""

  # Check für destruktive Migrationen
  HAS_DESTRUCTIVE=$(PYTHONPATH=src python3 -c "
from brix.migrations import MIGRATIONS
remote = $REMOTE_MIGRATION_PRE
new = [m for m in MIGRATIONS if m['version'] > remote]
destructive = any(kw in m.get('sql', '').upper() for m in new for kw in ['DROP', 'DELETE FROM', 'RENAME'])
print('yes' if destructive else 'no')
")

  if [ "$HAS_DESTRUCTIVE" = "yes" ]; then
    echo "  ⚠ WARNUNG: Destruktive Migrationen gefunden!"
    echo "  Collir-DB wird VOR dem Sync gesichert, aber bitte prüfen."
    echo ""
    read -p "  Trotzdem weitermachen? [y/N] " CONFIRM
    if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
      echo "  Abgebrochen."
      exit 1
    fi
  else
    echo "  ✓ Alle Migrationen sind additiv (kein DROP/DELETE/RENAME)"
  fi
else
  echo "  ✓ Keine neuen Migrationen"
fi

# Collir DB-Statistiken
echo ""
echo "  Collir DB-Inhalt der geschützt wird:"
ssh collir "docker exec brix-mcp python3 -c \"
import sqlite3
db = sqlite3.connect('/root/.brix/brix.db')
p = db.execute('SELECT COUNT(*) FROM pipelines').fetchone()[0]
h = db.execute('SELECT COUNT(*) FROM helpers').fetchone()[0]
r = db.execute('SELECT COUNT(*) FROM runs').fetchone()[0]
t = db.execute(\\\"SELECT COUNT(*) FROM triggers\\\").fetchone()[0]
v = db.execute('SELECT COUNT(*) FROM variables').fetchone()[0]
print(f'    {p} Pipelines, {h} Helpers, {r} Runs, {t} Triggers, {v} Variables')
\""
echo ""
read -p "  Sync starten? [y/N] " CONFIRM
if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
  echo "  Abgebrochen."
  exit 1
fi
echo ""

# 1. Backup on Collir before anything
BACKUP_DIR="/root/.brix/backups/pre-sync-$(date +%Y%m%d-%H%M%S)"
echo "→ backup on collir ($BACKUP_DIR)..."
ssh collir "mkdir -p $BACKUP_DIR && cp /root/.brix/brix.db $BACKUP_DIR/brix.db"

# Export all projects as bundles
PROJECTS=$(ssh collir "docker exec brix-mcp python3 -c \"
import sqlite3
db = sqlite3.connect('/root/.brix/brix.db')
projects = [r[0] for r in db.execute(\\\"SELECT DISTINCT project FROM pipelines WHERE project != '' AND project IS NOT NULL\\\").fetchall()]
print(' '.join(projects))
\"")
for PROJECT in $PROJECTS; do
  echo "  backup project: $PROJECT"
  ssh collir "docker exec brix-mcp brix bundle export-project $PROJECT -o /root/.brix/backups/pre-sync-$PROJECT.project.brix.tar.gz 2>/dev/null" || true
done
echo "  DB + $PROJECTS backed up"

# 1. Rsync code (exclude runtime data)
echo "→ rsync..."
rsync -avz --delete \
  --exclude '.git/' \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  --exclude 'brix.db' \
  --exclude '.brix/' \
  --exclude 'backup/' \
  --exclude 'data/' \
  --exclude 'analyze_report.json' \
  /root/docker/brix/ collir:/root/docker/brix/

# 2. Rebuild on Collir
echo "→ rebuild on collir..."
ssh collir "cd /root/docker/brix && docker compose build --quiet && docker compose up -d"

# 3. Verify
echo "→ verify..."
REMOTE_VERSION=$(ssh collir "docker exec brix-mcp brix --version 2>/dev/null" | awk '{print $NF}')
echo "Remote version: $REMOTE_VERSION"

REMOTE_MIGRATION=$(ssh collir "docker exec brix-mcp python3 -c \"
import sqlite3
db = sqlite3.connect('/root/.brix/brix.db')
print(db.execute('SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1').fetchone()[0])
\"")

if [ "$LOCAL_VERSION" = "$REMOTE_VERSION" ]; then
  echo "=== Sync OK: $REMOTE_VERSION_PRE → $REMOTE_VERSION (migration v$REMOTE_MIGRATION) ==="
else
  echo "=== WARNING: Version mismatch! Local=$LOCAL_VERSION Remote=$REMOTE_VERSION ==="
  echo "    Rollback: ssh collir 'cp $BACKUP_DIR/brix.db /root/.brix/brix.db'"
fi
