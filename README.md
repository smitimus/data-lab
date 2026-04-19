# Data Lab

A full analytics engineering stack with realistic grocery store mock data, pre-wired and ready to explore. One command on a fresh Debian machine gives you a complete data pipeline.

**Stack:** Verisim Grocery (mock data) → Meltano (EL) → PostgreSQL EDW → dbt (transform) → Superset (BI) + OpenMetadata (catalog) + CloudBeaver (DB IDE) + Airflow (orchestration)

---

## One-Liner Install

```bash
curl -fsSL https://raw.githubusercontent.com/smitimus/data-lab/main/install.sh | sudo bash
```

Requires: Debian or Ubuntu, internet access, ~10 GB disk space. Installs Docker if needed.

---

## What You Get

```
Verisim Grocery          →  Meltano EL            →  PostgreSQL EDW
(mock data generator)       (tap-postgres →            (raw_* schemas)
30-day auto-backfill        target-postgres)
                                    ↓
                             dbt (Airflow)
                        (staging → mart models)
                                    ↓
                     Superset (dashboards) + OpenMetadata (catalog)
```

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| Homepage | http://SERVER_IP:80 | no auth | Service dashboard |
| Airflow | http://SERVER_IP:8080 | admin / admin | Pipeline orchestration |
| Superset | http://SERVER_IP:8088 | admin / admin | BI dashboards |
| CloudBeaver | http://SERVER_IP:8978 | set on first login | Database IDE |
| OpenMetadata | http://SERVER_IP:8585 | admin / admin | Data catalog |
| Dockhand | http://SERVER_IP:3000 | admin / admin | Docker management |
| Verisim UI | http://SERVER_IP:8501 | no auth | Generator control |
| Verisim API | http://SERVER_IP:8010/docs | no auth | REST API |
| PostgreSQL EDW | SERVER_IP:5432 | postgres / postgres | Analytics database |
| Verisim DB | SERVER_IP:5499 | verisim / verisim | Source database |

---

## Manual Setup (without curl-pipe-bash)

<details>
<summary>Expand for step-by-step</summary>

### 1. Install Docker on Debian

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/debian $(lsb_release -cs) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER
```

Log out and back in for the group change to take effect.

### 2. Set OpenMetadata kernel parameter

```bash
sudo sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf
```

### 3. Clone and configure

```bash
git clone https://github.com/smitimus/data-lab.git /opt/data-lab
cd /opt/data-lab

# Edit global.env — set YOUR_SERVER_IP and YOUR_INSTALL_DIR
nano global.env

# Generate .env files from templates
for d in airflow postgres meltano superset cloudbeaver homepage openmetadata dockhand verisim-grocery; do
  cp $d/.env.example $d/.env
done

# Generate secrets and replace placeholders (or edit manually)
FERNET=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
SECRET=$(openssl rand -base64 42)
sed -i "s|GENERATE_ME_FERNET_KEY|${FERNET}|g" airflow/.env openmetadata/.env
sed -i "s|GENERATE_ME_SECRET|${SECRET}|g" airflow/.env openmetadata/.env superset/.env
sed -i "s|GENERATE_ME_ENCRYPTION_KEY|$(openssl rand -base64 32)|g" dockhand/.env
DGID=$(getent group docker | cut -d: -f3)
sed -i "s|DETECT_ME_DOCKER_GID|${DGID}|g" airflow/.env openmetadata/.env

python3 global-env-sync.py
```

### 4. Start

```bash
bash init.sh
bash start.sh
```

### 5. Import Superset dashboards (manual)

1. Open Superset → Dashboards → ⋮ → Import
2. Select files from `superset/dashboards/`

</details>

---

## First-Time Setup After Install

1. **Trigger the Airflow DAG:** Airflow → DAGs → `grocery_pipeline` → ▶ Trigger
   - Runs Meltano EL + dbt staging + dbt marts + dbt tests (~2–5 min)
2. **Check Superset dashboards** — data appears after the first successful DAG run
3. **Adopt stacks in Dockhand:** `bash setup.sh` (or follow prompts)
4. **OpenMetadata dbt lineage** — auto-ingested after the first DAG run generates `manifest.json`

---

## Stack Reference

Each service has its own README with access details, config files, and usage notes:

| Service | README |
|---------|--------|
| Airflow | [airflow/README.md](airflow/README.md) |
| PostgreSQL | [postgres/README.md](postgres/README.md) |
| Meltano | [meltano/README.md](meltano/README.md) |
| Superset | [superset/README.md](superset/README.md) |
| CloudBeaver | [cloudbeaver/README.md](cloudbeaver/README.md) |
| Homepage | [homepage/README.md](homepage/README.md) |
| OpenMetadata | [openmetadata/README.md](openmetadata/README.md) |
| Dockhand | [dockhand/README.md](dockhand/README.md) |
| Verisim Grocery | [verisim-grocery/README.md](verisim-grocery/README.md) |

---

## Adding Your Own Data Sources

**Add a Meltano tap:** Edit `conf/meltano/meltano.yml`, add the tap config, restart the meltano container.

**Add a dbt project:** Create a new project in `airflow/dbt/`, add a profiles entry in `airflow/dbt/profiles.yml`, add a DAG in `airflow/dags/`.

**Add a Superset data source:** Superset → Settings → Database Connections → + Database.

---

## Management

```bash
bash start.sh              # start all stacks
bash stop.sh               # stop all stacks
bash init.sh               # reseed conf/ (prompts to wipe)
bash setup.sh              # adopt stacks in Dockhand (first-time)
python3 global-env-sync.py # sync global.env changes to all .env files
```

Reset a single stack:
```bash
cd stacks/<service> && docker compose down
rm -rf conf/<service>
# re-run init.sh or manually reseed, then:
cd stacks/<service> && docker compose up -d
```

---

## License

MIT — see [LICENSE](LICENSE)
