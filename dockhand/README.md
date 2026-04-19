# Dockhand

## Access
| Item     | Value                              |
|----------|------------------------------------|
| URL      | http://YOUR_SERVER_IP:3000         |
| Username | admin                              |
| Password | admin                              |
| Port     | 3000                               |

## What It Does
Docker stack management UI. Provides a web interface to start, stop, and manage all docker-compose stacks without SSH. All stacks in the data-lab directory are bulk-adopted on first setup via `setup.sh`.

## Key Config Files
- `stacks/dockhand/.env` — `ENCRYPTION_KEY` (generated per install; keep stable — changing it loses stored credentials), `HOST_DATA_DIR` (host-side path to conf/dockhand)

## Usage Notes
- **Bulk adopt all stacks:** Run `bash setup.sh` after first start (or via Dockhand UI → Import)
- **ENCRYPTION_KEY** must be base64-encoded 32 bytes (`openssl rand -base64 32`). Hex format silently fails.
- Stack directories are mounted at matching host paths to avoid path translation issues
- If stacks disappear from the UI, re-run `python3 stacks/dockhand/adopt.py`
