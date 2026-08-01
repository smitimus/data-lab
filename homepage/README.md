# Homepage

## Access
| Item | Value                    |
|------|--------------------------|
| URL  | http://YOUR_SERVER_IP:80 |
| Auth | None                     |
| Port | 80                       |

## What It Does
Service dashboard and link aggregator. Displays all running stack services as clickable tiles, reads service metadata via Docker socket, and shows health status. The homepage auto-discovers services via Docker labels — no manual config needed when adding new stacks.

## Key Config Files
- `conf/homepage/` — all config YAML files (seeded from `stacks/homepage/` by `init.sh`)
  - `services.yaml` — service group definitions
  - `bookmarks.yaml` — quick links
  - `settings.yaml` — layout and appearance

## Usage Notes
- Service tiles show the URL, description, and icon from `homepage.*` Docker labels on each service
- Descriptions include default credentials for quick reference
- To add a new service: add `homepage.*` labels to its `compose.yaml` (no homepage restart needed)
- If a tile is missing, check that the service's `compose.yaml` has all required homepage labels
