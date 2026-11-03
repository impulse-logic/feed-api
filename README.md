# Feed API

A REST API built with [Gin](https://github.com/gin-gonic/gin), [GORM](https://gorm.io) and PostgreSQL for managing vendor feed data.

## Contents
- [Run locally](#run-locally)
- [Build a binary](#build-a-binary)
- [Host live (Ubuntu VPS)](#host-live-ubuntu-vps)
- [Production checklist](#production-checklist)
- [Lifecycle guide: startup → request → DB insert → go-live](docs/LIFECYCLE.md)

## Run locally

### 1. Install the tools

| Tool       | Windows                                                        | macOS                         | Ubuntu/Debian                       |
|------------|----------------------------------------------------------------|-------------------------------|-------------------------------------|
| Go 1.27+   | Installer from https://go.dev/dl/                              | `brew install go`             | Tarball from https://go.dev/dl/     |
| PostgreSQL | Installer from https://www.postgresql.org/download/windows/    | `brew install postgresql@17`  | `sudo apt install -y postgresql`    |
| Git        | https://git-scm.com/download/win                               | `xcode-select --install`      | `sudo apt install -y git`           |

Check the installs:
```sh
go version
psql --version
```

### 2. Create a database

Skip this step if you already have a database (for example one shared with another app).

```sh
# Windows: open "SQL Shell (psql)" from the Start menu, or: psql -U postgres
# macOS/Linux: sudo -u postgres psql   (macOS with brew: psql postgres)
CREATE USER feedapi WITH PASSWORD 'feedapi_local_pw';
CREATE DATABASE feedapi OWNER feedapi;
\q
```

### 3. Get the code and configure it

```sh
git clone <repository-url>
cd feed-api
go mod download
```

Copy the example env file:
```sh
copy .env.example .env      # Windows (cmd / PowerShell)
cp .env.example .env        # macOS / Linux
```

Edit `.env`:

| Variable          | Description                                         | Default     |
|-------------------|-----------------------------------------------------|-------------|
| `DB_HOST`         | PostgreSQL host                                     | `localhost` |
| `DB_PORT`         | PostgreSQL port                                     | `5432`      |
| `DB_USER`         | PostgreSQL username                                 | `postgres`  |
| `DB_PASSWORD`     | PostgreSQL password                                 | *(none)*    |
| `DB_NAME`         | PostgreSQL database name                            | `postgres`  |
| `DB_SSLMODE`      | `disable`, `require`, `verify-full`                 | `disable`   |
| `DB_AUTO_MIGRATE` | `true` = create/update tables on startup            | `false`     |
| `SERVER_PORT`     | Port the API listens on                             | `8080`      |
| `GIN_MODE`        | `debug` or `release`                                | `debug`     |
| `SERVICE_BUS_CONNECTION_STRING` | Azure Service Bus connection string (Listen key) | *(none = queue routes off)* |
| `SERVICE_BUS_NAMESPACE` | Used only when the connection string is empty (Azure AD login) | *(none)* |
| `SERVICE_BUS_ENTITY` | Queue names the API may read, comma-separated | `vendor,product-hierarchies,products,product-upcs,boh,deliveries,stocktake,product-variants,pos` |
| `QUEUE_ADMIN_TOKEN` | Secret for the `/queue` endpoints (`X-Admin-Token` header) | *(none = routes off)* |
| `QUEUE_WORKERS` | Parallel queue readers for `/queue/{name}/process` (1-32) | `8` |
| `QUEUE_BATCH_SIZE` | Messages per batch / DB transaction (1-1000) | `100` |

Azure Service Bus setup and the `/queue` endpoints are explained in [docs/SERVICE_BUS.md](docs/SERVICE_BUS.md).

### 4. Tables

- **Fresh database you created in step 2:** set `DB_AUTO_MIGRATE=true`. GORM creates the `vendors` table on startup.
- **Existing/shared database** (the table is managed by another application): keep `DB_AUTO_MIGRATE=false`. Otherwise GORM will try to change constraints it does not own.

For reference, the minimum `vendors` table is:
```sql
CREATE TABLE vendors (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL UNIQUE,
    external_id VARCHAR(255) NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 5. Start the server

```sh
go run ./cmd
```

You should see:
```
Successfully connected to PostgreSQL!
Server starting on port 8080...
```

### 6. Test it

bash / macOS / Linux / Git Bash:
```sh
curl -X POST http://localhost:8080/vendors \
  -H "Content-Type: application/json" \
  -d '{"name":"Acme Corp","external_id":"acme-001"}'
```

Windows PowerShell:
```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/vendors `
  -ContentType "application/json" `
  -Body '{"name":"Acme Corp","external_id":"acme-001"}'
```

### Common problems

| Message                                         | Fix                                                                                |
|-------------------------------------------------|------------------------------------------------------------------------------------|
| `connection refused`                            | PostgreSQL isn't running, or `DB_HOST`/`DB_PORT` is wrong. Start the Postgres service. |
| `password authentication failed`                | Wrong `DB_USER`/`DB_PASSWORD` in `.env`.                                           |
| `database "..." does not exist`                 | Create it (step 2) or fix `DB_NAME`.                                               |
| `relation "vendors" does not exist`             | Set `DB_AUTO_MIGRATE=true` once, or create the table with the SQL above.           |
| `bind: address already in use` / `Only one usage of each socket address` | Another program uses the port. Change `SERVER_PORT`. |
| `409 ... already exists`                        | `name` and `external_id` must be unique. Use different values.                     |
| `Warning: .env file not found`                  | Run the command from the project root, where `.env` lives.                          |

---

## Build a binary

For your current OS:
```sh
go build -o bin/feed-api ./cmd          # on Windows use: -o bin/feed-api.exe
```

For a Linux server, from **Windows PowerShell**:
```powershell
$env:GOOS="linux"; $env:GOARCH="amd64"; $env:CGO_ENABLED="0"
go build -trimpath -ldflags="-s -w" -o bin/feed-api ./cmd
Remove-Item Env:GOOS, Env:GOARCH, Env:CGO_ENABLED
```

For a Linux server, from **macOS / Linux / Git Bash**:
```sh
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/feed-api ./cmd
```

The result is a single file with no dependencies. That one file is all the server needs, plus a `.env`.

---

## Host live (Ubuntu VPS)

This deploys the API on an Ubuntu 22.04/24.04 server behind Nginx with free HTTPS. In the commands, replace:
- `SERVER_IP` with your server's public IP
- `api.example.com` with your domain
- `deploy` with your server username

### 1. Get a server and a domain
- Rent a VPS. Any provider works (DigitalOcean, Hetzner, Vultr, AWS Lightsail, Linode); 1 vCPU and 1 GB RAM is enough.
- At your domain's DNS provider, add an **A record**: `api.example.com → SERVER_IP`.

### 2. First login and basic setup
```sh
ssh root@SERVER_IP

apt update && apt upgrade -y
adduser deploy
usermod -aG sudo deploy
exit

ssh deploy@SERVER_IP
```

### 3. Install PostgreSQL and create the database
Skip this step if you use a managed database (see [Using a managed PostgreSQL](#using-a-managed-postgresql)).

```sh
sudo apt install -y postgresql
sudo systemctl enable --now postgresql

sudo -u postgres psql <<'SQL'
CREATE USER feedapi WITH PASSWORD 'CHANGE-ME-long-random-password';
CREATE DATABASE feedapi OWNER feedapi;
SQL
```
PostgreSQL only listens on localhost by default, which is what we want.

### 4. Upload the binary
On **your computer**, [build for Linux](#build-a-binary), then:
```sh
scp bin/feed-api deploy@SERVER_IP:/tmp/feed-api
```

Alternatively, build on the server: install Go from https://go.dev/dl/, then `git clone`, then `go build -o /tmp/feed-api ./cmd`.

### 5. Install the app
On the **server**:
```sh
sudo useradd --system --no-create-home --shell /usr/sbin/nologin feedapi
sudo mkdir -p /opt/feed-api
sudo mv /tmp/feed-api /opt/feed-api/feed-api
sudo chmod +x /opt/feed-api/feed-api
sudo nano /opt/feed-api/.env
```

Contents of `/opt/feed-api/.env`:
```ini
DB_HOST=localhost
DB_PORT=5432
DB_USER=feedapi
DB_PASSWORD=CHANGE-ME-long-random-password
DB_NAME=feedapi
DB_SSLMODE=disable
DB_AUTO_MIGRATE=true
SERVER_PORT=8080
GIN_MODE=release
SERVICE_BUS_CONNECTION_STRING="Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<listen-policy>;SharedAccessKey=<key>"
SERVICE_BUS_ENTITY=vendor,product-hierarchies,products,product-upcs,boh,deliveries,stocktake,product-variants,pos
QUEUE_ADMIN_TOKEN=CHANGE-ME-long-random-token
```

Lock it down:
```sh
sudo chown -R feedapi:feedapi /opt/feed-api
sudo chmod 600 /opt/feed-api/.env
```

Quick manual test (stop it with Ctrl+C):
```sh
cd /opt/feed-api && sudo -u feedapi ./feed-api
```

### 6. Run it as a service (systemd)
This keeps the API running in the background, restarts it if it crashes, and starts it at boot.

```sh
sudo nano /etc/systemd/system/feed-api.service
```
```ini
[Unit]
Description=Feed API
After=network.target postgresql.service

[Service]
Type=simple
User=feedapi
Group=feedapi
WorkingDirectory=/opt/feed-api
ExecStart=/opt/feed-api/feed-api
Restart=always
RestartSec=5
NoNewPrivileges=true
ProtectSystem=full
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```
The app reads `.env` from its working directory, so `WorkingDirectory` must be `/opt/feed-api`.

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now feed-api
sudo systemctl status feed-api        # should say "active (running)"
curl -i -X POST http://127.0.0.1:8080/vendors -H "Content-Type: application/json" -d '{}'   # expect 400
```

### 7. Nginx reverse proxy
Nginx listens on ports 80/443 and forwards requests to the API on `127.0.0.1:8080`.

```sh
sudo apt install -y nginx
sudo nano /etc/nginx/sites-available/feed-api
```
```nginx
server {
    listen 80;
    server_name api.example.com;

    client_max_body_size 10m;

    location / {
        proxy_pass         http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
        proxy_read_timeout 60s;
    }
}
```
```sh
sudo ln -s /etc/nginx/sites-available/feed-api /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx
```

### 8. HTTPS (free, Let's Encrypt)
DNS from step 1 must already point to the server.
```sh
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d api.example.com
sudo certbot renew --dry-run      # check that auto-renewal works
```

### 9. Firewall
```sh
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw enable
sudo ufw status
```
Port 8080 and PostgreSQL (5432) stay closed to the internet, so everything goes through Nginx.

### 10. Test the live API
```sh
curl -X POST https://api.example.com/vendors \
  -H "Content-Type: application/json" \
  -d '{"name":"Acme Corp","external_id":"acme-001"}'
```

### 11. Deploy a new version
On your computer:
```sh
git pull                                   # or your local changes
# build for Linux (see "Build a binary")
scp bin/feed-api deploy@SERVER_IP:/tmp/feed-api
```
On the server:
```sh
sudo mv /tmp/feed-api /opt/feed-api/feed-api
sudo chown feedapi:feedapi /opt/feed-api/feed-api
sudo chmod +x /opt/feed-api/feed-api
sudo systemctl restart feed-api
sudo systemctl status feed-api
```

### 12. Day-to-day commands
```sh
journalctl -u feed-api -f              # live logs
journalctl -u feed-api --since today   # today's logs
sudo systemctl restart feed-api        # restart
sudo systemctl stop feed-api           # stop
sudo nano /opt/feed-api/.env           # change config, then restart
sudo tail -f /var/log/nginx/access.log /var/log/nginx/error.log
```

### Using a managed PostgreSQL
With a hosted database (AWS RDS, DigitalOcean Managed DB, Supabase, Neon, etc.), skip step 3 and set these in `/opt/feed-api/.env`:
```ini
DB_HOST=your-db-host.provider.com
DB_PORT=5432            # or the port your provider gives
DB_USER=...
DB_PASSWORD=...
DB_NAME=...
DB_SSLMODE=require
```
In the provider's dashboard, allow connections from your server's IP.

---

## Production checklist

- [ ] `GIN_MODE=release`
- [ ] Strong, unique `DB_PASSWORD`; `.env` has `chmod 600` and is never committed to git
- [ ] `DB_SSLMODE=require` (or `verify-full`) for any remote/managed database
- [ ] `DB_AUTO_MIGRATE=false` if another application owns the schema
- [ ] API reachable only through Nginx with HTTPS; port 8080 closed in `ufw`
- [ ] PostgreSQL not exposed to the internet
- [ ] Daily backups, for example with a cron job (`sudo crontab -e`):
  ```sh
  0 3 * * * sudo -u postgres pg_dump feedapi | gzip > /var/backups/feedapi-$(date +\%F).sql.gz
  ```
  Restore: `gunzip -c backup.sql.gz | sudo -u postgres psql feedapi`
- [ ] Regular OS updates: `sudo apt update && sudo apt upgrade -y`
