# feed-api: AI assistant guide

REST API for vendor feed data. Go 1.27 · Gin · GORM (`gorm.io/gorm` + `gorm.io/driver/postgres`/pgx) · PostgreSQL.

## Commands
```sh
go run ./cmd                 # run locally (reads .env)
go build ./... && go vet ./...
gofmt -w internal cmd
go mod tidy
```

## Architecture (request flow)
```
routes/<x>_routes.go  ->  handler/<x>_handler.go  ->  repository/<x>_repository.go  ->  model/<x>.go
                             uses dto/<x>_dto.go
```
- `cmd/main.go` is wiring only: config, DB, optional migrate, `routes.SetupRouter(db)`, run. Do not register routes here.
- `internal/routes/routes.go` (`SetupRouter`) builds repositories and handlers and calls each `Register<X>Routes(r.Group("/x"), handler)`.
- `internal/routes/<x>_routes.go` holds only the route table for one resource.
- `internal/handler/` is the HTTP layer: bind/validate the DTO, call the repository, map errors to status codes. **Never use `*gorm.DB` in handlers.**
- `internal/repository/` holds **all** DB queries. Always use `r.db.WithContext(ctx)`, and pass `c.Request.Context()` from the handler.
- `internal/dto/` holds request/response structs. `binding:"..."` tags go here, **not** on models.
- `internal/model/` holds GORM entities (`gorm:"..."` and `json:"..."` tags).

## Adding a new resource (e.g. `feeds`)
1. `internal/model/feed.go`: GORM struct.
2. `internal/dto/feed_dto.go`: `CreateFeedRequest` etc. with `binding` tags.
3. `internal/repository/feed_repository.go`: `FeedRepository{db *gorm.DB}`, `NewFeedRepository`, and query methods taking `ctx`.
4. `internal/handler/feed_handler.go`: `FeedHandler{repo}`, `NewFeedHandler`, and handler methods.
5. `internal/routes/feed_routes.go`: `RegisterFeedRoutes(rg *gin.RouterGroup, h *handler.FeedHandler)`.
6. `internal/routes/routes.go`: build the repo and handler, then call `RegisterFeedRoutes(r.Group("/feeds"), feedHandler)`.
7. If this app owns the table, add the model to `database.Migrate` in `internal/database/db.go`.
8. Add the requests (with test scripts) as a new folder in `postman/feed-api.json`. `README.md` holds only run/host steps; don't document endpoints there.

## Conventions
- Errors: the DB is opened with `TranslateError: true`, so use `errors.Is(err, gorm.ErrDuplicatedKey)` → 409 and `gorm.ErrRecordNotFound` → 404. Never string-match driver errors.
- Never return `err.Error()` from the DB to clients. Use `log.Printf` and send a generic 500 message.
- Response shape: success is `{"message": "...", "data": ...}` and failure is `{"error": "..."}`.
- Keep comment density low and match the existing numbered-step comments in handlers.

## Database (important)
- The local/dev Postgres `vendors` table is **owned by another application** (Laravel-style constraint names). It has an FK `picklist_group_identifier_id → picklist_group_identifiers`, and **both `name` and `external_id` are unique**.
- `DB_AUTO_MIGRATE` defaults to `false`. Do **not** enable it against that shared database: GORM tries to rewrite its constraints and fails. Only enable it on a fresh database this app owns.
- Never run destructive SQL against the shared DB. Clean up any test rows you insert.

## Hosting
No Docker. Deployment is the Go binary on a Linux VPS with systemd + Nginx + Certbot. The full steps are in `README.md`; keep it in sync when config or run steps change.

## Config (env vars, see `.env.example`)
`DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, DB_SSLMODE, DB_AUTO_MIGRATE, SERVER_PORT, GIN_MODE`,
plus Azure Service Bus: `SERVICE_BUS_CONNECTION_STRING, SERVICE_BUS_NAMESPACE, SERVICE_BUS_ENTITY, QUEUE_ADMIN_TOKEN, QUEUE_WORKERS, QUEUE_BATCH_SIZE` (see `docs/SERVICE_BUS.md`). `.env` is git-ignored and must never be committed or read into output.
