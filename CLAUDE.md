- Hard Rules
  - Keep changes scoped to this repository.
  - Do not commit changes unless explicitly requested.
  - Keep `go.mod` and `go.sum` in sync with Go dependency changes.
  - Use existing build files and package scripts before adding new tooling.

- Authority & Links
  - Go module: `github.com/slntopp/nocloud-driver-virtual`.
  - `go.mod`.
  - `Dockerfile`.
  - Entrypoint: `main.go`.

- Setup / Test
  - Use `go mod download` to fetch Go module dependencies.
  - Use `go test ./...` for Go package checks.

- Workflow
  - `go mod download`
  - `go test ./...`
  - `go run .`
  - `docker build .`

- Stop Conditions
  - Ask before using or changing secrets, credentials, or `.env` files.
  - Ask before broad regeneration or formatting that changes unrelated files.
  - Ask if required external services, credentials, or generator plugins are missing.
  - Refuse destructive git operations unless explicitly requested.
  - Omit uncertain repository rules instead of guessing.
