[CmdletBinding()]
param(
    [string]$WeKnoraPath = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Stop'

$resolvedWeKnora = try {
    (Resolve-Path -LiteralPath $WeKnoraPath -ErrorAction Stop).Path
}
catch {
    throw "WeKnora 源码路径不存在：$WeKnoraPath；从外挂运行时请使用 -WeKnoraPath <WeKnora源码路径>"
}
if (-not (Test-Path -LiteralPath (Join-Path $resolvedWeKnora 'go.mod') -PathType Leaf)) {
    throw "WeKnora 源码路径缺少 go.mod：$resolvedWeKnora；从外挂运行时请使用 -WeKnoraPath <WeKnora源码路径>"
}

$suffix = "${PID}-$([guid]::NewGuid().ToString('N').Substring(0, 8))"
$network = "weknora-g004-test-$suffix"
$postgresContainer = "weknora-g004-pg-$suffix"

try {
    docker network create $network | Out-Null
    docker run -d --name $postgresContainer --network $network `
        -e POSTGRES_USER=g004 -e POSTGRES_PASSWORD=g004test -e POSTGRES_DB=g004 `
        paradedb/paradedb:v0.22.2-pg17 | Out-Null

    $ready = $false
    $consecutiveReady = 0
    foreach ($attempt in 1..60) {
        docker exec -e PGPASSWORD=g004test $postgresContainer `
            psql -h 127.0.0.1 -U g004 -d g004 -Atqc 'SELECT 1' 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $consecutiveReady++
            if ($consecutiveReady -ge 3) {
                $ready = $true
                break
            }
        }
        else {
            $consecutiveReady = 0
        }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) {
        throw 'ephemeral PostgreSQL did not become ready within 60 seconds'
    }

    docker run --rm --network $network `
        -e WEKNORA_TEST_POSTGRES_EPHEMERAL=1 `
        -e "WEKNORA_TEST_POSTGRES_DSN=postgres://g004:g004test@${postgresContainer}:5432/g004?sslmode=disable" `
        -v "${resolvedWeKnora}:/workspace" -v weknora-go-mod-cache:/go/pkg/mod `
        -v weknora-go-build-cache:/root/.cache/go-build -w /workspace `
        golang:1.26-bookworm go test `
        ./internal/application/repository ./internal/database `
        -run 'Test(KnowledgeSpanRepo_Postgres|QuestionGenerationManifestRepository_Postgres|KnowledgeBaseDeletionRepository_Postgres|WikiCheckpointPostgres|PostgresMigration|PostgresFresh)' -count=1 -v
    if ($LASTEXITCODE -ne 0) {
        throw "G004 PostgreSQL tests failed with exit code $LASTEXITCODE"
    }

    docker run --rm --network $network `
        -e WEKNORA_MIGRATE_ONLY=true -e AUTO_RECOVER_DIRTY=false `
        -e DB_DRIVER=postgres -e DB_USER=g004 -e DB_PASSWORD=g004test -e DB_NAME=g004 `
        -e DB_PORT=5432 -e "DB_HOST=$postgresContainer" -e RETRIEVE_DRIVER=postgres `
        -v "${resolvedWeKnora}:/workspace" -v weknora-go-mod-cache:/go/pkg/mod `
        -v weknora-go-build-cache:/root/.cache/go-build -w /workspace `
        golang:1.26-bookworm go run ./cmd/server
    if ($LASTEXITCODE -ne 0) {
        throw "one-shot migrator failed with exit code $LASTEXITCODE"
    }
}
finally {
    docker rm -f -v $postgresContainer 2>$null | Out-Null
    docker network rm $network 2>$null | Out-Null
}
