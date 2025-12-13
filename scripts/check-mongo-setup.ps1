# PowerShell Script to Verify MongoDB Setup
# Run this from project root: .\scripts\check-mongo-setup.ps1

Write-Host "=== MongoDB Setup Verification ===" -ForegroundColor Green

# Check if container is running
Write-Host "`n1. Checking MongoDB container status..." -ForegroundColor Yellow
$MONGO_CONTAINER = docker ps --filter "name=mongo" --format "{{.Names}}" | Select-Object -First 1

if (-not $MONGO_CONTAINER) {
    Write-Host "   [FAIL] MongoDB container is not running!" -ForegroundColor Red
    Write-Host "   → Start it with: docker compose up -d mongo" -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "   [OK] Container running: $MONGO_CONTAINER" -ForegroundColor Green
}

# Check volume configuration
Write-Host "`n2. Checking data volume..." -ForegroundColor Yellow
$volumes = docker volume ls --format "{{.Name}}" | Select-String "mongo"
if ($volumes) {
    Write-Host "   [OK] Volume exists: $volumes" -ForegroundColor Green

    # Show volume details
    $volumeInfo = docker volume inspect $volumes | ConvertFrom-Json
    $mountPoint = $volumeInfo[0].Mountpoint
    Write-Host "   Mount point: $mountPoint" -ForegroundColor Gray
} else {
    Write-Host "   [WARN] No MongoDB volume found - data will be lost on container removal!" -ForegroundColor Yellow
    Write-Host "   → Check docker-compose.yml has: mongo_data:/data/db" -ForegroundColor Yellow
}

# Check database and collections
Write-Host "`n3. Checking database contents..." -ForegroundColor Yellow

$DATABASE = "yelpAcademicDatasets"
$collectionsCheck = docker exec $MONGO_CONTAINER mongosh --quiet --eval "db.getSiblingDB('$DATABASE').getCollectionNames()" 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host "   [OK] Database '$DATABASE' accessible" -ForegroundColor Green

    # Count documents in each collection
    $expectedCollections = @("business", "review", "tip", "checkin", "user")
    $foundCollections = 0

    foreach ($coll in $expectedCollections) {
        $count = docker exec $MONGO_CONTAINER mongosh --quiet --eval "db.getSiblingDB('$DATABASE').$coll.countDocuments()" 2>&1

        if ($LASTEXITCODE -eq 0 -and $count -match '^\d+$' -and [int]$count -gt 0) {
            Write-Host "   [OK] $coll : $count documents" -ForegroundColor Green
            $foundCollections++
        } else {
            Write-Host "   [MISS] $coll : not found or empty" -ForegroundColor Yellow
        }
    }

    if ($foundCollections -eq 0) {
        Write-Host "`n   [WARN] No data found! Run import script:" -ForegroundColor Yellow
        Write-Host "   → .\scripts\import-mongo-data.ps1" -ForegroundColor Yellow
    }

} else {
    Write-Host "   [FAIL] Cannot connect to database" -ForegroundColor Red
    Write-Host "   Error: $collectionsCheck" -ForegroundColor Red
}

# Check network connectivity (for Airflow)
Write-Host "`n4. Checking network connectivity..." -ForegroundColor Yellow
$airflowContainer = docker ps --filter "name=airflow-scheduler" --format "{{.Names}}" | Select-Object -First 1

if ($airflowContainer) {
    Write-Host "   Airflow scheduler found: $airflowContainer" -ForegroundColor Gray

    # Test ping from Airflow to MongoDB
    $pingTest = docker exec $airflowContainer ping -c 1 mongo 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   [OK] Airflow can reach MongoDB container" -ForegroundColor Green
    } else {
        Write-Host "   [WARN] Cannot ping mongo from Airflow" -ForegroundColor Yellow
        Write-Host "   → Check they are on same Docker network" -ForegroundColor Yellow
    }
} else {
    Write-Host "   [INFO] Airflow not running (optional check)" -ForegroundColor Gray
}

# Check configuration file
Write-Host "`n5. Checking configuration files..." -ForegroundColor Yellow

$configFiles = @("src/main/resources/dev.conf", "src/main/resources/local.conf")
foreach ($configFile in $configFiles) {
    if (Test-Path $configFile) {
        $content = Get-Content $configFile -Raw

        if ($content -match 'mongodb\s*\{') {
            Write-Host "   [OK] $configFile has mongodb config" -ForegroundColor Green

            # Extract enabled flag
            if ($content -match 'enabled\s*=\s*(true|false)') {
                $enabled = $matches[1]
                Write-Host "      → enabled = $enabled" -ForegroundColor Gray
            }

            # Extract URI
            if ($content -match 'uri\s*=\s*"([^"]+)"') {
                $uri = $matches[1]
                Write-Host "      → uri = $uri" -ForegroundColor Gray
            }
        } else {
            Write-Host "   [WARN] $configFile missing mongodb config" -ForegroundColor Yellow
        }
    }
}

# Summary
Write-Host "`n=== Summary ===" -ForegroundColor Green
Write-Host "MongoDB container: " -NoNewline
if ($MONGO_CONTAINER) { Write-Host "✓ Running" -ForegroundColor Green } else { Write-Host "✗ Not running" -ForegroundColor Red }

Write-Host "Data volume: " -NoNewline
if ($volumes) { Write-Host "✓ Configured" -ForegroundColor Green } else { Write-Host "⚠ Not configured" -ForegroundColor Yellow }

Write-Host "Collections imported: " -NoNewline
if ($foundCollections -ge 3) {
    Write-Host "✓ $foundCollections/5 found" -ForegroundColor Green
} elseif ($foundCollections -gt 0) {
    Write-Host "⚠ $foundCollections/5 found" -ForegroundColor Yellow
} else {
    Write-Host "✗ None found" -ForegroundColor Red
}

Write-Host "`nNext steps:" -ForegroundColor Cyan
if (-not $MONGO_CONTAINER) {
    Write-Host "  1. Start MongoDB: docker compose up -d mongo" -ForegroundColor White
}
if ($foundCollections -eq 0) {
    Write-Host "  2. Import data: .\scripts\import-mongo-data.ps1" -ForegroundColor White
}
Write-Host "  3. Run pipeline: bin\run-local.cmd --process bronze_ingest --env local --tables business --run_date 2020-01-31" -ForegroundColor White
Write-Host ""

