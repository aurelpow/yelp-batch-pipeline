# PowerShell Script to Import Yelp Data into MongoDB Docker Container
# Run this from project root: .\scripts\import-mongo-data.ps1

Write-Host "=== Importing Yelp Data into MongoDB Container ===" -ForegroundColor Green

# Configuration
$DATABASE = "yelpAcademicDatasets"
$DATA_DIR = "data/raw"

# Check if container is running and get actual name
Write-Host "`nChecking MongoDB container..." -ForegroundColor Yellow
$MONGO_CONTAINER = docker ps --filter "name=mongo" --format "{{.Names}}" | Select-Object -First 1

if (-not $MONGO_CONTAINER) {
    Write-Host "ERROR: MongoDB container is not running!" -ForegroundColor Red
    Write-Host "Start it with: docker compose up -d mongo" -ForegroundColor Yellow
    Write-Host "Or check running containers: docker ps" -ForegroundColor Yellow
    exit 1
}
Write-Host "[OK] Container is running: $MONGO_CONTAINER" -ForegroundColor Green

# Collections to import
$collections = @(
    @{name="business"; file="yelp_academic_dataset_business.json"},
    @{name="review"; file="yelp_academic_dataset_review.json"},
    @{name="tip"; file="yelp_academic_dataset_tip.json"},
    @{name="checkin"; file="yelp_academic_dataset_checkin.json"},
    @{name="user"; file="yelp_academic_dataset_user.json"}
)

# Import each collection
foreach ($coll in $collections) {
    $collName = $coll.name
    $fileName = $coll.file
    $filePath = "$DATA_DIR/$fileName"

    Write-Host "`n--- Importing $collName ---" -ForegroundColor Cyan

    # Check if file exists
    if (-not (Test-Path $filePath)) {
        Write-Host "  [SKIP] File not found - $filePath" -ForegroundColor Yellow
        continue
    }

    # Get file size
    $fileSize = (Get-Item $filePath).Length / 1MB
    $fileSizeRounded = [math]::Round($fileSize, 2)
    Write-Host "  File: $fileName ($fileSizeRounded MB)"

    # Copy file to container
    Write-Host "  > Copying file to container..."
    docker cp $filePath "$MONGO_CONTAINER`:/tmp/$fileName"

    # Import using mongoimport (newline-delimited JSON, not array)
    Write-Host "  > Importing to MongoDB (this may take a while)..."
    $importOutput = docker exec $MONGO_CONTAINER mongoimport `
        --db $DATABASE `
        --collection $collName `
        --file "/tmp/$fileName" `
        2>&1

    if ($LASTEXITCODE -eq 0) {
        # Get document count
        $count = docker exec $MONGO_CONTAINER mongosh --quiet --eval "db.getSiblingDB('$DATABASE').$collName.countDocuments()"
        Write-Host "  [OK] Imported successfully: $count documents" -ForegroundColor Green
    } else {
        Write-Host "  [ERROR] Import failed!" -ForegroundColor Red
        Write-Host "  Error: $importOutput" -ForegroundColor Red
    }

    # Clean up temp file in container
    docker exec $MONGO_CONTAINER rm "/tmp/$fileName"
}

# Verify import
Write-Host "`n=== Verification ===" -ForegroundColor Green
docker exec $MONGO_CONTAINER mongosh --quiet --eval "
    db = db.getSiblingDB('$DATABASE');
    print('Database: $DATABASE');
    print('Collections:');
    db.getCollectionNames().forEach(function(coll) {
        print('  - ' + coll + ': ' + db[coll].countDocuments() + ' documents');
    });
"

Write-Host "`n[SUCCESS] Import complete!" -ForegroundColor Green
Write-Host "You can now run your Airflow DAG to process this data." -ForegroundColor Yellow