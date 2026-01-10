Param()

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = (Resolve-Path (Join-Path $scriptDir '..')).Path
$modelsDir = Join-Path $projectRoot 'models\marker-cache'

if (-not (Test-Path $modelsDir)) {
    New-Item -ItemType Directory -Path $modelsDir | Out-Null
}

$pythonStatement = "from marker.models import create_model_dict; print('Downloading Marker artifacts... this may take a few minutes.'); create_model_dict(); print('Marker artifacts downloaded.')"

Push-Location $projectRoot
try {
    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        & docker-compose run --rm marker python -c "$pythonStatement"
    } else {
        & docker compose run --rm marker python -c "$pythonStatement"
    }
}
finally {
    Pop-Location
}
