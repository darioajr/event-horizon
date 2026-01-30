<# 
.SYNOPSIS
    Build script for Windows
.DESCRIPTION
    Builds the Event Horizon project on Windows using vcpkg and CMake
.PARAMETER BuildType
    Release or Debug (default: Release)
.PARAMETER Test
    Run tests after build
.PARAMETER Clean
    Clean build directory before building
.PARAMETER Generator
    CMake generator: vs2022, vs2019, ninja (default: vs2022)
.EXAMPLE
    .\build.ps1 -BuildType Release
    .\build.ps1 -BuildType Debug -Test
    .\build.ps1 -Clean -Generator ninja
#>

param(
    [ValidateSet("Release", "Debug")]
    [string]$BuildType = "Release",
    
    [switch]$Test,
    [switch]$Clean,
    
    [ValidateSet("vs2022", "vs2019", "vs2025", "ninja", "auto")]
    [string]$Generator = "auto"
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BuildDir = Join-Path $ScriptDir "build\windows-$($BuildType.ToLower())"

# =============================================================================
# Detect Visual Studio
# =============================================================================
function Find-VisualStudio {
    $vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    if (Test-Path $vswhere) {
        $vsPath = & $vswhere -latest -property installationPath
        $vsVersion = & $vswhere -latest -property catalog_productLineVersion
        return @{
            Path = $vsPath
            Version = $vsVersion
        }
    }
    return $null
}

$VS = Find-VisualStudio

# Auto-detect generator
if ($Generator -eq "auto") {
    if ($VS) {
        switch ($VS.Version) {
            "2025" { $Generator = "vs2025" }
            "2022" { $Generator = "vs2022" }
            "2019" { $Generator = "vs2019" }
            default { 
                # Try to detect from path
                if ($VS.Path -match "\\18\\") { $Generator = "vs2025" }
                elseif ($VS.Path -match "\\2022\\") { $Generator = "vs2022" }
                elseif ($VS.Path -match "\\2019\\") { $Generator = "vs2019" }
                else { $Generator = "ninja" }
            }
        }
        Write-Host "Auto-detected: $Generator (from $($VS.Path))" -ForegroundColor Cyan
    } else {
        $Generator = "ninja"
        Write-Host "No Visual Studio found, using Ninja" -ForegroundColor Yellow
    }
}

Write-Host "==============================================" -ForegroundColor Cyan
Write-Host " Event Horizon - Windows Build" -ForegroundColor Cyan
Write-Host " Build Type: $BuildType" -ForegroundColor Cyan
Write-Host " Generator:  $Generator" -ForegroundColor Cyan
Write-Host "==============================================" -ForegroundColor Cyan

# =============================================================================
# Find CMake
# =============================================================================
function Find-CMake {
    # Check if cmake is in PATH
    $cmakeInPath = Get-Command cmake -ErrorAction SilentlyContinue
    if ($cmakeInPath) {
        return $cmakeInPath.Source
    }
    
    # Common CMake locations
    $possiblePaths = @(
        "$env:ProgramFiles\CMake\bin\cmake.exe",
        "${env:ProgramFiles(x86)}\CMake\bin\cmake.exe",
        "$env:ProgramFiles\Microsoft Visual Studio\2022\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe",
        "$env:ProgramFiles\Microsoft Visual Studio\2022\Professional\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe",
        "$env:ProgramFiles\Microsoft Visual Studio\2022\Enterprise\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe",
        "$env:ProgramFiles\Microsoft Visual Studio\2019\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe",
        "$env:ProgramFiles\Microsoft Visual Studio\2019\Professional\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe",
        "$env:LOCALAPPDATA\CMake\bin\cmake.exe",
        "C:\CMake\bin\cmake.exe"
    )
    
    foreach ($path in $possiblePaths) {
        if (Test-Path $path) {
            return $path
        }
    }
    
    return $null
}

$CMAKE = Find-CMake
if (-not $CMAKE) {
    Write-Host "Error: CMake not found!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Install CMake using one of these methods:" -ForegroundColor Yellow
    Write-Host "  1. Download from: https://cmake.org/download/" -ForegroundColor White
    Write-Host "  2. Via winget:    winget install Kitware.CMake" -ForegroundColor White
    Write-Host "  3. Via scoop:     scoop install cmake" -ForegroundColor White
    Write-Host "  4. Via choco:     choco install cmake" -ForegroundColor White
    Write-Host ""
    Write-Host "After installing, restart PowerShell or add CMake to PATH" -ForegroundColor Yellow
    exit 1
}

Write-Host "Using CMake: $CMAKE" -ForegroundColor Green

# =============================================================================
# Find vcpkg
# =============================================================================
if (-not $env:VCPKG_ROOT) {
    $possiblePaths = @(
        "C:\vcpkg",
        "C:\fontes\vcpkg",
        "C:\src\vcpkg",
        "$env:USERPROFILE\vcpkg",
        "D:\vcpkg"
    )
    
    foreach ($path in $possiblePaths) {
        if (Test-Path $path) {
            $env:VCPKG_ROOT = $path
            break
        }
    }
    
    if (-not $env:VCPKG_ROOT) {
        Write-Host "Error: VCPKG_ROOT not set and vcpkg not found" -ForegroundColor Red
        Write-Host "Install vcpkg:" -ForegroundColor Yellow
        Write-Host "  git clone https://github.com/microsoft/vcpkg.git C:\vcpkg"
        Write-Host "  C:\vcpkg\bootstrap-vcpkg.bat"
        Write-Host '  $env:VCPKG_ROOT = "C:\vcpkg"'
        exit 1
    }
}

Write-Host "Using vcpkg: $env:VCPKG_ROOT" -ForegroundColor Green

# Select generator
$CMakeGenerator = switch ($Generator) {
    "vs2025" { "Visual Studio 18 2026" }
    "vs2022" { "Visual Studio 17 2022" }
    "vs2019" { "Visual Studio 16 2019" }
    "ninja"  { "Ninja" }
}

$Triplet = "x64-windows"

# Clean if requested
if ($Clean -and (Test-Path $BuildDir)) {
    Write-Host "`nCleaning build directory..." -ForegroundColor Yellow
    Remove-Item -Recurse -Force $BuildDir
}

# Create build directory
if (-not (Test-Path $BuildDir)) {
    New-Item -ItemType Directory -Path $BuildDir | Out-Null
}

Push-Location $BuildDir

try {
    # Configure
    Write-Host "`nConfiguring..." -ForegroundColor Yellow
    
    $cmakeArgs = @(
        $ScriptDir,
        "-G", $CMakeGenerator,
        "-DCMAKE_TOOLCHAIN_FILE=$env:VCPKG_ROOT\scripts\buildsystems\vcpkg.cmake",
        "-DVCPKG_TARGET_TRIPLET=$Triplet",
        "-DBUILD_TESTS=ON"
    )
    
    if ($Generator -eq "ninja") {
        $cmakeArgs += "-DCMAKE_BUILD_TYPE=$BuildType"
    } else {
        $cmakeArgs += "-A", "x64"
    }
    
    & $CMAKE @cmakeArgs
    
    if ($LASTEXITCODE -ne 0) {
        throw "CMake configure failed"
    }
    
    # Build
    Write-Host "`nBuilding..." -ForegroundColor Yellow
    & $CMAKE --build . --config $BuildType --parallel
    
    if ($LASTEXITCODE -ne 0) {
        throw "Build failed"
    }
    
    Write-Host "`n==============================================" -ForegroundColor Green
    Write-Host " Build complete!" -ForegroundColor Green
    Write-Host " Binary: $BuildDir\$BuildType\event_horizon.exe" -ForegroundColor Green
    Write-Host "==============================================" -ForegroundColor Green
    
    # Run tests if requested
    if ($Test) {
        Write-Host "`nRunning tests..." -ForegroundColor Yellow
        & ctest --build-config $BuildType --output-on-failure
        
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Some tests failed!" -ForegroundColor Red
        } else {
            Write-Host "All tests passed!" -ForegroundColor Green
        }
    }
}
finally {
    Pop-Location
}
