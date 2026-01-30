#!/bin/bash
# =============================================================================
# Build script for Linux
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_TYPE="${1:-Release}"
BUILD_DIR="$SCRIPT_DIR/build/linux-$(echo $BUILD_TYPE | tr '[:upper:]' '[:lower:]')"

echo "=============================================="
echo " Event Horizon - Linux Build"
echo " Build Type: $BUILD_TYPE"
echo "=============================================="

# Check vcpkg
if [ -z "$VCPKG_ROOT" ]; then
    if [ -d "$HOME/vcpkg" ]; then
        export VCPKG_ROOT="$HOME/vcpkg"
    elif [ -d "/opt/vcpkg" ]; then
        export VCPKG_ROOT="/opt/vcpkg"
    else
        echo "Error: VCPKG_ROOT not set and vcpkg not found in common locations"
        echo "Install vcpkg:"
        echo "  git clone https://github.com/microsoft/vcpkg.git ~/vcpkg"
        echo "  ~/vcpkg/bootstrap-vcpkg.sh"
        echo "  export VCPKG_ROOT=~/vcpkg"
        exit 1
    fi
fi

echo "Using vcpkg: $VCPKG_ROOT"

# Check for ninja or make
if command -v ninja &> /dev/null; then
    GENERATOR="Ninja"
elif command -v make &> /dev/null; then
    GENERATOR="Unix Makefiles"
else
    echo "Error: Neither ninja nor make found. Install one of them."
    exit 1
fi

echo "Using generator: $GENERATOR"

# Create build directory
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Configure
echo ""
echo "Configuring..."
cmake "$SCRIPT_DIR" \
    -G "$GENERATOR" \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
    -DVCPKG_TARGET_TRIPLET="x64-linux" \
    -DBUILD_TESTS=ON

# Build
echo ""
echo "Building..."
cmake --build . --config "$BUILD_TYPE" -j$(nproc)

echo ""
echo "=============================================="
echo " Build complete!"
echo " Binary: $BUILD_DIR/event_horizon"
echo "=============================================="

# Run tests
if [ "$2" == "--test" ]; then
    echo ""
    echo "Running tests..."
    ctest --output-on-failure
fi
