#!/bin/bash
set -e

echo "========================================="
echo "Event Horizon - Post Create Setup"
echo "========================================="

# Garantir permissões do diretório
sudo chown -R vscode:vscode /workspaces/eventhorizon 2>/dev/null || true

# Configurar Git
git config --global --add safe.directory /workspaces/eventhorizon
git config --global core.autocrlf input

# Corrigir permissões do SSH (se montado)
if [ -d "/home/vscode/.ssh" ]; then
    echo "Corrigindo permissões SSH..."
    sudo chown -R vscode:vscode /home/vscode/.ssh 2>/dev/null || true
    chmod 700 /home/vscode/.ssh 2>/dev/null || true
    chmod 600 /home/vscode/.ssh/* 2>/dev/null || true
    chmod 644 /home/vscode/.ssh/*.pub 2>/dev/null || true
fi

# Corrigir permissões do vcpkg
echo "Corrigindo permissões do vcpkg..."
sudo chown -R $(whoami) /vcpkg

# Atualizar vcpkg para versão mais recente
echo "Atualizando vcpkg..."
cd /vcpkg && git checkout master && git pull origin master && ./bootstrap-vcpkg.sh
cd /workspaces/eventhorizon

# Instalar/atualizar dependências do vcpkg se necessário
echo "Verificando dependências do vcpkg..."
if [ -f "vcpkg.json" ]; then
    vcpkg install --triplet x64-linux
fi

# Configurar CMake
echo "Configurando CMake..."
cmake --preset linux-debug

echo "========================================="
echo "Setup concluído!"
echo ""
echo "Comandos úteis:"
echo "  cmake --build build/linux-debug --parallel   # Build Debug"
echo "  cmake --build build/linux-release --parallel # Build Release"
echo "  ./build/linux-debug/eventhorizon_tests       # Executar testes"
echo "========================================="
