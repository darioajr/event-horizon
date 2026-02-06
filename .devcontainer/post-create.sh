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
    # Copiar SSH para evitar problemas de permissões com bind mount
    cp -r /home/vscode/.ssh /tmp/.ssh-copy
    sudo rm -rf /home/vscode/.ssh
    sudo mv /tmp/.ssh-copy /home/vscode/.ssh
    sudo chown -R vscode:vscode /home/vscode/.ssh
    chmod 700 /home/vscode/.ssh
    find /home/vscode/.ssh -type f -exec chmod 600 {} \;
    find /home/vscode/.ssh -name "*.pub" -exec chmod 644 {} \;
    if [ -f "/home/vscode/.ssh/config" ]; then
        chmod 600 /home/vscode/.ssh/config
    fi
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
