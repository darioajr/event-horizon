#!/bin/bash
set -e

echo "========================================="
echo "Event Horizon - Post Create Setup"
echo "========================================="

# Garantir permissões do diretório
sudo chown -R vscode:vscode /workspaces/eventhorizon 2>/dev/null || true

# Copiar .gitconfig do host (montado como readonly) para local editável
GITCONFIG_HOST="/home/vscode/.gitconfig-host"
GITCONFIG_LOCAL="/home/vscode/.gitconfig"

if [ -f "$GITCONFIG_HOST" ]; then
    echo "Copiando .gitconfig do host..."
    cp "$GITCONFIG_HOST" "$GITCONFIG_LOCAL"
    chown vscode:vscode "$GITCONFIG_LOCAL"
    chmod 644 "$GITCONFIG_LOCAL"
fi

# Configurar Git
git config --global --add safe.directory /workspaces/eventhorizon
git config --global core.autocrlf input

# Corrigir permissões do SSH (se montado)
# Bind mounts do Windows não permitem alterar permissões, então copiamos para local não-montado
SSH_LOCAL="/home/vscode/.ssh-local"
SSH_MOUNTED="/home/vscode/.ssh"

if [ -d "$SSH_MOUNTED" ]; then
    echo "Corrigindo permissões SSH..."
    
    # Remover cópia antiga se existir
    rm -rf "$SSH_LOCAL"
    
    # Copiar para local não-montado
    cp -r "$SSH_MOUNTED" "$SSH_LOCAL"
    
    # Corrigir ownership e permissões
    chown -R vscode:vscode "$SSH_LOCAL"
    chmod 700 "$SSH_LOCAL"
    find "$SSH_LOCAL" -type f -exec chmod 600 {} \;
    find "$SSH_LOCAL" -name "*.pub" -exec chmod 644 {} \;
    
    if [ -f "$SSH_LOCAL/config" ]; then
        chmod 600 "$SSH_LOCAL/config"
    fi
    
    # Descobrir qual chave SSH usar
    SSH_KEY=""
    for key in id_ed25519 id_rsa id_ecdsa id_dsa; do
        if [ -f "$SSH_LOCAL/$key" ]; then
            SSH_KEY="$SSH_LOCAL/$key"
            break
        fi
    done
    
    # Construir GIT_SSH_COMMAND baseado no que está disponível
    SSH_OPTS="-o IdentitiesOnly=yes -o UserKnownHostsFile=$SSH_LOCAL/known_hosts -o StrictHostKeyChecking=accept-new"
    
    if [ -f "$SSH_LOCAL/config" ]; then
        SSH_OPTS="-F $SSH_LOCAL/config $SSH_OPTS"
    fi
    
    if [ -n "$SSH_KEY" ]; then
        SSH_OPTS="-i $SSH_KEY $SSH_OPTS"
    fi
    
    GIT_SSH_CMD="ssh $SSH_OPTS"
    
    # Adicionar configuração no .bashrc para usar o SSH local
    if ! grep -q "SSH_LOCAL" /home/vscode/.bashrc 2>/dev/null; then
        cat >> /home/vscode/.bashrc << EOF

# SSH config fix for devcontainer (bind mount permissions)
export GIT_SSH_COMMAND="$GIT_SSH_CMD"
EOF
    fi
    
    # Aplicar para sessão atual também
    export GIT_SSH_COMMAND="$GIT_SSH_CMD"
    
    echo "SSH configurado para usar: $SSH_LOCAL"
else
    echo "Aviso: Diretório SSH não encontrado em $SSH_MOUNTED"
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
