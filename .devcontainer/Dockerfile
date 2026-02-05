# Event Horizon - DevContainer para desenvolvimento Linux
# Using Ubuntu base image as fallback when MCR is unreachable
FROM ubuntu:22.04

# Argumentos para configuração
ARG VCPKG_VERSION=2025.01.13
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Create the user
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# Instalar dependências do sistema
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        # Build tools
        build-essential \
        ninja-build \
        cmake \
        pkg-config \
        # Development tools
        gdb \
        valgrind \
        clang-format \
        clang-tidy \
        cppcheck \
        # Bibliotecas de desenvolvimento
        libssl-dev \
        libcurl4-openssl-dev \
        # Utilitários
        curl \
        wget \
        git \
        zip \
        unzip \
        tar \
        ca-certificates \
        locales \
        # Para documentação
        doxygen \
        graphviz \
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/* \
    && locale-gen en_US.UTF-8

ENV LANG=en_US.UTF-8

# Instalar vcpkg
RUN git clone https://github.com/microsoft/vcpkg.git /vcpkg \
    && cd /vcpkg \
    && git checkout ${VCPKG_VERSION} \
    && ./bootstrap-vcpkg.sh \
    && ln -s /vcpkg/vcpkg /usr/local/bin/vcpkg

# Configurar variáveis de ambiente do vcpkg
ENV VCPKG_ROOT=/vcpkg
ENV PATH="${VCPKG_ROOT}:${PATH}"

# Configurar diretório de trabalho
WORKDIR /workspaces/eventhorizon

# Skip pre-installing vcpkg dependencies at build time - let CMake handle it at runtime
# The dependencies specified in vcpkg.json will be installed when building the project

# Configurar usuário vscode
USER vscode

# Configurar git
RUN git config --global --add safe.directory /workspaces/eventhorizon
