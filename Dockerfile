# =============================================================================
# Event Horizon - Kafka-Compatible Event Streaming Platform
# Multi-stage build for Linux (x64 and ARM64)
# Base image: Same as Confluent Kafka (Ubuntu)
# =============================================================================

# Build arguments
ARG VERSION=1.0.0
ARG BUILD_NUMBER=0
ARG BUILD_DATE
ARG VCS_REF
ARG VCS_URL=https://github.com/darioajr/eventhorizon

# -----------------------------------------------------------------------------
# Stage 1: Build
# -----------------------------------------------------------------------------
FROM ubuntu:22.04 AS builder

# Re-declare ARGs after FROM
ARG VERSION
ARG BUILD_NUMBER

# Evitar prompts interativos durante instalação
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependências de build
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    curl \
    zip \
    unzip \
    tar \
    pkg-config \
    linux-libc-dev \
    # Dependências para vcpkg e bibliotecas
    autoconf \
    automake \
    libtool \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Instalar vcpkg
ENV VCPKG_ROOT=/opt/vcpkg
RUN git clone https://github.com/microsoft/vcpkg.git ${VCPKG_ROOT} \
    && ${VCPKG_ROOT}/bootstrap-vcpkg.sh -disableMetrics

# Detectar arquitetura e definir triplet
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then \
        echo "arm64-linux" > /tmp/triplet; \
    else \
        echo "x64-linux" > /tmp/triplet; \
    fi

# Copiar arquivos do projeto
WORKDIR /src
COPY vcpkg.json CMakeLists.txt CMakePresets.json version.txt ./
COPY src/ ./src/

# Instalar dependências via vcpkg e compilar
RUN TRIPLET=$(cat /tmp/triplet) && \
    cmake -B build \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
        -DVCPKG_TARGET_TRIPLET=${TRIPLET} \
        -DBUILD_TESTS=OFF \
        -DVERSION_SUFFIX="" \
        -DBUILD_NUMBER=${BUILD_NUMBER} \
    && cmake --build build --config Release --parallel $(nproc)

# -----------------------------------------------------------------------------
# Stage 2: Runtime
# Usando a mesma base que Confluent Kafka (Ubuntu 22.04 / jammy)
# -----------------------------------------------------------------------------
FROM ubuntu:22.04 AS runtime

# Re-declare ARGs for labels
ARG VERSION
ARG BUILD_NUMBER
ARG BUILD_DATE
ARG VCS_REF
ARG VCS_URL

# Labels compatíveis com OCI
LABEL org.opencontainers.image.title="Event Horizon"
LABEL org.opencontainers.image.description="Kafka-Compatible Event Streaming Platform"
LABEL org.opencontainers.image.vendor="Event Horizon"
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.created="${BUILD_DATE}"
LABEL org.opencontainers.image.revision="${VCS_REF}"
LABEL org.opencontainers.image.source="${VCS_URL}"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Variáveis de ambiente
ENV DEBIAN_FRONTEND=noninteractive \
    EVENT_HORIZON_HOME=/opt/eventhorizon \
    EVENT_HORIZON_DATA=/var/lib/eventhorizon \
    EVENT_HORIZON_CONFIG=/etc/eventhorizon \
    EVENT_HORIZON_LOGS=/var/log/eventhorizon

# Instalar dependências mínimas de runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libstdc++6 \
    libc6 \
    # Utilitários úteis (como no Kafka)
    curl \
    jq \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Criar usuário não-root (similar ao Kafka)
RUN groupadd --gid 1000 eventhorizon \
    && useradd --uid 1000 --gid eventhorizon --shell /bin/bash --create-home eventhorizon

# Criar diretórios
RUN mkdir -p ${EVENT_HORIZON_HOME}/bin \
             ${EVENT_HORIZON_DATA} \
             ${EVENT_HORIZON_CONFIG} \
             ${EVENT_HORIZON_LOGS} \
    && chown -R eventhorizon:eventhorizon ${EVENT_HORIZON_HOME} \
                                           ${EVENT_HORIZON_DATA} \
                                           ${EVENT_HORIZON_CONFIG} \
                                           ${EVENT_HORIZON_LOGS}

# Copiar binário compilado
COPY --from=builder /src/build/event_horizon ${EVENT_HORIZON_HOME}/bin/

# Copiar configuração padrão (se existir)
COPY --chown=eventhorizon:eventhorizon config.json ${EVENT_HORIZON_CONFIG}/config.json

# Criar script de entrypoint
RUN cat > ${EVENT_HORIZON_HOME}/bin/docker-entrypoint.sh << 'EOF'
#!/bin/bash
set -e

# Variáveis com defaults
BROKER_ID=${BROKER_ID:-0}
BROKER_HOST=${BROKER_HOST:-0.0.0.0}
BROKER_PORT=${BROKER_PORT:-9092}
LOG_LEVEL=${LOG_LEVEL:-info}
DATA_DIR=${EVENT_HORIZON_DATA:-/var/lib/eventhorizon}
CONFIG_FILE=${EVENT_HORIZON_CONFIG}/config.json

# Criar diretório de dados se não existir
mkdir -p "${DATA_DIR}"

echo "Starting Event Horizon..."
echo "  Broker ID: ${BROKER_ID}"
echo "  Host: ${BROKER_HOST}"
echo "  Port: ${BROKER_PORT}"
echo "  Data Dir: ${DATA_DIR}"
echo "  Log Level: ${LOG_LEVEL}"

exec ${EVENT_HORIZON_HOME}/bin/event_horizon \
    --config "${CONFIG_FILE}" \
    --port "${BROKER_PORT}" \
    --data "${DATA_DIR}" \
    --log-level "${LOG_LEVEL}" \
    "$@"
EOF

RUN chmod +x ${EVENT_HORIZON_HOME}/bin/docker-entrypoint.sh \
    && ln -s ${EVENT_HORIZON_HOME}/bin/event_horizon /usr/local/bin/event_horizon \
    && ln -s ${EVENT_HORIZON_HOME}/bin/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

# Mudar para usuário não-root
USER eventhorizon
WORKDIR ${EVENT_HORIZON_HOME}

# Expor porta Kafka padrão
EXPOSE 9092

# Volumes para dados e configuração
VOLUME ["${EVENT_HORIZON_DATA}", "${EVENT_HORIZON_CONFIG}"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD nc -z localhost ${BROKER_PORT:-9092} || exit 1

# Entrypoint e comando padrão
ENTRYPOINT ["docker-entrypoint.sh"]
CMD []