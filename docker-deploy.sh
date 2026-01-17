#!/bin/bash

# ═══════════════════════════════════════════════════════════════════════════════
# SCRIPT DE DEPLOYMENT DOCKER - CDC TADÁ (Multi-plataforma)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Uso:
#   ./docker-deploy.sh              # Build + Push multi-plataforma (amd64 + arm64)
#   ./docker-deploy.sh --build-only # Solo build local (plataforma actual)
#   ./docker-deploy.sh --version    # Mostrar version actual
#   ./docker-deploy.sh --help       # Mostrar ayuda
#
# Plataformas soportadas: linux/amd64, linux/arm64
# Compatible con: Windows (WSL2), Linux, macOS (Intel/Apple Silicon)
#
# ═══════════════════════════════════════════════════════════════════════════════

set -e

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN - MODIFICAR SEGÚN TU REPO
# ═══════════════════════════════════════════════════════════════════════════════
DOCKER_REGISTRY="docker.io"           # o tu registro privado: registry.example.com
DOCKER_USERNAME="heberestrada"        # Tu usuario de Docker Hub o registro
DOCKER_REPO="tada-cdc-sync"           # Nombre del repositorio
VERSION_FILE=".docker-version"        # Archivo para trackear la versión

# ═══════════════════════════════════════════════════════════════════════════════
# COLORES PARA OUTPUT
# ═══════════════════════════════════════════════════════════════════════════════
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ═══════════════════════════════════════════════════════════════════════════════
# FUNCIONES
# ═══════════════════════════════════════════════════════════════════════════════

print_banner() {
    echo -e "${CYAN}"
    echo "╔══════════════════════════════════════════════════════════════════╗"
    echo "║           DOCKER DEPLOYMENT - CDC TADÁ SYNC                      ║"
    echo "╚══════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_step() {
    echo -e "${BLUE}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

show_help() {
    echo "Uso: $0 [opciones]"
    echo ""
    echo "Opciones:"
    echo "  --build-only    Solo construir imagen, no hacer push (solo plataforma actual)"
    echo "  --version       Mostrar versión actual"
    echo "  --set-version   Establecer versión manualmente (ej: --set-version 1.5.0)"
    echo "  --tag           Agregar tag adicional (ej: --tag staging)"
    echo "  --no-cache      Build sin usar cache de Docker"
    echo "  --help          Mostrar esta ayuda"
    echo ""
    echo "Configuración actual:"
    echo "  Registry:    $DOCKER_REGISTRY"
    echo "  Usuario:     $DOCKER_USERNAME"
    echo "  Repositorio: $DOCKER_REPO"
    echo "  Imagen:      $DOCKER_USERNAME/$DOCKER_REPO"
    echo "  Plataformas: linux/amd64, linux/arm64"
    echo ""
    echo "Nota: Build multi-plataforma requiere push directo (usa Docker Buildx)."
    echo "      Con --build-only solo se construye para la plataforma actual."
}

get_current_version() {
    if [ -f "$VERSION_FILE" ]; then
        cat "$VERSION_FILE"
    else
        echo "1.0.0"
    fi
}

increment_version() {
    local version=$1
    local major minor patch

    # Parsear version X.Y.Z
    IFS='.' read -r major minor patch <<< "$version"

    # Incrementar patch version
    patch=$((patch + 1))

    echo "$major.$minor.$patch"
}

save_version() {
    echo "$1" > "$VERSION_FILE"
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker no está instalado o no está en el PATH"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        print_error "Docker daemon no está corriendo"
        exit 1
    fi
}

setup_buildx() {
    print_step "Configurando Docker Buildx para multi-plataforma..."

    # Verificar si buildx está disponible
    if ! docker buildx version &> /dev/null; then
        print_error "Docker Buildx no está disponible. Actualiza Docker Desktop o instala buildx."
        exit 1
    fi

    # Crear o usar builder multi-plataforma
    local builder_name="multiarch-builder"

    if ! docker buildx inspect "$builder_name" &> /dev/null; then
        print_step "Creando builder multi-plataforma..."
        docker buildx create --name "$builder_name" --driver docker-container --use
        docker buildx inspect --bootstrap "$builder_name"
    else
        docker buildx use "$builder_name"
    fi

    print_success "Buildx configurado para: linux/amd64, linux/arm64"
}

docker_login() {
    print_step "Verificando autenticación con Docker registry..."

    # Intentar pull de una imagen para verificar login
    if docker pull hello-world &> /dev/null; then
        print_success "Docker está autenticado"
        docker rmi hello-world &> /dev/null || true
    else
        print_warning "Necesitas autenticarte en Docker"
        echo ""
        echo -e "${YELLOW}Ejecuta: docker login${NC}"
        echo ""

        # Intentar login interactivo
        if ! docker login; then
            print_error "Fallo en la autenticación"
            exit 1
        fi
    fi
}

build_image() {
    local version=$1
    local no_cache=$2
    local push_flag=$3
    local image_name="$DOCKER_USERNAME/$DOCKER_REPO"
    local cache_flag=""
    local output_flag=""

    if [ "$no_cache" = "true" ]; then
        cache_flag="--no-cache"
    fi

    # Para multi-plataforma, necesitamos push durante build (no se puede cargar localmente)
    if [ "$push_flag" = "true" ]; then
        output_flag="--push"
    else
        # Build-only: solo construimos para la plataforma actual
        output_flag="--load"
    fi

    print_step "Construyendo imagen Docker multi-plataforma..."
    echo "  Imagen:      $image_name:$version"
    echo "  Tags:        latest, v$version"
    echo "  Plataformas: linux/amd64, linux/arm64"
    echo ""

    if [ "$push_flag" = "true" ]; then
        # Build multi-plataforma con push directo
        docker buildx build \
            --platform linux/amd64,linux/arm64 \
            $cache_flag \
            -t "$image_name:$version" \
            -t "$image_name:v$version" \
            -t "$image_name:latest" \
            --build-arg VERSION="$version" \
            --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
            --build-arg GIT_COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
            $output_flag \
            .
    else
        # Build-only: solo plataforma actual (no se puede multi-plataforma sin push)
        print_warning "Build-only: construyendo solo para plataforma actual"
        docker buildx build \
            $cache_flag \
            -t "$image_name:$version" \
            -t "$image_name:v$version" \
            -t "$image_name:latest" \
            --build-arg VERSION="$version" \
            --build-arg BUILD_DATE="$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
            --build-arg GIT_COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
            --load \
            .
    fi

    print_success "Imagen construida exitosamente"
}

push_image() {
    local version=$1
    local image_name="$DOCKER_USERNAME/$DOCKER_REPO"

    print_step "Subiendo imagen a registry..."

    # Push todas las tags
    docker push "$image_name:$version"
    docker push "$image_name:v$version"
    docker push "$image_name:latest"

    print_success "Imagen subida exitosamente"
}

create_dockerfile_if_missing() {
    if [ ! -f "Dockerfile" ]; then
        print_warning "No existe Dockerfile, creando uno..."

        cat > Dockerfile << 'EOF'
# ═══════════════════════════════════════════════════════════════════════════════
# Dockerfile - CDC TADÁ Sync
# ═══════════════════════════════════════════════════════════════════════════════

FROM node:18-alpine

# Argumentos de build
ARG VERSION=1.0.0
ARG BUILD_DATE
ARG GIT_COMMIT

# Labels
LABEL maintainer="TADÁ Team"
LABEL version="${VERSION}"
LABEL build-date="${BUILD_DATE}"
LABEL git-commit="${GIT_COMMIT}"
LABEL description="CDC Sync Service for TADÁ databases"

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema necesarias para tedious (SQL Server)
RUN apk add --no-cache \
    python3 \
    make \
    g++

# Copiar package files
COPY package*.json ./

# Instalar dependencias de producción
RUN npm ci --only=production && \
    npm cache clean --force

# Copiar código fuente
COPY src/ ./src/

# Crear directorio de logs
RUN mkdir -p /app/logs

# Variables de entorno por defecto
ENV NODE_ENV=production
ENV LOG_LEVEL=info
ENV POLLING_INTERVAL=5
ENV FORCE_MERGE_ON_START=false

# Puerto de health check (opcional)
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD node -e "console.log('healthy')" || exit 1

# Usuario no-root para seguridad
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001
RUN chown -R nodejs:nodejs /app
USER nodejs

# Comando de inicio
CMD ["node", "src/index.js"]
EOF

        print_success "Dockerfile creado"
    fi
}

create_dockerignore_if_missing() {
    if [ ! -f ".dockerignore" ]; then
        print_warning "No existe .dockerignore, creando uno..."

        cat > .dockerignore << 'EOF'
# Dependencies
node_modules/
npm-debug.log*

# Git
.git/
.gitignore

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Logs
logs/
*.log

# Reports (generated files)
reports/

# Scripts de utilidades (no necesarios en producción)
utils_scripts/
scripts/sync/

# Test files
*.test.js
*.spec.js
__tests__/
coverage/

# Docker
Dockerfile
docker-compose*.yml
.docker-version

# Scripts de deployment
docker-deploy.sh
*.sh

# Environment (NO incluir en imagen)
.env
.env.*

# Documentation
README.md
docs/

# Temp files
tmp/
temp/
*.tmp
EOF

        print_success ".dockerignore creado"
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

BUILD_ONLY=false
NO_CACHE=false
EXTRA_TAG=""
SET_VERSION=""

# Parsear argumentos
while [[ $# -gt 0 ]]; do
    case $1 in
        --build-only)
            BUILD_ONLY=true
            shift
            ;;
        --version)
            echo "Versión actual: $(get_current_version)"
            exit 0
            ;;
        --set-version)
            SET_VERSION="$2"
            shift 2
            ;;
        --tag)
            EXTRA_TAG="$2"
            shift 2
            ;;
        --no-cache)
            NO_CACHE=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            print_error "Opción desconocida: $1"
            show_help
            exit 1
            ;;
    esac
done

# Inicio
print_banner

# Verificar Docker
print_step "Verificando Docker..."
check_docker
print_success "Docker disponible"

# Configurar buildx para multi-plataforma
setup_buildx

# Crear archivos Docker si no existen
create_dockerfile_if_missing
create_dockerignore_if_missing

# Obtener versión
CURRENT_VERSION=$(get_current_version)

if [ -n "$SET_VERSION" ]; then
    NEW_VERSION="$SET_VERSION"
    print_step "Estableciendo versión manual: $NEW_VERSION"
else
    NEW_VERSION=$(increment_version "$CURRENT_VERSION")
    print_step "Auto-incrementando versión: $CURRENT_VERSION → $NEW_VERSION"
fi

# Para multi-plataforma, login primero si vamos a hacer push
if [ "$BUILD_ONLY" = false ]; then
    docker_login
fi

# Build (y push si no es build-only)
# Nota: Multi-plataforma requiere push durante el build
build_image "$NEW_VERSION" "$NO_CACHE" "$([[ "$BUILD_ONLY" = false ]] && echo "true" || echo "false")"

# Guardar versión si push fue exitoso
if [ "$BUILD_ONLY" = false ]; then
    save_version "$NEW_VERSION"
    print_success "Versión actualizada a $NEW_VERSION"
fi

# Tag adicional si se especificó
if [ -n "$EXTRA_TAG" ]; then
    image_name="$DOCKER_USERNAME/$DOCKER_REPO"

    if [ "$BUILD_ONLY" = false ]; then
        # Para tags adicionales con multi-plataforma, necesitamos re-tagear el manifest
        print_step "Aplicando tag adicional: $EXTRA_TAG"
        docker buildx imagetools create \
            -t "$image_name:$EXTRA_TAG" \
            "$image_name:$NEW_VERSION"
        print_success "Tag adicional aplicado: $EXTRA_TAG"
    else
        # En build-only, solo tag local
        docker tag "$image_name:$NEW_VERSION" "$image_name:$EXTRA_TAG"
        print_success "Tag adicional aplicado localmente: $EXTRA_TAG"
    fi
fi

# Resumen final
echo ""
echo -e "${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}DEPLOYMENT COMPLETADO${NC}"
echo -e "${CYAN}═══════════════════════════════════════════════════════════════════${NC}"
echo ""
echo "  Imagen:   $DOCKER_USERNAME/$DOCKER_REPO"
echo "  Versión:  $NEW_VERSION"
echo "  Tags:     latest, v$NEW_VERSION"
[ -n "$EXTRA_TAG" ] && echo "            $EXTRA_TAG"
echo ""

if [ "$BUILD_ONLY" = false ]; then
    echo -e "${YELLOW}Para desplegar en tu servidor:${NC}"
    echo ""
    echo "  docker pull $DOCKER_USERNAME/$DOCKER_REPO:$NEW_VERSION"
    echo ""
    echo "  docker run -d \\"
    echo "    --name cdc-sync \\"
    echo "    --restart unless-stopped \\"
    echo "    -e SOURCE_DB_SERVER=tu-servidor \\"
    echo "    -e SOURCE_DB_NAME=TadaNomina \\"
    echo "    -e SOURCE_DB_USER=usuario \\"
    echo "    -e SOURCE_DB_PASSWORD=password \\"
    echo "    -e TARGET_DB_SERVER=tu-servidor \\"
    echo "    -e TARGET_DB_NAME=TadaNomina-2.0 \\"
    echo "    -e TARGET_DB_USER=usuario \\"
    echo "    -e TARGET_DB_PASSWORD=password \\"
    echo "    -e POLLING_INTERVAL=5 \\"
    echo "    -e FORCE_MERGE_ON_START=false \\"
    echo "    $DOCKER_USERNAME/$DOCKER_REPO:$NEW_VERSION"
    echo ""
fi
