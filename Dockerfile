# Dockerfile - CDC TADÁ Sync
FROM node:18-alpine

# Crear directorio de la aplicación
WORKDIR /usr/src/app

# Instalar dependencias del sistema necesarias para tedious (SQL Server driver)
# - python3, make, g++: necesarios para compilar módulos nativos
# - openssl: necesario para conexiones TLS a SQL Server
RUN apk add --no-cache \
    python3 \
    make \
    g++ \
    openssl

# Copiar archivos de configuración
COPY package*.json ./

# Instalar dependencias
RUN npm ci --only=production && \
    npm cache clean --force

# Copiar código fuente
COPY . .

# Crear directorio de logs
RUN mkdir -p /app/logs

# Variables de entorno por defecto
ENV NODE_ENV=production

# Puerto de la aplicación
EXPOSE 3000

# Comando para iniciar la aplicación
CMD [ "node", "src/index.js" ]
