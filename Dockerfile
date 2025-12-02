# Dockerfile
FROM node:18-alpine

# Crear directorio de la aplicación
WORKDIR /usr/src/app

# Copiar archivos de configuración
COPY package*.json ./

# Instalar dependencias
RUN npm ci --only=production

# Copiar código fuente
COPY . .

# Puerto de la aplicación
EXPOSE 3000

# Comando para iniciar la aplicación
CMD [ "node", "src/index.js" ]
