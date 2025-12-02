// README.md
# Sistema de Sincronización CDC con SQL Server y Node.js

## Descripción
Sistema de sincronización en tiempo real que utiliza Change Data Capture (CDC) de SQL Server para mantener sincronizadas múltiples tablas entre diferentes bases de datos, aplicando reglas de negocio personalizadas.

## Características
- ✅ Sincronización en tiempo real usando CDC
- ✅ Soporte para múltiples bases de datos
- ✅ Reglas de negocio personalizables
- ✅ Logging detallado
- ✅ Manejo de errores robusto
- ✅ Configuración flexible

## Requisitos
- Node.js 14+
- SQL Server 2016+ (con CDC habilitado)
- Permisos de sysadmin en SQL Server

## Instalación

1. Instalar dependencias:
```bash
npm install
```

2. Configurar variables de entorno:
```bash
cp .env.example .env
# Editar .env con tus configuraciones
```

3. Configurar CDC en SQL Server:
```bash
npm run setup-cdc
```

4. Iniciar la aplicación:
```bash
npm start
```

## Configuración

### Variables de Entorno
- `SOURCE_DB_*`: Configuración de la base de datos origen
- `TARGET_DB_*`: Configuración de la base de datos destino
- `POLLING_INTERVAL`: Intervalo de sincronización en segundos
- `LOG_LEVEL`: Nivel de logging (debug, info, warn, error)

### Reglas de Negocio
Puedes agregar reglas personalizadas en `src/services/businessRules.js`:

```javascript
// Ejemplo de regla personalizada
this.addRule('mi_tabla', 'mi_regla', (data, operation) => {
  if (data.campo === 'valor_especial') {
    data.campo_modificado = 'nuevo_valor';
  }
  return data;
});
```

## Uso
La aplicación monitoreará automáticamente los cambios en las tablas configuradas y los sincronizará según las reglas definidas.

## Logging
Los logs se guardan en:
- `logs/error.log`: Solo errores
- `logs/combined.log`: Todos los eventos
- Consola: Salida en tiempo real

## Despliegue con Docker

### Construir la imagen localmente
```bash
docker build -t cdc-sync .
```

### Ejecutar el contenedor
```bash
docker run -d \
  --env-file .env \
  -v $(pwd)/logs:/usr/src/app/logs \
  --name cdc-sync \
  cdc-sync
```

### Usar docker-compose (desarrollo)
```bash
docker-compose up -d
```

## Configuración de GitHub Actions

El workflow está configurado para:
- Construir la imagen Docker en cada push a la rama main
- Hacer push de la imagen a Docker Hub
- Usar caché para builds más rápidos

### Secrets requeridos en GitHub:
- `DOCKERHUB_USERNAME`: Tu nombre de usuario de Docker Hub
- `DOCKERHUB_TOKEN`: Token de acceso a Docker Hub

## Contribución
1. Fork el proyecto
2. Crea una rama para tu feature
3. Commit tus cambios
4. Push a la rama
5. Abre un Pull Request
