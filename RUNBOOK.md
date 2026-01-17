# Runbook - CDC TADA Sync

Sistema de sincronización en tiempo real usando Change Data Capture (CDC) de SQL Server.

---

## Tabla de Contenidos

1. [Descripción General](#descripción-general)
2. [Requisitos Previos](#requisitos-previos)
3. [Configuración](#configuración)
4. [Comparación de Estructuras](#comparación-de-estructuras)
5. [Comparación de Tablas Compartidas](#comparación-de-tablas-compartidas)
6. [Validación de Datos Post-Full Load](#validación-de-datos-post-full-load)
7. [Infraestructura de Logging y Auditoría](#infraestructura-de-logging-y-auditoría)
8. [Despliegue con Docker](#despliegue-con-docker)
9. [Comandos Útiles](#comandos-útiles)
10. [Monitoreo y Logs](#monitoreo-y-logs)
11. [Troubleshooting](#troubleshooting)
12. [Arquitectura](#arquitectura)
13. [Resumen de Scripts Disponibles](#resumen-de-scripts-disponibles)

---

## Descripción General

**CDC TADA Sync** es un servicio Node.js que sincroniza datos entre dos bases de datos SQL Server en tiempo real utilizando Change Data Capture (CDC).

### Características principales:
- Snapshot inicial de todas las tablas configuradas
- Sincronización incremental en tiempo real usando CDC
- Reintentos automáticos con backoff exponencial
- Soporte para más de 300 tablas
- Logging detallado con Winston

---

## Requisitos Previos

### En el servidor de bases de datos (SQL Server)

1. **SQL Server 2016 o superior** con CDC habilitado
2. **SQL Server Agent** debe estar corriendo (requerido para CDC)
3. Usuario con permisos suficientes:
   - `db_owner` en ambas bases de datos
   - Permisos para ejecutar `sp_cdc_enable_db` y `sp_cdc_enable_table`

### En el servidor donde corre Docker

1. **Docker** >= 20.10
2. **Docker Compose** >= 2.0
3. Conectividad de red hacia el SQL Server (puerto 1433)

---

## Configuración

### Variables de Entorno

Crear un archivo `.env` en el directorio del proyecto o configurar las variables en `docker-compose.yml`:

```env
# Base de datos ORIGEN (de donde se leen los cambios)
SOURCE_DB_SERVER=172.17.0.247
SOURCE_DB_NAME=TadaNomina
SOURCE_DB_USER=usuario
SOURCE_DB_PASSWORD=tu_password_seguro

# Base de datos DESTINO (donde se replican los cambios)
TARGET_DB_SERVER=172.17.0.247
TARGET_DB_NAME=TadaNomina-2.0
TARGET_DB_USER=usuario
TARGET_DB_PASSWORD=tu_password_seguro

# Configuración de sincronización
POLLING_INTERVAL=5              # Segundos entre cada verificación de cambios
FORCE_MERGE_ON_START=false      # true = fuerza MERGE inicial en todas las tablas
LOG_LEVEL=info                  # Niveles: error, warn, info, debug

# Zona horaria
TZ=America/Mexico_City
```

### Tablas a Sincronizar

Las tablas se configuran en `src/config/tablesToSync.js`. El archivo contiene un array con el nombre y schema de cada tabla:

```javascript
module.exports = [
  { name: 'Empleados', schema: 'dbo' },
  { name: 'Usuarios', schema: 'dbo' },
  // ... más tablas
];
```

---

## Comparación de Estructuras

Antes de iniciar la sincronización, es importante verificar que las estructuras de las bases de datos origen y destino sean compatibles.

### Pares de Bases de Datos

El sistema compara las siguientes bases de datos:

| Par | Base Origen             | Base Destino              |
| --- | ----------------------- | ------------------------- |
| 1   | TadaNomina              | TadaNomina-2.0            |
| 2   | TadaModuloSeguridad     | TadaModuloSeguridad-2.0   |
| 3   | TadaChecador            | TadaChecador-2.0          |

### Ejecutar Comparación

```bash
# Desde el directorio del proyecto
npm run compare-db

# O directamente
node scripts/compare-db-structures.js
```

### Reporte Generado

El script genera un reporte Excel en `reports/` con:

- **Hoja "Resumen Ejecutivo"**: Vista general de los 3 pares con estado y total de diferencias
- **Hoja "Sección de Aprobación"**: Espacio para firmas del equipo antes de proceder
- **Hojas por par**: Diferencias detalladas (tablas, columnas, índices)
- **Hojas de estructura**: Listado completo de tablas de cada base

### Criterios de Aceptación

| Criterio               | Descripción                                          |
| ---------------------- | ---------------------------------------------------- |
| Formato legible        | Reporte en Excel (.xlsx) con formato profesional     |
| Diferencias detalladas | Incluye tablas, columnas, índices y tipos de datos   |
| Estructuras actuales   | Cada base tiene su hoja con listado de tablas        |
| Sin diferencias        | Marca explícitamente "SIN DIFERENCIAS" si coincide   |
| Aprobación             | Sección para firma de al menos un miembro del equipo |

### Ejemplo de Salida

```text
═══ Procesando: TadaNomina ═══
Origen: TadaNomina
Destino: TadaNomina-2.0

Obteniendo estructura de ORIGEN:
  Conectando a TadaNomina...
  Obteniendo tablas...
  Obteniendo columnas...

Comparando estructuras...
  ✅ SIN DIFERENCIAS

═══ Generando reporte Excel ═══
✅ Reporte generado: reports/db-structure-comparison-2026-01-10T12-00-00.xlsx
```

---

## Comparación de Tablas Compartidas

Para comparar únicamente las tablas que comparten funcionalidad entre TadaNomina 1.0 y 2.0, excluyendo las tablas de empleados/personas que fueron rediseñadas para TADÁ 2.0.

### Contexto

En TADÁ 2.0, la estructura de empleados/personas fue completamente rediseñada. Por lo tanto, no tiene sentido comparar estas tablas entre versiones. Este script permite comparar SOLO las tablas que mantienen funcionalidad compartida.

### Tablas Excluidas

Las siguientes tablas se excluyen automáticamente de la comparación:

| Categoría              | Tablas Excluidas                                                             |
| ---------------------- | ---------------------------------------------------------------------------- |
| Empleados rediseñados  | Empleados, Empleados_B, EmpleadoInformacionComplementaria, InfoEmpleado      |
| Historial de empleados | HistorialEmpleados, Historial_Estatus_Empleado, EmpleadosAccionDisciplinaria |
| Catálogos de empleados | Cat_EstatusEmpleado, Cat_EmpleadoSolicitud, Cat_EmpleadosServicio            |
| Tablas de sistema      | CDC_SyncLog, sysdiagrams, systranschemas                                     |
| Tablas temporales      | tmp_BajaCliente, tmp_BajaCliente_Wingstop, dbo.tmp_ag_siem                   |
| Logs de migración      | LogErrores_MaestroPersonaDatosTada20, LogErrores_personasTada20              |

### Ejecutar Comparación Filtrada

```bash
# Desde el directorio del proyecto
npm run compare-shared

# O directamente
node scripts/compare-shared-tables.js
```

### Reporte de Tablas Compartidas

El script genera un reporte Excel consolidado en `reports/` con las siguientes hojas:

| Hoja                    | Contenido                                                    |
| ----------------------- | ------------------------------------------------------------ |
| Resumen Ejecutivo       | Métricas generales, estado y sección de aprobación           |
| Tablas Excluidas        | Lista completa de tablas no comparadas con razón             |
| Diferencias Detalladas  | Todas las diferencias en tablas, columnas e índices          |
| Tablas Comparadas       | Conteo de registros en origen vs destino para cada tabla     |

### Ejemplo de Ejecución

```text
╔══════════════════════════════════════════════════════════════════╗
║  COMPARACIÓN DE ESTRUCTURAS - TABLAS CON FUNCIONALIDAD COMPARTIDA║
║                                                                  ║
║  NOTA: Se excluyen tablas de Empleados/Personas (TADÁ 2.0)       ║
╚══════════════════════════════════════════════════════════════════╝

═══ Configuración ═══
  Tablas a comparar: 302
  Tablas excluidas: 25

═══ Obteniendo estructura de ORIGEN ═══
  Conectando a TadaNomina...
  Tablas encontradas: 298

═══ Obteniendo estructura de DESTINO ═══
  Conectando a TadaNomina-2.0...
  Tablas encontradas: 298

═══ Comparando estructuras ═══
  SIN DIFERENCIAS en tablas compartidas

═══ Generando reporte consolidado ═══
  Reporte generado: reports/shared-tables-comparison-2026-01-12T10-30-00.xlsx

IMPORTANTE:
  - Se compararon SOLO tablas con funcionalidad compartida
  - Las tablas de Empleados/Personas de TADÁ 2.0 fueron EXCLUIDAS
  - Revisar el reporte Excel para detalles completos
```

### Diferencia con compare-db

| Característica         | compare-db                     | compare-shared                          |
| ---------------------- | ------------------------------ | --------------------------------------- |
| Alcance                | Todas las tablas               | Solo tablas compartidas                 |
| Tablas de empleados    | Incluidas                      | Excluidas                               |
| Pares de BD            | 3 pares configurables          | Solo TadaNomina                         |
| Uso recomendado        | Comparación inicial completa   | Validación para migración TADÁ 2.0      |

---

## Validación de Datos Post-Full Load

Después del Full Load, es crítico validar que los datos sean consistentes entre la BD 1.0 (origen) y BD 2.0 (destino).

### Ejecutar Validación

```bash
# Desde el directorio del proyecto
npm run validate-data

# O directamente
node scripts/validate-data-consistency.js
```

### Validaciones Realizadas

| Validación              | Descripción                                              |
| ----------------------- | -------------------------------------------------------- |
| Conteo de registros     | Compara el número de filas en cada tabla                 |
| Checksums               | Verifica integridad con CHECKSUM_AGG en tablas críticas  |
| Claves primarias        | Valida que las PKs coincidan entre origen y destino      |
| Muestreo de datos       | Compara registros específicos en tablas críticas         |
| Distribución de NULLs   | Detecta diferencias en valores nulos por columna         |

### Tablas Críticas Validadas

Las siguientes tablas reciben validación exhaustiva:

- `Empleados`
- `Usuarios`
- `Nomina`
- `PeriodoNomina`
- `Cat_Clientes`
- `Cat_UnidadNegocio`

### Reporte de Validación

El script genera un reporte Excel en `reports/` con:

| Hoja                    | Contenido                                                |
| ----------------------- | -------------------------------------------------------- |
| Resumen Ejecutivo       | Estado general de cada par de BD                         |
| [Par] - Conteos         | Discrepancias en número de registros                     |
| [Par] - Checksums       | Resultados de validación de integridad                   |
| [Par] - Resumen         | Resumen de todas las validaciones                        |
| Causas y Correcciones   | Plantilla para documentar y corregir discrepancias       |

### Flujo de Corrección de Discrepancias

```text
1. Ejecutar validación
   └─> npm run validate-data

2. Revisar reporte Excel
   └─> reports/data-validation-[timestamp].xlsx

3. Para cada discrepancia:
   ├─> Identificar causa raíz
   ├─> Documentar en hoja "Causas y Correcciones"
   └─> Aplicar corrección

4. Re-ejecutar validación
   └─> npm run validate-data

5. Verificar que todas las discrepancias estén resueltas
   └─> Estado: "CONSISTENTE" para todos los pares

6. Obtener aprobación del equipo
   └─> Firmar sección de aprobación en el reporte
```

### Ejemplo de Ejecución

```text
╔══════════════════════════════════════════════════════════════════╗
║   VALIDACIÓN DE CONSISTENCIA DE DATOS - POST FULL LOAD          ║
╚══════════════════════════════════════════════════════════════════╝

═══ Validando: TadaNomina ═══
  Conectando a bases de datos...
    ✓ Conexiones establecidas
  Validando conteos de registros...
    ✓ 320 tablas coinciden
    ⚠ 5 tablas con diferencias
  Validando checksums de tablas críticas...
    ✓ 6 checksums coinciden
  Validando claves primarias...
    ✓ 325 PKs coinciden

═══ RESUMEN DE VALIDACIÓN ═══
  TadaNomina: ⚠️  5 discrepancias
  TadaModuloSeguridad: ✅ CONSISTENTE
  TadaChecador: ✅ CONSISTENTE

⚠️  ACCIÓN REQUERIDA:
   1. Revisar el reporte Excel generado
   2. Documentar las causas en la hoja "Causas y Correcciones"
   3. Aplicar correcciones necesarias
   4. Re-ejecutar esta validación
```

### Estados de Validación

| Estado             | Significado                                              |
| ------------------ | -------------------------------------------------------- |
| CONSISTENTE        | Todos los datos coinciden entre origen y destino         |
| X DISCREPANCIAS    | Se encontraron X diferencias que deben investigarse      |
| ERROR DE CONEXIÓN  | No se pudo conectar a una o ambas bases de datos         |

---

## Infraestructura de Logging y Auditoría

El sistema incluye una infraestructura completa de logging para registrar cada operación de replicación y facilitar la depuración y monitoreo.

### Crear Tabla de Auditoría

Antes de iniciar la replicación, crear la tabla de auditoría:

```bash
npm run create-auditoria
```

### Estructura de la Tabla AuditoriaReplicacion

| Columna              | Tipo              | Descripción                                         |
| -------------------- | ----------------- | --------------------------------------------------- |
| AuditoriaId          | BIGINT            | ID único del registro                               |
| FechaHora            | DATETIME2         | Timestamp de la operación                           |
| ProcesoId            | NVARCHAR(100)     | ID del proceso (PID-xxx-timestamp)                  |
| BaseDatosOrigen      | NVARCHAR(128)     | Base de datos origen                                |
| BaseDatosDestino     | NVARCHAR(128)     | Base de datos destino                               |
| EsquemaTabla         | NVARCHAR(128)     | Schema de la tabla (ej: dbo)                        |
| NombreTabla          | NVARCHAR(128)     | Nombre de la tabla                                  |
| TipoOperacion        | VARCHAR(20)       | INSERT, UPDATE, DELETE, MERGE, etc.                 |
| CodigoOperacionCDC   | TINYINT           | 1=DELETE, 2=INSERT, 3=UPDATE_BEFORE, 4=UPDATE_AFTER |
| RegistroId           | NVARCHAR(100)     | ID del registro afectado                            |
| Estado               | VARCHAR(20)       | SUCCESS, ERROR, SKIPPED, RETRY                      |
| MensajeError         | NVARCHAR(MAX)     | Mensaje de error si aplica                          |
| DatosAntes           | NVARCHAR(MAX)     | JSON con datos anteriores                           |
| DatosDespues         | NVARCHAR(MAX)     | JSON con datos nuevos                               |
| TiempoEjecucionMs    | INT               | Tiempo de ejecución en ms                           |
| RegistrosProcesados  | INT               | Registros en operación batch                        |
| LSN_Inicio           | VARBINARY(10)     | LSN de inicio del cambio                            |
| LSN_Fin              | VARBINARY(10)     | LSN de fin del cambio                               |

### Consultar Auditoría

```bash
# Ver resumen de auditoría
npm run view-auditoria

# Ver solo errores
npm run view-auditoria -- --errores

# Filtrar por tabla
npm run view-auditoria -- --tabla Empleados

# Últimas 24 horas
npm run view-auditoria -- --ultimas 24h

# Estadísticas agregadas
npm run view-auditoria -- --stats

# Exportar a JSON
npm run view-auditoria -- --export

# Combinar filtros
npm run view-auditoria -- --errores --ultimas 1h --export
```

### Ejemplo de Consulta de Auditoría

```text
╔══════════════════════════════════════════════════════════════════╗
║   Visor de Auditoría de Replicación CDC                          ║
╚══════════════════════════════════════════════════════════════════╝

--- Resumen General ---
  Total operaciones:    15,432
  Exitosas:             15,398
  Errores:              34
  Omitidas:             0
  Total registros:      1,245,678
  Tiempo promedio:      45 ms

--- Top 20 Tablas por Operaciones ---
┌─────────┬─────────────────────┬────────────┬─────┬─────────┬─────────────────┐
│ Tabla   │ Operaciones         │ OK         │ Err │ Registros│ Tiempo Prom (ms)│
├─────────┼─────────────────────┼────────────┼─────┼─────────┼─────────────────┤
│ Empleados│ 5,234              │ 5,230      │ 4   │ 125,456 │ 23              │
│ Nomina  │ 3,421               │ 3,421      │ 0   │ 89,234  │ 67              │
└─────────┴─────────────────────┴────────────┴─────┴─────────┴─────────────────┘
```

### Servicio de Auditoría en Código

El servicio `AuditoriaService` está disponible para uso programático:

```javascript
const AuditoriaService = require('./services/auditoriaService');

// Inicializar
const auditoria = new AuditoriaService(connectionRunner, {
  processId: 'mi-proceso',
  sourceDb: 'TadaNomina',
  targetDb: 'TadaNomina-2.0',
  bufferSize: 50,      // Registros antes de flush automático
  flushInterval: 5000  // ms entre flushes
});

// Registrar operaciones
await auditoria.registrarInsert('Empleados', 12345, datos, tiempoMs);
await auditoria.registrarUpdate('Empleados', 12345, antes, despues, tiempoMs);
await auditoria.registrarError('Empleados', 'INSERT', 12345, error);

// Consultar
const errores = await auditoria.getErroresRecientes(10);
const stats = await auditoria.getEstadisticasDB();

// Detener (flush final)
await auditoria.stop();
```

---

## Despliegue con Docker

### Opción 1: Build Local

#### 1. Clonar el repositorio

```bash
git clone <url-del-repositorio>
cd cdc
```

#### 2. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con los valores correctos
nano .env
```

#### 3. Construir la imagen

```bash
docker build -t tada-cdc-sync:latest .
```

#### 4. Iniciar el servicio

```bash
# Usando docker-compose (recomendado)
docker-compose up -d

# O usando docker run directamente
docker run -d \
  --name cdc-sync \
  --restart unless-stopped \
  --env-file .env \
  -v cdc-logs:/app/logs \
  tada-cdc-sync:latest
```

### Opción 2: Usando Imagen del Registry

#### 1. Crear archivo docker-compose.yml

```yaml
version: '3.8'

services:
  cdc-sync:
    image: tuusuario/tada-cdc-sync:latest
    container_name: cdc-sync
    restart: unless-stopped
    environment:
      - SOURCE_DB_SERVER=172.17.0.247
      - SOURCE_DB_NAME=TadaNomina
      - SOURCE_DB_USER=usuario
      - SOURCE_DB_PASSWORD=password
      - TARGET_DB_SERVER=172.17.0.247
      - TARGET_DB_NAME=TadaNomina-2.0
      - TARGET_DB_USER=usuario
      - TARGET_DB_PASSWORD=password
      - POLLING_INTERVAL=5
      - FORCE_MERGE_ON_START=false
      - LOG_LEVEL=info
      - NODE_ENV=production
      - TZ=America/Mexico_City
    volumes:
      - cdc-logs:/app/logs
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 512M
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

volumes:
  cdc-logs:
```

#### 2. Iniciar el servicio

```bash
docker-compose up -d
```

---

## Comandos Útiles

### Gestión del Contenedor

```bash
# Iniciar el servicio
docker-compose up -d

# Detener el servicio
docker-compose down

# Reiniciar el servicio
docker-compose restart

# Ver estado del contenedor
docker-compose ps

# Ver logs en tiempo real
docker-compose logs -f

# Ver últimas 100 líneas de logs
docker-compose logs --tail=100

# Entrar al contenedor
docker exec -it cdc-sync sh

# Verificar salud del contenedor
docker inspect --format='{{.State.Health.Status}}' cdc-sync
```

### Actualización del Servicio

```bash
# Actualizar a nueva versión
docker-compose pull
docker-compose up -d

# Rebuild local
docker-compose build --no-cache
docker-compose up -d
```

### Limpieza

```bash
# Eliminar contenedor y volúmenes
docker-compose down -v

# Eliminar imágenes no utilizadas
docker image prune -f
```

---

## Monitoreo y Logs

### Ubicación de Logs

Los logs se almacenan en:
- **Dentro del contenedor**: `/app/logs/`
- **Volumen Docker**: `cdc-logs`

### Estructura de Logs

```
logs/
├── combined.log    # Todos los logs
└── error.log       # Solo errores
```

### Ver Logs

```bash
# Logs en tiempo real
docker-compose logs -f cdc-sync

# Solo errores
docker-compose logs -f cdc-sync 2>&1 | grep -i error

# Logs del volumen
docker run --rm -v cdc-logs:/logs alpine cat /logs/combined.log
```

### Indicadores de Salud

El contenedor incluye un health check que verifica cada 30 segundos:

```bash
# Ver estado de salud
docker inspect --format='{{json .State.Health}}' cdc-sync | jq
```

---

## Troubleshooting

### Error: "No se puede conectar a SQL Server"

**Causa**: El servidor SQL no es accesible desde el contenedor.

**Solución**:
```bash
# Verificar conectividad desde el host
telnet 172.17.0.247 1433

# Verificar desde el contenedor
docker exec -it cdc-sync sh -c "nc -zv 172.17.0.247 1433"
```

### Error: "CDC not enabled"

**Causa**: CDC no está habilitado en la base de datos origen.

**Solución**:
```sql
-- Habilitar CDC en la base de datos
USE TadaNomina;
EXEC sys.sp_cdc_enable_db;

-- Verificar que está habilitado
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'TadaNomina';
```

### Error: "SQL Server Agent is not running"

**Causa**: El agente de SQL Server debe estar corriendo para CDC.

**Solución**:
```sql
-- Verificar estado del agente
EXEC xp_servicecontrol 'QUERYSTATE', 'SQLServerAgent';

-- Iniciar el agente (requiere permisos de admin)
EXEC xp_servicecontrol 'START', 'SQLServerAgent';
```

### El contenedor se reinicia constantemente

**Causa**: Error de conexión o configuración incorrecta.

**Solución**:
```bash
# Ver logs de error
docker-compose logs --tail=50 cdc-sync

# Verificar variables de entorno
docker exec cdc-sync env | grep DB
```

### Error de memoria

**Causa**: El contenedor excede el límite de memoria.

**Solución**: Aumentar el límite en `docker-compose.yml`:
```yaml
deploy:
  resources:
    limits:
      memory: 1G
```

### Tabla no se sincroniza

**Causa**: La tabla no existe en origen o no tiene CDC habilitado.

**Solución**:
```sql
-- Verificar si la tabla tiene CDC
SELECT name, is_tracked_by_cdc
FROM sys.tables
WHERE name = 'NombreTabla';

-- Habilitar CDC manualmente
EXEC sys.sp_cdc_enable_table
  @source_schema = N'dbo',
  @source_name = N'NombreTabla',
  @role_name = NULL,
  @supports_net_changes = 1;
```

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                        CDC TADA Sync                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │
│  │   index.js  │────▶│ syncService │────▶│  cdcService │       │
│  │   (main)    │     │             │     │             │       │
│  └─────────────┘     └─────────────┘     └─────────────┘       │
│         │                   │                   │               │
│         ▼                   ▼                   ▼               │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐       │
│  │tablesToSync │     │businessRules│     │  database   │       │
│  │   config    │     │             │     │  (tedious)  │       │
│  └─────────────┘     └─────────────┘     └─────────────┘       │
│                                                 │               │
└─────────────────────────────────────────────────┼───────────────┘
                                                  │
                    ┌─────────────────────────────┴─────────────────────────────┐
                    │                                                           │
                    ▼                                                           ▼
        ┌───────────────────────┐                               ┌───────────────────────┐
        │   SQL Server ORIGEN   │                               │  SQL Server DESTINO   │
        │     TadaNomina        │                               │   TadaNomina-2.0      │
        │                       │                               │                       │
        │  ┌─────────────────┐  │                               │  ┌─────────────────┐  │
        │  │ Tablas con CDC  │  │         Replicación           │  │ Tablas destino  │  │
        │  │   habilitado    │──┼──────────────────────────────▶│  │                 │  │
        │  └─────────────────┘  │                               │  └─────────────────┘  │
        └───────────────────────┘                               └───────────────────────┘
```

### Flujo de Sincronización

1. **Inicio**: La aplicación se conecta a ambas bases de datos
2. **Setup CDC**: Habilita CDC en las tablas configuradas
3. **Snapshot Inicial**: Sincroniza todos los datos existentes (MERGE)
4. **Monitoreo CDC**: Cada N segundos verifica cambios en las tablas CDC
5. **Replicación**: Aplica INSERT/UPDATE/DELETE en la base destino

### Operaciones CDC

| Código | Operación |
|--------|-----------|
| 1 | DELETE |
| 2 | INSERT |
| 3 | UPDATE (valor anterior) |
| 4 | UPDATE (valor nuevo) |

---

## Resumen de Scripts Disponibles

Referencia rápida de todos los scripts npm disponibles en el proyecto:

### Scripts de Ejecución Principal

| Comando              | Descripción                              |
| -------------------- | ---------------------------------------- |
| `npm start`          | Inicia el servicio de sincronización CDC |
| `npm run dev`        | Inicia en modo desarrollo con nodemon    |
| `npm run setup-cdc`  | Configura CDC en las tablas del origen   |

### Scripts de Comparación y Validación

| Comando                  | Script                         | Descripción                                             |
| ------------------------ | ------------------------------ | ------------------------------------------------------- |
| `npm run compare-db`     | `compare-db-structures.js`     | Compara TODAS las estructuras entre pares de BD         |
| `npm run compare-shared` | `compare-shared-tables.js`     | Compara solo tablas compartidas (excluye empleados 2.0) |
| `npm run validate-data`  | `validate-data-consistency.js` | Valida consistencia de datos post-Full Load             |

### Scripts de Auditoría

| Comando                    | Script                      | Descripción                                 |
| -------------------------- | --------------------------- | ------------------------------------------- |
| `npm run create-auditoria` | `create-auditoria-table.js` | Crea la tabla dbo.AuditoriaReplicacion      |
| `npm run view-auditoria`   | `view-auditoria.js`         | Consulta y visualiza registros de auditoría |

### Opciones de view-auditoria

```bash
npm run view-auditoria                      # Resumen general
npm run view-auditoria -- --errores         # Solo errores
npm run view-auditoria -- --tabla Nomina    # Filtrar por tabla
npm run view-auditoria -- --ultimas 24h     # Últimas 24 horas
npm run view-auditoria -- --stats           # Estadísticas agregadas
npm run view-auditoria -- --export          # Exportar a JSON
npm run view-auditoria -- --limit 100       # Limitar resultados
```

### Ubicación de Reportes

Todos los reportes se generan en el directorio `reports/`:

```text
reports/
├── db-structure-comparison-[timestamp].xlsx    # compare-db
├── db-structure-comparison-[timestamp].json
├── shared-tables-comparison-[timestamp].xlsx   # compare-shared
├── shared-tables-comparison-[timestamp].json
├── data-validation-[timestamp].xlsx            # validate-data
├── data-validation-[timestamp].json
└── auditoria-[timestamp].json                  # view-auditoria --export
```

---

## Soporte

Para reportar problemas o solicitar mejoras, contactar al equipo de desarrollo.

---

*Última actualización: Enero 2026*
