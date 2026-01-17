# Utils Scripts - CDC TADÁ

Carpeta de utilidades para reporteo, análisis y mantenimiento de las bases de datos de sincronización.

## Scripts Disponibles

### 1. compare-db-structures.js

**Propósito:** Comparar estructuras de bases de datos SQL Server entre ambientes y generar reportes de diferencias.

**Uso:**
```bash
node utils_scripts/compare-db-structures.js
```

**Pares de bases comparados:**
- TadaNomina vs TadaNomina-2.0
- TadaModuloSeguridad vs TadaModuloSeguridad-2.0
- TadaChecador vs TadaChecador-2.0

**Salidas:**
- `reports/db-comparison-YYYY-MM-DD_HH-mm-ss.xlsx` - Reporte Excel
- `reports/db-comparison-YYYY-MM-DD_HH-mm-ss.json` - Datos crudos en JSON

**Tipos de diferencias detectadas:**
- `TABLE_MISSING_IN_TARGET/SOURCE` - Tablas faltantes
- `COLUMN_MISSING_IN_TARGET/SOURCE` - Columnas faltantes
- `COLUMN_TYPE_MISMATCH` - Tipo de dato diferente
- `COLUMN_LENGTH_MISMATCH` - Longitud diferente
- `COLUMN_NULLABLE_MISMATCH` - Nullable diferente
- `COLUMN_IDENTITY_MISMATCH` - Identity diferente
- `INDEX_MISSING_*` - Índices faltantes
- `FK_MISSING_*` - Foreign Keys faltantes

---

### 2. 2-generate-sync-scripts.js

**Propósito:** Generar scripts SQL ejecutables para sincronizar estructuras entre bases de datos. Solo genera cambios estructurales (DDL), NO migración de datos.

**Uso:**
```bash
node utils_scripts/2-generate-sync-scripts.js
```

**Salidas:**
- `scripts/sync/sync-[DB_NAME]-YYYY-MM-DD_HH-mm-ss.sql` - Scripts SQL ejecutables
- `reports/sync-changes-YYYY-MM-DD_HH-mm-ss.json` - Log de cambios
- `reports/sync-verification-YYYY-MM-DD_HH-mm-ss.xlsx` - Reporte de verificación

**Cambios PERMITIDOS (se generan scripts):**
- `CREATE_TABLE` - Crear tablas vacías
- `ADD_COLUMN` - Agregar columnas NULL o con DEFAULT
- `MODIFY_LENGTH` - Aumentar tamaño de columnas
- `CREATE_INDEX` - Crear índices

**Cambios BLOQUEADOS (requieren migración de datos):**
- `CREATE_TABLE` con registros existentes
- `ADD_COLUMN NOT NULL` sin DEFAULT
- `MODIFY_LENGTH` para reducir tamaño
- `DROP_COLUMN` / `DROP_TABLE`

**Características del script generado:**
- Encabezado con información del cambio
- Agrupado por tipo de cambio
- Cada cambio separado con `GO`
- Comentarios descriptivos
- Listo para ejecutar en SQL Server Management Studio

---

## Flujo de Trabajo Recomendado

1. **Analizar diferencias:**
   ```bash
   node utils_scripts/compare-db-structures.js
   ```

2. **Generar scripts de sincronización:**
   ```bash
   node utils_scripts/2-generate-sync-scripts.js
   ```

3. **Revisar cambios bloqueados** en el reporte Excel

4. **Ejecutar scripts** en ambiente QA primero

5. **Verificar** ejecutando nuevamente el comparador

---

## Tablas Excluidas

Se excluyen automáticamente:
- Estructura nueva de personas TADÁ 2.0: `Personas`, `PersonasDirecciones`, etc.
- Tablas CDC: `CDC_SyncLog`, `cdc_*`
- Tablas temporales: `tmp_*`, `bak_*`
- Tablas de sistema: `sysdiagrams`, `systranschemas`

## Configuración

Los scripts usan las credenciales del archivo `.env` principal.

## Reportes

Los reportes se guardan en:
- `reports/` - Excel y JSON
- `scripts/sync/` - Scripts SQL ejecutables
