/**
 * Script para crear la tabla dbo.AuditoriaReplicacion
 *
 * Esta tabla registra cada operación de replicación para facilitar
 * la depuración y el monitoreo del proceso CDC.
 *
 * Uso: node scripts/create-auditoria-table.js
 */

require('dotenv').config();
const { Connection, Request } = require('tedious');

const config = {
  server: process.env.SOURCE_DB_SERVER || '172.17.0.247',
  authentication: {
    type: 'default',
    options: {
      userName: process.env.SOURCE_DB_USER || 'haestr4d4',
      password: process.env.SOURCE_DB_PASSWORD || ''
    }
  },
  options: {
    database: process.env.SOURCE_DB_NAME || 'TadaNomina',
    encrypt: true,
    trustServerCertificate: true,
    connectTimeout: 30000,
    requestTimeout: 60000
  }
};

function executeQuery(connection, sql) {
  return new Promise((resolve, reject) => {
    const results = [];
    const request = new Request(sql, (err) => {
      if (err) reject(err);
      else resolve(results);
    });

    request.on('row', (columns) => {
      const row = {};
      columns.forEach((column) => {
        row[column.metadata.colName] = column.value;
      });
      results.push(row);
    });

    connection.execSql(request);
  });
}

async function createAuditoriaTable() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║   CREACIÓN DE TABLA dbo.AuditoriaReplicacion                     ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');

  const connection = new Connection(config);

  return new Promise((resolve, reject) => {
    connection.on('connect', async (err) => {
      if (err) {
        console.error('❌ Error conectando:', err.message);
        reject(err);
        return;
      }

      console.log(`✓ Conectado a ${config.options.database}`);

      try {
        // Verificar si la tabla ya existe
        console.log('\nVerificando si la tabla existe...');
        const checkTable = await executeQuery(connection, `
          SELECT COUNT(*) as existe
          FROM INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA = 'dbo'
          AND TABLE_NAME = 'AuditoriaReplicacion'
        `);

        if (checkTable[0].existe > 0) {
          console.log('✓ Tabla dbo.AuditoriaReplicacion ya existe');

          // Mostrar estructura actual
          const columns = await executeQuery(connection, `
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'AuditoriaReplicacion'
            ORDER BY ORDINAL_POSITION
          `);

          console.log('\nEstructura actual:');
          console.table(columns);
        } else {
          console.log('Creando tabla dbo.AuditoriaReplicacion...');

          await executeQuery(connection, `
            CREATE TABLE dbo.AuditoriaReplicacion (
              -- Identificador único del registro de auditoría
              AuditoriaId BIGINT IDENTITY(1,1) PRIMARY KEY,

              -- Timestamp de cuando ocurrió la operación
              FechaHora DATETIME2(3) DEFAULT SYSDATETIME(),

              -- Identificación del proceso
              ProcesoId NVARCHAR(100) NOT NULL,           -- ID único del proceso (PID-xxx-timestamp)
              NombreServidor NVARCHAR(128) DEFAULT HOST_NAME(),

              -- Información de la operación
              BaseDatosOrigen NVARCHAR(128) NOT NULL,     -- Base de datos origen
              BaseDatosDestino NVARCHAR(128) NOT NULL,    -- Base de datos destino
              EsquemaTabla NVARCHAR(128) NOT NULL,        -- Schema de la tabla (ej: dbo)
              NombreTabla NVARCHAR(128) NOT NULL,         -- Nombre de la tabla

              -- Tipo de operación CDC
              TipoOperacion VARCHAR(20) NOT NULL,         -- INSERT, UPDATE, DELETE, MERGE, MERGE_ALL, BULK_INSERT
              CodigoOperacionCDC TINYINT NULL,            -- 1=DELETE, 2=INSERT, 3=UPDATE_BEFORE, 4=UPDATE_AFTER

              -- Identificación del registro afectado
              ClavesPrimarias NVARCHAR(500) NULL,         -- Valores de las PKs (JSON o CSV)
              RegistroId NVARCHAR(100) NULL,              -- ID principal del registro

              -- Estado de la operación
              Estado VARCHAR(20) NOT NULL,                -- SUCCESS, ERROR, SKIPPED, RETRY
              CodigoError INT NULL,                       -- Código de error SQL si aplica
              MensajeError NVARCHAR(MAX) NULL,            -- Mensaje de error detallado

              -- Datos del cambio (para debugging)
              DatosAntes NVARCHAR(MAX) NULL,              -- JSON con datos anteriores (UPDATE/DELETE)
              DatosDespues NVARCHAR(MAX) NULL,            -- JSON con datos nuevos (INSERT/UPDATE)

              -- Métricas de rendimiento
              TiempoEjecucionMs INT NULL,                 -- Tiempo de ejecución en milisegundos
              RegistrosProcesados INT DEFAULT 1,          -- Número de registros en operación batch
              BytesProcesados BIGINT NULL,                -- Tamaño aproximado de datos

              -- LSN para tracking CDC
              LSN_Inicio VARBINARY(10) NULL,              -- LSN de inicio del cambio
              LSN_Fin VARBINARY(10) NULL,                 -- LSN de fin del cambio

              -- Metadata adicional
              VersionApp NVARCHAR(50) NULL,               -- Versión de la aplicación
              Metadata NVARCHAR(MAX) NULL                 -- JSON con información adicional
            )
          `);

          console.log('✓ Tabla dbo.AuditoriaReplicacion creada exitosamente');

          // Crear índices para consultas eficientes
          console.log('\nCreando índices...');

          await executeQuery(connection, `
            CREATE NONCLUSTERED INDEX IX_AuditoriaReplicacion_FechaHora
            ON dbo.AuditoriaReplicacion(FechaHora DESC)
            INCLUDE (NombreTabla, TipoOperacion, Estado)
          `);
          console.log('  ✓ Índice IX_AuditoriaReplicacion_FechaHora creado');

          await executeQuery(connection, `
            CREATE NONCLUSTERED INDEX IX_AuditoriaReplicacion_Tabla
            ON dbo.AuditoriaReplicacion(EsquemaTabla, NombreTabla, FechaHora DESC)
          `);
          console.log('  ✓ Índice IX_AuditoriaReplicacion_Tabla creado');

          await executeQuery(connection, `
            CREATE NONCLUSTERED INDEX IX_AuditoriaReplicacion_Estado
            ON dbo.AuditoriaReplicacion(Estado, FechaHora DESC)
            WHERE Estado = 'ERROR'
          `);
          console.log('  ✓ Índice IX_AuditoriaReplicacion_Estado creado (filtrado para errores)');

          await executeQuery(connection, `
            CREATE NONCLUSTERED INDEX IX_AuditoriaReplicacion_ProcesoId
            ON dbo.AuditoriaReplicacion(ProcesoId, FechaHora DESC)
          `);
          console.log('  ✓ Índice IX_AuditoriaReplicacion_ProcesoId creado');

          await executeQuery(connection, `
            CREATE NONCLUSTERED INDEX IX_AuditoriaReplicacion_RegistroId
            ON dbo.AuditoriaReplicacion(NombreTabla, RegistroId)
            WHERE RegistroId IS NOT NULL
          `);
          console.log('  ✓ Índice IX_AuditoriaReplicacion_RegistroId creado');

          console.log('\n✓ Todos los índices creados exitosamente');
        }

        // Mostrar estadísticas si ya tiene datos
        console.log('\n--- Estadísticas de AuditoriaReplicacion ---');

        const stats = await executeQuery(connection, `
          SELECT
            NombreTabla,
            TipoOperacion,
            Estado,
            COUNT(*) as Total,
            AVG(TiempoEjecucionMs) as TiempoPromedioMs,
            SUM(RegistrosProcesados) as TotalRegistros
          FROM dbo.AuditoriaReplicacion
          GROUP BY NombreTabla, TipoOperacion, Estado
          ORDER BY Total DESC
        `);

        if (stats.length > 0) {
          console.table(stats);
        } else {
          console.log('La tabla está vacía (sin registros de auditoría aún)');
        }

        // Mostrar últimos errores si hay
        const errores = await executeQuery(connection, `
          SELECT TOP 5
            FechaHora,
            NombreTabla,
            TipoOperacion,
            RegistroId,
            MensajeError
          FROM dbo.AuditoriaReplicacion
          WHERE Estado = 'ERROR'
          ORDER BY FechaHora DESC
        `);

        if (errores.length > 0) {
          console.log('\n--- Últimos 5 errores ---');
          console.table(errores);
        }

        connection.close();
        console.log('\n✓ Proceso completado exitosamente');
        resolve();
      } catch (error) {
        console.error('❌ Error:', error.message);
        connection.close();
        reject(error);
      }
    });

    connection.connect();
  });
}

// Ejecutar
createAuditoriaTable().catch(console.error);
