const sql = require('mssql');

const config = {
  server: '172.17.0.247',
  database: 'TadaNomina',
  user: 'haestr4d4',
  password: 'HCq$9ynmF@V!%04P0u6#',
  options: {
    encrypt: true,
    trustServerCertificate: true
  }
};

async function createLogTable() {
  try {
    await sql.connect(config);
    console.log('Conectado a la base de datos origen');

    // Verificar si la tabla ya existe
    const checkTable = await sql.query`
      SELECT COUNT(*) as existe
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = 'dbo'
      AND TABLE_NAME = 'CDC_SyncLog'
    `;

    if (checkTable.recordset[0].existe > 0) {
      console.log('✓ Tabla CDC_SyncLog ya existe');
    } else {
      console.log('Creando tabla CDC_SyncLog...');

      await sql.query`
        CREATE TABLE dbo.CDC_SyncLog (
          LogId INT IDENTITY(1,1) PRIMARY KEY,
          FechaHora DATETIME2 DEFAULT GETDATE(),
          TablaNombre NVARCHAR(128) NOT NULL,
          TipoOperacion VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE, MERGE
          RegistroId NVARCHAR(50), -- Clave primaria del registro afectado
          Estado VARCHAR(20) NOT NULL, -- SUCCESS, ERROR, SKIPPED
          Mensaje NVARCHAR(MAX),
          DatosAntes NVARCHAR(MAX), -- JSON con datos anteriores (para UPDATE)
          DatosDespues NVARCHAR(MAX), -- JSON con datos nuevos
          TiempoEjecucionMs INT, -- Tiempo de ejecución en milisegundos
          ProcesoId NVARCHAR(50) -- ID del proceso que ejecutó el cambio
        )
      `;

      console.log('✓ Tabla CDC_SyncLog creada exitosamente');

      // Crear índice para consultas rápidas
      await sql.query`
        CREATE INDEX IX_CDC_SyncLog_FechaHora
        ON dbo.CDC_SyncLog(FechaHora DESC)
      `;

      await sql.query`
        CREATE INDEX IX_CDC_SyncLog_TablaNombre_RegistroId
        ON dbo.CDC_SyncLog(TablaNombre, RegistroId)
      `;

      console.log('✓ Índices creados exitosamente');
    }

    // Mostrar estadísticas si ya tiene datos
    const stats = await sql.query`
      SELECT
        TablaNombre,
        TipoOperacion,
        Estado,
        COUNT(*) as Total
      FROM dbo.CDC_SyncLog
      GROUP BY TablaNombre, TipoOperacion, Estado
      ORDER BY TablaNombre, TipoOperacion, Estado
    `;

    if (stats.recordset.length > 0) {
      console.log('\n=== Estadísticas de CDC_SyncLog ===');
      console.table(stats.recordset);
    } else {
      console.log('\nTabla CDC_SyncLog está vacía (recién creada o sin registros)');
    }

    await sql.close();
    console.log('\n✓ Proceso completado');
  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
}

createLogTable();
