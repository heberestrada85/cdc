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

async function viewSyncLog() {
  try {
    await sql.connect(config);
    console.log('Conectado a la base de datos origen\n');

    // Estadísticas generales
    console.log('=== ESTADÍSTICAS GENERALES ===');
    const stats = await sql.query`
      SELECT
        TablaNombre,
        TipoOperacion,
        Estado,
        COUNT(*) as Total,
        AVG(TiempoEjecucionMs) as TiempoPromedio,
        MIN(FechaHora) as PrimeraOperacion,
        MAX(FechaHora) as UltimaOperacion
      FROM dbo.CDC_SyncLog
      GROUP BY TablaNombre, TipoOperacion, Estado
      ORDER BY TablaNombre, TipoOperacion, Estado
    `;
    console.table(stats.recordset);

    // Últimas 50 operaciones
    console.log('\n=== ÚLTIMAS 50 OPERACIONES ===');
    const recent = await sql.query`
      SELECT TOP 50
        CONVERT(VARCHAR(23), FechaHora, 121) as Fecha,
        TablaNombre,
        TipoOperacion,
        RegistroId,
        Estado,
        LEFT(Mensaje, 60) as Mensaje,
        TiempoEjecucionMs as TiempoMs
      FROM dbo.CDC_SyncLog
      ORDER BY FechaHora DESC
    `;
    console.table(recent.recordset);

    // Errores recientes
    console.log('\n=== ERRORES RECIENTES ===');
    const errors = await sql.query`
      SELECT TOP 20
        CONVERT(VARCHAR(23), FechaHora, 121) as Fecha,
        TablaNombre,
        TipoOperacion,
        RegistroId,
        Mensaje
      FROM dbo.CDC_SyncLog
      WHERE Estado = 'ERROR'
      ORDER BY FechaHora DESC
    `;

    if (errors.recordset.length > 0) {
      console.table(errors.recordset);
    } else {
      console.log('✓ No hay errores recientes');
    }

    // Operaciones por proceso
    console.log('\n=== OPERACIONES POR PROCESO ===');
    const byProcess = await sql.query`
      SELECT
        ProcesoId,
        COUNT(*) as TotalOperaciones,
        MIN(FechaHora) as Inicio,
        MAX(FechaHora) as Fin,
        DATEDIFF(SECOND, MIN(FechaHora), MAX(FechaHora)) as DuracionSegundos
      FROM dbo.CDC_SyncLog
      GROUP BY ProcesoId
      ORDER BY MIN(FechaHora) DESC
    `;
    console.table(byProcess.recordset);

    // Registros con múltiples operaciones (posibles duplicados)
    console.log('\n=== REGISTROS PROCESADOS MÚLTIPLES VECES (Posibles duplicados) ===');
    const duplicates = await sql.query`
      SELECT TOP 20
        TablaNombre,
        RegistroId,
        COUNT(*) as VecesProcesado,
        STRING_AGG(TipoOperacion + ':' + Estado, ', ') as Operaciones
      FROM dbo.CDC_SyncLog
      GROUP BY TablaNombre, RegistroId
      HAVING COUNT(*) > 1
      ORDER BY COUNT(*) DESC
    `;

    if (duplicates.recordset.length > 0) {
      console.table(duplicates.recordset);
      console.log(`\n⚠️  Encontrados ${duplicates.recordset.length} registros procesados múltiples veces`);
    } else {
      console.log('✓ No hay registros procesados múltiples veces');
    }

    await sql.close();
  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
}

viewSyncLog();
