/**
 * Script para consultar y visualizar registros de auditoría de replicación
 *
 * Uso:
 *   node scripts/view-auditoria.js                    # Muestra resumen
 *   node scripts/view-auditoria.js --errores          # Muestra solo errores
 *   node scripts/view-auditoria.js --tabla Empleados  # Filtra por tabla
 *   node scripts/view-auditoria.js --ultimas 24h      # Últimas 24 horas
 *   node scripts/view-auditoria.js --stats            # Estadísticas agregadas
 *   node scripts/view-auditoria.js --export           # Exporta a JSON
 */

require('dotenv').config();
const { Connection, Request } = require('tedious');
const fs = require('fs');
const path = require('path');

// Parsear argumentos
const args = process.argv.slice(2);
const options = {
  errores: args.includes('--errores') || args.includes('-e'),
  tabla: args.includes('--tabla') ? args[args.indexOf('--tabla') + 1] : null,
  ultimas: args.includes('--ultimas') ? args[args.indexOf('--ultimas') + 1] : null,
  stats: args.includes('--stats') || args.includes('-s'),
  export: args.includes('--export') || args.includes('-x'),
  limit: args.includes('--limit') ? parseInt(args[args.indexOf('--limit') + 1]) : 50,
  help: args.includes('--help') || args.includes('-h')
};

if (options.help) {
  console.log(`
╔══════════════════════════════════════════════════════════════════╗
║   Visor de Auditoría de Replicación CDC                         ║
╚══════════════════════════════════════════════════════════════════╝

Uso: node scripts/view-auditoria.js [opciones]

Opciones:
  --errores, -e        Mostrar solo registros con errores
  --tabla <nombre>     Filtrar por nombre de tabla
  --ultimas <tiempo>   Filtrar por tiempo (ej: 1h, 24h, 7d)
  --stats, -s          Mostrar estadísticas agregadas
  --export, -x         Exportar resultados a JSON
  --limit <n>          Límite de registros (default: 50)
  --help, -h           Mostrar esta ayuda

Ejemplos:
  node scripts/view-auditoria.js --errores --ultimas 1h
  node scripts/view-auditoria.js --tabla Empleados --limit 100
  node scripts/view-auditoria.js --stats --export
`);
  process.exit(0);
}

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

function parseTimeFilter(timeStr) {
  const match = timeStr.match(/^(\d+)(h|d|m)$/);
  if (!match) return null;

  const value = parseInt(match[1]);
  const unit = match[2];

  const now = new Date();
  switch (unit) {
    case 'h': return new Date(now.getTime() - value * 60 * 60 * 1000);
    case 'd': return new Date(now.getTime() - value * 24 * 60 * 60 * 1000);
    case 'm': return new Date(now.getTime() - value * 60 * 1000);
    default: return null;
  }
}

function formatDate(date) {
  if (!date) return '-';
  return new Date(date).toLocaleString('es-MX', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
}

function truncate(str, length) {
  if (!str) return '-';
  if (str.length <= length) return str;
  return str.substring(0, length - 3) + '...';
}

async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║   Visor de Auditoría de Replicación CDC                          ║');
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
        // Verificar si la tabla existe
        const tableCheck = await executeQuery(connection, `
          SELECT COUNT(*) as existe
          FROM INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'AuditoriaReplicacion'
        `);

        if (tableCheck[0].existe === 0) {
          console.log('\n⚠️  La tabla dbo.AuditoriaReplicacion no existe.');
          console.log('   Ejecute: node scripts/create-auditoria-table.js');
          connection.close();
          resolve();
          return;
        }

        // Construir condiciones de filtro
        const conditions = ['1=1'];

        if (options.errores) {
          conditions.push("Estado = 'ERROR'");
        }

        if (options.tabla) {
          conditions.push(`NombreTabla = '${options.tabla.replace(/'/g, "''")}'`);
        }

        if (options.ultimas) {
          const desde = parseTimeFilter(options.ultimas);
          if (desde) {
            conditions.push(`FechaHora >= '${desde.toISOString()}'`);
          }
        }

        const whereClause = conditions.join(' AND ');

        if (options.stats) {
          // ═══════════════════════════════════════════════════════════════════
          // MODO ESTADÍSTICAS
          // ═══════════════════════════════════════════════════════════════════
          console.log('\n═══ ESTADÍSTICAS DE AUDITORÍA ═══\n');

          // Resumen general
          const resumen = await executeQuery(connection, `
            SELECT
              COUNT(*) as TotalOperaciones,
              SUM(CASE WHEN Estado = 'SUCCESS' THEN 1 ELSE 0 END) as Exitosas,
              SUM(CASE WHEN Estado = 'ERROR' THEN 1 ELSE 0 END) as Errores,
              SUM(CASE WHEN Estado = 'SKIPPED' THEN 1 ELSE 0 END) as Omitidas,
              SUM(RegistrosProcesados) as TotalRegistros,
              AVG(TiempoEjecucionMs) as TiempoPromedioMs,
              MIN(FechaHora) as PrimeraOperacion,
              MAX(FechaHora) as UltimaOperacion
            FROM dbo.AuditoriaReplicacion
            WHERE ${whereClause}
          `);

          console.log('--- Resumen General ---');
          if (resumen[0]) {
            console.log(`  Total operaciones:    ${resumen[0].TotalOperaciones || 0}`);
            console.log(`  Exitosas:             ${resumen[0].Exitosas || 0}`);
            console.log(`  Errores:              ${resumen[0].Errores || 0}`);
            console.log(`  Omitidas:             ${resumen[0].Omitidas || 0}`);
            console.log(`  Total registros:      ${resumen[0].TotalRegistros || 0}`);
            console.log(`  Tiempo promedio:      ${Math.round(resumen[0].TiempoPromedioMs || 0)} ms`);
            console.log(`  Primera operación:    ${formatDate(resumen[0].PrimeraOperacion)}`);
            console.log(`  Última operación:     ${formatDate(resumen[0].UltimaOperacion)}`);
          }

          // Por tabla
          const porTabla = await executeQuery(connection, `
            SELECT TOP 20
              NombreTabla,
              COUNT(*) as Operaciones,
              SUM(CASE WHEN Estado = 'SUCCESS' THEN 1 ELSE 0 END) as OK,
              SUM(CASE WHEN Estado = 'ERROR' THEN 1 ELSE 0 END) as Err,
              SUM(RegistrosProcesados) as Registros,
              AVG(TiempoEjecucionMs) as TiempoMs
            FROM dbo.AuditoriaReplicacion
            WHERE ${whereClause}
            GROUP BY NombreTabla
            ORDER BY Operaciones DESC
          `);

          console.log('\n--- Top 20 Tablas por Operaciones ---');
          if (porTabla.length > 0) {
            console.table(porTabla.map(r => ({
              Tabla: r.NombreTabla,
              Operaciones: r.Operaciones,
              OK: r.OK,
              Errores: r.Err,
              Registros: r.Registros,
              'Tiempo Prom (ms)': Math.round(r.TiempoMs || 0)
            })));
          } else {
            console.log('  Sin datos');
          }

          // Por tipo de operación
          const porTipo = await executeQuery(connection, `
            SELECT
              TipoOperacion,
              COUNT(*) as Total,
              SUM(CASE WHEN Estado = 'SUCCESS' THEN 1 ELSE 0 END) as OK,
              SUM(CASE WHEN Estado = 'ERROR' THEN 1 ELSE 0 END) as Err
            FROM dbo.AuditoriaReplicacion
            WHERE ${whereClause}
            GROUP BY TipoOperacion
            ORDER BY Total DESC
          `);

          console.log('\n--- Por Tipo de Operación ---');
          if (porTipo.length > 0) {
            console.table(porTipo);
          } else {
            console.log('  Sin datos');
          }

          if (options.export) {
            const exportData = { resumen: resumen[0], porTabla, porTipo };
            const fileName = `auditoria-stats-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`;
            const filePath = path.join(__dirname, '..', 'reports', fileName);
            fs.writeFileSync(filePath, JSON.stringify(exportData, null, 2));
            console.log(`\n✓ Estadísticas exportadas a: ${filePath}`);
          }

        } else {
          // ═══════════════════════════════════════════════════════════════════
          // MODO LISTADO
          // ═══════════════════════════════════════════════════════════════════
          const registros = await executeQuery(connection, `
            SELECT TOP ${options.limit}
              AuditoriaId,
              FechaHora,
              NombreTabla,
              TipoOperacion,
              RegistroId,
              Estado,
              TiempoEjecucionMs,
              RegistrosProcesados,
              MensajeError
            FROM dbo.AuditoriaReplicacion
            WHERE ${whereClause}
            ORDER BY FechaHora DESC
          `);

          const titulo = options.errores
            ? '═══ ERRORES DE REPLICACIÓN ═══'
            : '═══ REGISTROS DE AUDITORÍA ═══';

          console.log(`\n${titulo}\n`);

          if (registros.length === 0) {
            console.log('No se encontraron registros con los filtros especificados.');
          } else {
            // Formato de tabla más legible
            console.table(registros.map(r => ({
              ID: r.AuditoriaId,
              Fecha: formatDate(r.FechaHora),
              Tabla: truncate(r.NombreTabla, 25),
              Operación: r.TipoOperacion,
              RegID: r.RegistroId || '-',
              Estado: r.Estado,
              'ms': r.TiempoEjecucionMs || 0,
              'Regs': r.RegistrosProcesados,
              Error: truncate(r.MensajeError, 40)
            })));

            console.log(`\nMostrando ${registros.length} de ${options.limit} registros máximo`);
          }

          if (options.export && registros.length > 0) {
            const fileName = `auditoria-${new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)}.json`;
            const filePath = path.join(__dirname, '..', 'reports', fileName);

            // Para el export, obtener datos completos
            const registrosCompletos = await executeQuery(connection, `
              SELECT TOP ${options.limit} *
              FROM dbo.AuditoriaReplicacion
              WHERE ${whereClause}
              ORDER BY FechaHora DESC
            `);

            fs.writeFileSync(filePath, JSON.stringify(registrosCompletos, null, 2));
            console.log(`\n✓ Registros exportados a: ${filePath}`);
          }

          // Si hay errores, mostrar los últimos
          if (!options.errores) {
            const erroresRecientes = await executeQuery(connection, `
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

            if (erroresRecientes.length > 0) {
              console.log('\n═══ ÚLTIMOS 5 ERRORES ═══\n');
              console.table(erroresRecientes.map(r => ({
                Fecha: formatDate(r.FechaHora),
                Tabla: r.NombreTabla,
                Operación: r.TipoOperacion,
                RegID: r.RegistroId || '-',
                Error: truncate(r.MensajeError, 50)
              })));
            }
          }
        }

        connection.close();
        console.log('\n✓ Consulta completada');
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
main().catch(console.error);
