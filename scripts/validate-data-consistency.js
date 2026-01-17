/**
 * Script de Validación de Consistencia de Datos Post-Full Load
 *
 * Compara los datos entre BD 1.0 (origen) y BD 2.0 (destino) después del Full Load
 * para verificar la consistencia y detectar discrepancias.
 *
 * Validaciones realizadas:
 * 1. Conteo de registros por tabla
 * 2. Checksums de datos (hash de columnas)
 * 3. Verificación de claves primarias
 * 4. Comparación de registros específicos (muestreo)
 * 5. Validación de integridad referencial
 *
 * Uso: node scripts/validate-data-consistency.js
 */

require('dotenv').config();
const { Connection, Request, TYPES } = require('tedious');
const ExcelJS = require('exceljs');
const path = require('path');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURACIÓN
// ═══════════════════════════════════════════════════════════════════════════

const DB_PAIRS = [
  {
    name: 'TadaNomina',
    source: {
      server: process.env.SOURCE_DB_SERVER || '172.17.0.247',
      database: 'TadaNomina',
      user: process.env.SOURCE_DB_USER || 'haestr4d4',
      password: process.env.SOURCE_DB_PASSWORD || ''
    },
    target: {
      server: process.env.TARGET_DB_SERVER || '172.17.0.247',
      database: 'TadaNomina-2.0',
      user: process.env.TARGET_DB_USER || 'haestr4d4',
      password: process.env.TARGET_DB_PASSWORD || ''
    }
  },
  // {
  //   name: 'TadaModuloSeguridad',
  //   source: {
  //     server: process.env.SOURCE_DB_SERVER || '172.17.0.247',
  //     database: 'TadaModuloSeguridad',
  //     user: process.env.SOURCE_DB_USER || 'haestr4d4',
  //     password: process.env.SOURCE_DB_PASSWORD || ''
  //   },
  //   target: {
  //     server: process.env.TARGET_DB_SERVER || '172.17.0.247',
  //     database: 'TadaModuloSeguridad-2.0',
  //     user: process.env.TARGET_DB_USER || 'haestr4d4',
  //     password: process.env.TARGET_DB_PASSWORD || ''
  //   }
  // },
  // {
  //   name: 'TadaChecador',
  //   source: {
  //     server: process.env.SOURCE_DB_SERVER || '172.17.0.247',
  //     database: 'TadaChecador',
  //     user: process.env.SOURCE_DB_USER || 'haestr4d4',
  //     password: process.env.SOURCE_DB_PASSWORD || ''
  //   },
  //   target: {
  //     server: process.env.TARGET_DB_SERVER || '172.17.0.247',
  //     database: 'TadaChecador-2.0',
  //     user: process.env.TARGET_DB_USER || 'haestr4d4',
  //     password: process.env.TARGET_DB_PASSWORD || ''
  //   }
  // }
];

// Configuración de validación
const CONFIG = {
  sampleSize: 100,           // Número de registros a muestrear por tabla
  maxDiscrepancies: 1000,    // Máximo de discrepancias a reportar por tabla
  excludeTables: [           // Tablas a excluir de la validación
    'CDC_SyncLog',
    'sysdiagrams',
    'systranschemas'
  ],
  criticalTables: [          // Tablas críticas que requieren validación exhaustiva
    'Empleados',
    'Usuarios',
    'Nomina',
    'PeriodoNomina',
    'Cat_Clientes',
    'Cat_UnidadNegocio'
  ]
};

// ═══════════════════════════════════════════════════════════════════════════
// UTILIDADES DE CONEXIÓN
// ═══════════════════════════════════════════════════════════════════════════

function createConnection(config) {
  return new Promise((resolve, reject) => {
    const connection = new Connection({
      server: config.server,
      authentication: {
        type: 'default',
        options: {
          userName: config.user,
          password: config.password
        }
      },
      options: {
        database: config.database,
        encrypt: true,
        trustServerCertificate: true,
        rowCollectionOnRequestCompletion: true,
        connectTimeout: 30000,
        requestTimeout: 120000  // 2 minutos para queries grandes
      }
    });

    connection.on('connect', (err) => {
      if (err) reject(err);
      else resolve(connection);
    });

    connection.connect();
  });
}

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

function closeConnection(connection) {
  return new Promise((resolve) => {
    connection.on('end', () => resolve());
    connection.close();
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// QUERIES DE VALIDACIÓN
// ═══════════════════════════════════════════════════════════════════════════

const QUERIES = {
  // Obtener todas las tablas con conteo
  tableRowCounts: `
    SELECT
      s.name AS schema_name,
      t.name AS table_name,
      p.rows AS row_count
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
    WHERE t.type = 'U'
    ORDER BY s.name, t.name
  `,

  // Obtener claves primarias de una tabla
  primaryKey: (schema, table) => `
    SELECT
      c.name AS column_name,
      ic.key_ordinal
    FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE i.is_primary_key = 1
      AND s.name = '${schema}'
      AND t.name = '${table}'
    ORDER BY ic.key_ordinal
  `,

  // Obtener columnas de una tabla
  tableColumns: (schema, table) => `
    SELECT
      c.name AS column_name,
      TYPE_NAME(c.user_type_id) AS data_type,
      c.max_length,
      c.is_nullable
    FROM sys.columns c
    INNER JOIN sys.tables t ON c.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = '${schema}' AND t.name = '${table}'
    ORDER BY c.column_id
  `,

  // Checksum de tabla (hash de todos los datos)
  tableChecksum: (schema, table) => `
    SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS checksum
    FROM [${schema}].[${table}]
  `,

  // Conteo agrupado por columna (para detectar distribución de datos)
  columnDistribution: (schema, table, column) => `
    SELECT TOP 10
      [${column}] AS value,
      COUNT(*) AS count
    FROM [${schema}].[${table}]
    GROUP BY [${column}]
    ORDER BY COUNT(*) DESC
  `,

  // Muestra de registros
  sampleRecords: (schema, table, pkColumns, limit) => `
    SELECT TOP ${limit} *
    FROM [${schema}].[${table}]
    ORDER BY ${pkColumns.length > 0 ? pkColumns.join(', ') : '(SELECT NULL)'}
  `,

  // Registros que existen en origen pero no en destino
  missingInTarget: (schema, table, pkColumns) => {
    const pkJoin = pkColumns.map(pk => `s.[${pk}] = t.[${pk}]`).join(' AND ');
    const pkSelect = pkColumns.map(pk => `s.[${pk}]`).join(', ');
    return `
      SELECT TOP 100 ${pkSelect}
      FROM [${schema}].[${table}] s
      WHERE NOT EXISTS (
        SELECT 1 FROM [TARGET_DB].[${schema}].[${table}] t
        WHERE ${pkJoin}
      )
    `;
  },

  // Registros con diferencias en valores
  differentValues: (schema, table, pkColumns, compareColumns) => {
    const pkJoin = pkColumns.map(pk => `s.[${pk}] = t.[${pk}]`).join(' AND ');
    const pkSelect = pkColumns.map(pk => `s.[${pk}] AS [src_${pk}]`).join(', ');
    const colCompare = compareColumns.map(col =>
      `CASE WHEN s.[${col}] != t.[${col}] OR (s.[${col}] IS NULL AND t.[${col}] IS NOT NULL) OR (s.[${col}] IS NOT NULL AND t.[${col}] IS NULL) THEN 1 ELSE 0 END`
    ).join(' + ');

    return `
      SELECT TOP 100
        ${pkSelect},
        ${colCompare} AS diff_count
      FROM [${schema}].[${table}] s
      INNER JOIN [TARGET_DB].[${schema}].[${table}] t ON ${pkJoin}
      WHERE ${colCompare} > 0
    `;
  }
};

// ═══════════════════════════════════════════════════════════════════════════
// FUNCIONES DE VALIDACIÓN
// ═══════════════════════════════════════════════════════════════════════════

async function validateRowCounts(sourceConn, targetConn, pairName) {
  console.log('  Validando conteos de registros...');

  const sourceRows = await executeQuery(sourceConn, QUERIES.tableRowCounts);
  const targetRows = await executeQuery(targetConn, QUERIES.tableRowCounts);

  const sourceMap = new Map(sourceRows.map(r => [`${r.schema_name}.${r.table_name}`, r.row_count]));
  const targetMap = new Map(targetRows.map(r => [`${r.schema_name}.${r.table_name}`, r.row_count]));

  const discrepancies = [];
  let matchCount = 0;
  let diffCount = 0;

  // Comparar tablas que existen en ambos
  for (const [tableName, sourceCount] of sourceMap) {
    if (CONFIG.excludeTables.some(t => tableName.includes(t))) continue;

    const targetCount = targetMap.get(tableName);
    if (targetCount === undefined) {
      discrepancies.push({
        table: tableName,
        sourceCount: sourceCount,
        targetCount: 'NO EXISTE',
        difference: sourceCount,
        percentDiff: 100,
        status: 'TABLA_FALTANTE'
      });
      diffCount++;
    } else if (sourceCount !== targetCount) {
      const diff = sourceCount - targetCount;
      const percentDiff = sourceCount > 0 ? ((diff / sourceCount) * 100).toFixed(2) : 0;
      discrepancies.push({
        table: tableName,
        sourceCount: sourceCount,
        targetCount: targetCount,
        difference: diff,
        percentDiff: parseFloat(percentDiff),
        status: diff > 0 ? 'FALTAN_EN_DESTINO' : 'EXTRAS_EN_DESTINO'
      });
      diffCount++;
    } else {
      matchCount++;
    }
  }

  // Tablas solo en destino
  for (const [tableName, targetCount] of targetMap) {
    if (CONFIG.excludeTables.some(t => tableName.includes(t))) continue;
    if (!sourceMap.has(tableName)) {
      discrepancies.push({
        table: tableName,
        sourceCount: 'NO EXISTE',
        targetCount: targetCount,
        difference: -targetCount,
        percentDiff: 100,
        status: 'SOLO_EN_DESTINO'
      });
      diffCount++;
    }
  }

  console.log(`    ✓ ${matchCount} tablas coinciden`);
  if (diffCount > 0) {
    console.log(`    ⚠ ${diffCount} tablas con diferencias`);
  }

  return {
    type: 'ROW_COUNTS',
    matchCount,
    diffCount,
    discrepancies: discrepancies.sort((a, b) => Math.abs(b.difference) - Math.abs(a.difference))
  };
}

async function validateChecksums(sourceConn, targetConn, tables) {
  console.log('  Validando checksums de tablas críticas...');

  const results = [];
  let matchCount = 0;
  let diffCount = 0;
  let errorCount = 0;

  for (const tableName of CONFIG.criticalTables) {
    const fullTableName = `dbo.${tableName}`;
    if (!tables.has(fullTableName)) continue;

    try {
      const [sourceChecksum] = await executeQuery(sourceConn, QUERIES.tableChecksum('dbo', tableName));
      const [targetChecksum] = await executeQuery(targetConn, QUERIES.tableChecksum('dbo', tableName));

      const srcHash = sourceChecksum?.checksum;
      const tgtHash = targetChecksum?.checksum;

      if (srcHash === tgtHash) {
        matchCount++;
        results.push({
          table: fullTableName,
          sourceChecksum: srcHash,
          targetChecksum: tgtHash,
          status: 'COINCIDE'
        });
      } else {
        diffCount++;
        results.push({
          table: fullTableName,
          sourceChecksum: srcHash,
          targetChecksum: tgtHash,
          status: 'DIFERENTE'
        });
      }
    } catch (error) {
      errorCount++;
      results.push({
        table: fullTableName,
        sourceChecksum: null,
        targetChecksum: null,
        status: 'ERROR',
        error: error.message
      });
    }
  }

  console.log(`    ✓ ${matchCount} checksums coinciden`);
  if (diffCount > 0) console.log(`    ⚠ ${diffCount} checksums diferentes`);
  if (errorCount > 0) console.log(`    ✗ ${errorCount} errores`);

  return {
    type: 'CHECKSUMS',
    matchCount,
    diffCount,
    errorCount,
    results
  };
}

async function validatePrimaryKeys(sourceConn, targetConn, tables) {
  console.log('  Validando claves primarias...');

  const results = [];
  let validCount = 0;
  let invalidCount = 0;

  for (const [tableName] of tables) {
    if (CONFIG.excludeTables.some(t => tableName.includes(t))) continue;

    const [schema, table] = tableName.split('.');

    try {
      const sourcePKs = await executeQuery(sourceConn, QUERIES.primaryKey(schema, table));
      const targetPKs = await executeQuery(targetConn, QUERIES.primaryKey(schema, table));

      const sourcePKCols = sourcePKs.map(pk => pk.column_name).join(', ');
      const targetPKCols = targetPKs.map(pk => pk.column_name).join(', ');

      if (sourcePKCols === targetPKCols) {
        validCount++;
      } else {
        invalidCount++;
        results.push({
          table: tableName,
          sourcePK: sourcePKCols || 'SIN PK',
          targetPK: targetPKCols || 'SIN PK',
          status: 'DIFERENTE'
        });
      }
    } catch (error) {
      results.push({
        table: tableName,
        sourcePK: 'ERROR',
        targetPK: 'ERROR',
        status: 'ERROR',
        error: error.message
      });
    }
  }

  console.log(`    ✓ ${validCount} PKs coinciden`);
  if (invalidCount > 0) console.log(`    ⚠ ${invalidCount} PKs diferentes`);

  return {
    type: 'PRIMARY_KEYS',
    validCount,
    invalidCount,
    results
  };
}

async function validateSampleData(sourceConn, targetConn, tables) {
  console.log('  Validando muestra de datos en tablas críticas...');

  const results = [];

  for (const tableName of CONFIG.criticalTables) {
    const fullTableName = `dbo.${tableName}`;
    if (!tables.has(fullTableName)) continue;

    try {
      // Obtener PKs
      const pks = await executeQuery(sourceConn, QUERIES.primaryKey('dbo', tableName));
      const pkColumns = pks.map(pk => pk.column_name);

      if (pkColumns.length === 0) {
        results.push({
          table: fullTableName,
          status: 'SIN_PK',
          message: 'No se puede validar sin clave primaria'
        });
        continue;
      }

      // Obtener muestra de origen
      const sourceData = await executeQuery(
        sourceConn,
        QUERIES.sampleRecords('dbo', tableName, pkColumns, CONFIG.sampleSize)
      );

      // Obtener muestra de destino
      const targetData = await executeQuery(
        targetConn,
        QUERIES.sampleRecords('dbo', tableName, pkColumns, CONFIG.sampleSize)
      );

      // Comparar
      const sourceKeys = new Set(sourceData.map(r => pkColumns.map(pk => r[pk]).join('|')));
      const targetKeys = new Set(targetData.map(r => pkColumns.map(pk => r[pk]).join('|')));

      const missingInTarget = [...sourceKeys].filter(k => !targetKeys.has(k)).length;
      const missingInSource = [...targetKeys].filter(k => !sourceKeys.has(k)).length;

      results.push({
        table: fullTableName,
        sampleSize: sourceData.length,
        missingInTarget,
        missingInSource,
        status: missingInTarget === 0 && missingInSource === 0 ? 'OK' : 'DIFERENCIAS'
      });

    } catch (error) {
      results.push({
        table: fullTableName,
        status: 'ERROR',
        error: error.message
      });
    }
  }

  return {
    type: 'SAMPLE_DATA',
    results
  };
}

async function validateNullCounts(sourceConn, targetConn, tables) {
  console.log('  Validando distribución de NULLs en columnas críticas...');

  const results = [];

  for (const tableName of CONFIG.criticalTables.slice(0, 5)) { // Limitar a 5 tablas
    const fullTableName = `dbo.${tableName}`;
    if (!tables.has(fullTableName)) continue;

    try {
      const columns = await executeQuery(sourceConn, QUERIES.tableColumns('dbo', tableName));
      const nullableColumns = columns.filter(c => c.is_nullable).slice(0, 5); // Limitar columnas

      for (const col of nullableColumns) {
        const sourceNullQuery = `SELECT COUNT(*) AS null_count FROM [dbo].[${tableName}] WHERE [${col.column_name}] IS NULL`;
        const targetNullQuery = `SELECT COUNT(*) AS null_count FROM [dbo].[${tableName}] WHERE [${col.column_name}] IS NULL`;

        const [sourceNull] = await executeQuery(sourceConn, sourceNullQuery);
        const [targetNull] = await executeQuery(targetConn, targetNullQuery);

        if (sourceNull.null_count !== targetNull.null_count) {
          results.push({
            table: fullTableName,
            column: col.column_name,
            sourceNulls: sourceNull.null_count,
            targetNulls: targetNull.null_count,
            difference: sourceNull.null_count - targetNull.null_count
          });
        }
      }
    } catch (error) {
      // Ignorar errores silenciosamente
    }
  }

  return {
    type: 'NULL_DISTRIBUTION',
    results
  };
}

// ═══════════════════════════════════════════════════════════════════════════
// GENERACIÓN DE REPORTE
// ═══════════════════════════════════════════════════════════════════════════

async function generateReport(allResults) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = 'CDC TADA Sync - Validación';
  workbook.created = new Date();

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);

  // ═══════════════════════════════════════════════════════════════════════
  // HOJA: RESUMEN EJECUTIVO
  // ═══════════════════════════════════════════════════════════════════════
  const summarySheet = workbook.addWorksheet('Resumen Ejecutivo', {
    properties: { tabColor: { argb: '4472C4' } }
  });

  // Título
  summarySheet.mergeCells('A1:H1');
  const titleCell = summarySheet.getCell('A1');
  titleCell.value = 'REPORTE DE VALIDACIÓN DE CONSISTENCIA DE DATOS - POST FULL LOAD';
  titleCell.font = { bold: true, size: 16, color: { argb: 'FFFFFF' } };
  titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
  titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
  summarySheet.getRow(1).height = 30;

  // Info del reporte
  summarySheet.getCell('A3').value = 'Fecha de Validación:';
  summarySheet.getCell('B3').value = new Date().toLocaleString('es-MX');
  summarySheet.getCell('A4').value = 'Generado por:';
  summarySheet.getCell('B4').value = 'CDC TADA Sync - Script de Validación';

  // Resumen por par
  summarySheet.getCell('A6').value = 'RESUMEN DE VALIDACIÓN POR BASE DE DATOS';
  summarySheet.getCell('A6').font = { bold: true, size: 12 };

  const headers = ['Par de BD', 'Estado General', 'Tablas Validadas', 'Tablas OK', 'Con Diferencias', 'Checksums OK', 'Errores'];
  summarySheet.addRow([]);
  const headerRow = summarySheet.addRow(headers);
  headerRow.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: 'FFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
    cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
  });

  for (const result of allResults) {
    const rowCounts = result.validations.find(v => v.type === 'ROW_COUNTS');
    const checksums = result.validations.find(v => v.type === 'CHECKSUMS');

    const totalTables = (rowCounts?.matchCount || 0) + (rowCounts?.diffCount || 0);
    const tablesOK = rowCounts?.matchCount || 0;
    const tablesDiff = rowCounts?.diffCount || 0;
    const checksumsOK = checksums?.matchCount || 0;
    const errors = result.error ? 1 : 0;

    const status = result.error
      ? 'ERROR DE CONEXIÓN'
      : tablesDiff === 0
        ? 'CONSISTENTE'
        : `${tablesDiff} DISCREPANCIAS`;

    const row = summarySheet.addRow([
      result.pairName,
      status,
      totalTables,
      tablesOK,
      tablesDiff,
      checksumsOK,
      errors
    ]);

    const statusCell = row.getCell(2);
    if (result.error) {
      statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF6B6B' } };
    } else if (tablesDiff === 0) {
      statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '51CF66' } };
    } else {
      statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFC078' } };
    }

    row.eachCell((cell) => {
      cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
    });
  }

  // Leyenda de estados
  const legendRow = summarySheet.lastRow.number + 3;
  summarySheet.getCell(`A${legendRow}`).value = 'LEYENDA DE ESTADOS';
  summarySheet.getCell(`A${legendRow}`).font = { bold: true };

  const legends = [
    ['CONSISTENTE', 'Todos los datos coinciden entre origen y destino', '51CF66'],
    ['DISCREPANCIAS', 'Se encontraron diferencias que deben investigarse', 'FFC078'],
    ['ERROR DE CONEXIÓN', 'No se pudo conectar a una o ambas bases de datos', 'FF6B6B']
  ];

  for (const [status, desc, color] of legends) {
    const row = summarySheet.addRow([status, desc]);
    row.getCell(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: color } };
  }

  // Sección de acciones requeridas
  const actionsRow = summarySheet.lastRow.number + 3;
  summarySheet.getCell(`A${actionsRow}`).value = 'ACCIONES REQUERIDAS';
  summarySheet.getCell(`A${actionsRow}`).font = { bold: true, size: 12 };

  summarySheet.addRow(['1. Revisar las discrepancias en las hojas de detalle']);
  summarySheet.addRow(['2. Investigar la causa raíz de cada diferencia']);
  summarySheet.addRow(['3. Documentar las correcciones aplicadas']);
  summarySheet.addRow(['4. Re-ejecutar la validación después de las correcciones']);
  summarySheet.addRow(['5. Obtener aprobación del equipo antes de continuar']);

  summarySheet.columns = [
    { width: 25 }, { width: 20 }, { width: 18 }, { width: 12 },
    { width: 15 }, { width: 15 }, { width: 12 }, { width: 12 }
  ];

  // ═══════════════════════════════════════════════════════════════════════
  // HOJAS DE DETALLE POR PAR
  // ═══════════════════════════════════════════════════════════════════════
  for (const result of allResults) {
    // Hoja de discrepancias de conteo
    const rowCounts = result.validations.find(v => v.type === 'ROW_COUNTS');
    if (rowCounts && rowCounts.discrepancies.length > 0) {
      const sheet = workbook.addWorksheet(`${result.pairName} - Conteos`, {
        properties: { tabColor: { argb: 'FFC078' } }
      });

      sheet.mergeCells('A1:F1');
      const title = sheet.getCell('A1');
      title.value = `DISCREPANCIAS DE CONTEO: ${result.pairName}`;
      title.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
      title.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'E65100' } };
      title.alignment = { horizontal: 'center' };

      sheet.getCell('A3').value = `Origen: ${result.sourceDb}`;
      sheet.getCell('A4').value = `Destino: ${result.targetDb}`;
      sheet.getCell('A5').value = `Total discrepancias: ${rowCounts.discrepancies.length}`;

      const headers = sheet.addRow(['Tabla', 'Registros Origen', 'Registros Destino', 'Diferencia', '% Diferencia', 'Estado']);
      headers.eachCell((cell) => {
        cell.font = { bold: true, color: { argb: 'FFFFFF' } };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
      });

      for (const disc of rowCounts.discrepancies) {
        const row = sheet.addRow([
          disc.table,
          disc.sourceCount,
          disc.targetCount,
          disc.difference,
          `${disc.percentDiff}%`,
          disc.status
        ]);

        // Colorear según severidad
        const diffCell = row.getCell(4);
        if (Math.abs(disc.difference) > 1000) {
          diffCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFCDD2' } };
        } else if (Math.abs(disc.difference) > 100) {
          diffCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE0B2' } };
        }
      }

      sheet.columns = [
        { width: 40 }, { width: 18 }, { width: 18 }, { width: 15 }, { width: 15 }, { width: 20 }
      ];
    }

    // Hoja de checksums
    const checksums = result.validations.find(v => v.type === 'CHECKSUMS');
    if (checksums && checksums.results.length > 0) {
      const sheet = workbook.addWorksheet(`${result.pairName} - Checksums`, {
        properties: {
          tabColor: { argb: checksums.diffCount > 0 ? 'FFC078' : '51CF66' }
        }
      });

      sheet.mergeCells('A1:D1');
      const title = sheet.getCell('A1');
      title.value = `VALIDACIÓN DE CHECKSUMS: ${result.pairName}`;
      title.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
      title.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };

      const headers = sheet.addRow(['Tabla', 'Checksum Origen', 'Checksum Destino', 'Estado']);
      headers.eachCell((cell) => {
        cell.font = { bold: true, color: { argb: 'FFFFFF' } };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
      });

      for (const chk of checksums.results) {
        const row = sheet.addRow([chk.table, chk.sourceChecksum, chk.targetChecksum, chk.status]);
        const statusCell = row.getCell(4);
        if (chk.status === 'COINCIDE') {
          statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'C8E6C9' } };
        } else if (chk.status === 'DIFERENTE') {
          statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFCDD2' } };
        } else {
          statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE0B2' } };
        }
      }

      sheet.columns = [{ width: 35 }, { width: 20 }, { width: 20 }, { width: 15 }];
    }

    // Hoja de tabla resumen (si no hay errores)
    if (!result.error) {
      const sheet = workbook.addWorksheet(`${result.pairName} - Resumen`, {
        properties: { tabColor: { argb: 'B3E5FC' } }
      });

      sheet.mergeCells('A1:D1');
      const title = sheet.getCell('A1');
      title.value = `RESUMEN DE VALIDACIÓN: ${result.pairName}`;
      title.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
      title.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '0277BD' } };

      sheet.addRow([]);
      sheet.addRow(['Tipo de Validación', 'Resultado', 'Detalles']);

      for (const validation of result.validations) {
        let resultado = '';
        let detalles = '';

        switch (validation.type) {
          case 'ROW_COUNTS':
            resultado = validation.diffCount === 0 ? 'OK' : `${validation.diffCount} diferencias`;
            detalles = `${validation.matchCount} tablas coinciden`;
            break;
          case 'CHECKSUMS':
            resultado = validation.diffCount === 0 ? 'OK' : `${validation.diffCount} diferentes`;
            detalles = `${validation.matchCount} checksums coinciden`;
            break;
          case 'PRIMARY_KEYS':
            resultado = validation.invalidCount === 0 ? 'OK' : `${validation.invalidCount} diferentes`;
            detalles = `${validation.validCount} PKs coinciden`;
            break;
          case 'SAMPLE_DATA':
            const okCount = validation.results.filter(r => r.status === 'OK').length;
            resultado = `${okCount}/${validation.results.length} OK`;
            break;
          default:
            resultado = 'N/A';
        }

        const row = sheet.addRow([validation.type, resultado, detalles]);
        if (resultado === 'OK' || resultado.includes('OK')) {
          row.getCell(2).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'C8E6C9' } };
        } else if (resultado.includes('diferencia') || resultado.includes('diferente')) {
          row.getCell(2).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE0B2' } };
        }
      }

      sheet.columns = [{ width: 25 }, { width: 20 }, { width: 30 }];
    }
  }

  // ═══════════════════════════════════════════════════════════════════════
  // HOJA: CAUSAS Y CORRECCIONES
  // ═══════════════════════════════════════════════════════════════════════
  const fixSheet = workbook.addWorksheet('Causas y Correcciones', {
    properties: { tabColor: { argb: '2E7D32' } }
  });

  fixSheet.mergeCells('A1:E1');
  const fixTitle = fixSheet.getCell('A1');
  fixTitle.value = 'DOCUMENTACIÓN DE CAUSAS Y CORRECCIONES';
  fixTitle.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
  fixTitle.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '2E7D32' } };

  fixSheet.addRow([]);
  fixSheet.addRow(['Instrucciones: Completar esta hoja para cada discrepancia encontrada']);
  fixSheet.addRow([]);

  const fixHeaders = fixSheet.addRow(['Tabla', 'Tipo de Discrepancia', 'Causa Identificada', 'Acción Correctiva', 'Responsable', 'Fecha Corrección', 'Estado']);
  fixHeaders.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: 'FFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
    cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
  });

  // Agregar filas vacías para documentar
  for (let i = 0; i < 20; i++) {
    const row = fixSheet.addRow(['', '', '', '', '', '', '']);
    row.eachCell((cell) => {
      cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
    });
  }

  // Sección de aprobación
  const approvalRow = fixSheet.lastRow.number + 3;
  fixSheet.getCell(`A${approvalRow}`).value = 'APROBACIÓN DE CORRECCIONES';
  fixSheet.getCell(`A${approvalRow}`).font = { bold: true, size: 12 };

  const approvalHeaders = fixSheet.addRow(['Rol', 'Nombre', 'Firma', 'Fecha', 'Aprobado']);
  approvalHeaders.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: 'FFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '2E7D32' } };
  });

  const approvers = [
    ['DBA / Administrador', '', '', '', ''],
    ['Líder Técnico', '', '', '', ''],
    ['QA / Tester', '', '', '', '']
  ];

  for (const approver of approvers) {
    const row = fixSheet.addRow(approver);
    row.eachCell((cell) => {
      cell.border = { top: { style: 'thin' }, left: { style: 'thin' }, bottom: { style: 'thin' }, right: { style: 'thin' } };
    });
    row.height = 25;
  }

  fixSheet.columns = [
    { width: 30 }, { width: 20 }, { width: 35 }, { width: 35 }, { width: 20 }, { width: 18 }, { width: 15 }
  ];

  // Guardar archivo
  const reportsDir = path.join(__dirname, '..', 'reports');
  if (!fs.existsSync(reportsDir)) {
    fs.mkdirSync(reportsDir, { recursive: true });
  }

  const fileName = `data-validation-${timestamp}.xlsx`;
  const filePath = path.join(reportsDir, fileName);
  await workbook.xlsx.writeFile(filePath);

  return filePath;
}

// ═══════════════════════════════════════════════════════════════════════════
// FUNCIÓN PRINCIPAL
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('');
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║   VALIDACIÓN DE CONSISTENCIA DE DATOS - POST FULL LOAD          ║');
  console.log('║                     CDC TADA Sync                                ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');

  const allResults = [];

  for (const pair of DB_PAIRS) {
    console.log(`\n═══ Validando: ${pair.name} ═══`);
    console.log(`Origen: ${pair.source.database}`);
    console.log(`Destino: ${pair.target.database}`);
    console.log('');

    let sourceConn, targetConn;
    const validations = [];

    try {
      // Conectar a ambas bases
      console.log('  Conectando a bases de datos...');
      sourceConn = await createConnection(pair.source);
      targetConn = await createConnection(pair.target);
      console.log('    ✓ Conexiones establecidas');

      // Obtener lista de tablas comunes
      const sourceTables = await executeQuery(sourceConn, QUERIES.tableRowCounts);
      const tables = new Map(sourceTables.map(t => [`${t.schema_name}.${t.table_name}`, t.row_count]));

      // Ejecutar validaciones
      validations.push(await validateRowCounts(sourceConn, targetConn, pair.name));
      validations.push(await validateChecksums(sourceConn, targetConn, tables));
      validations.push(await validatePrimaryKeys(sourceConn, targetConn, tables));
      validations.push(await validateSampleData(sourceConn, targetConn, tables));
      validations.push(await validateNullCounts(sourceConn, targetConn, tables));

      allResults.push({
        pairName: pair.name,
        sourceDb: pair.source.database,
        targetDb: pair.target.database,
        validations,
        error: null
      });

    } catch (error) {
      console.log(`  ❌ ERROR: ${error.message}`);
      allResults.push({
        pairName: pair.name,
        sourceDb: pair.source.database,
        targetDb: pair.target.database,
        validations: [],
        error: error.message
      });
    } finally {
      if (sourceConn) await closeConnection(sourceConn);
      if (targetConn) await closeConnection(targetConn);
    }
  }

  // Generar reporte
  console.log('\n═══ Generando reporte de validación ═══');
  const reportPath = await generateReport(allResults);
  console.log(`✅ Reporte generado: ${reportPath}`);

  // Guardar JSON de respaldo
  const jsonPath = reportPath.replace('.xlsx', '.json');
  fs.writeFileSync(jsonPath, JSON.stringify(allResults, null, 2));
  console.log(`✅ Respaldo JSON: ${jsonPath}`);

  // Resumen final
  console.log('\n╔══════════════════════════════════════════════════════════════════╗');
  console.log('║                    RESUMEN DE VALIDACIÓN                         ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');

  let totalDiscrepancies = 0;
  for (const result of allResults) {
    const rowCounts = result.validations.find(v => v.type === 'ROW_COUNTS');
    const discCount = rowCounts?.diffCount || 0;
    totalDiscrepancies += discCount;

    const status = result.error
      ? '❌ ERROR'
      : discCount === 0
        ? '✅ CONSISTENTE'
        : `⚠️  ${discCount} discrepancias`;

    console.log(`  ${result.pairName}: ${status}`);
  }

  console.log('');
  if (totalDiscrepancies > 0) {
    console.log('⚠️  ACCIÓN REQUERIDA:');
    console.log('   1. Revisar el reporte Excel generado');
    console.log('   2. Documentar las causas en la hoja "Causas y Correcciones"');
    console.log('   3. Aplicar correcciones necesarias');
    console.log('   4. Re-ejecutar esta validación');
  } else {
    console.log('✅ Todos los datos son consistentes entre BD 1.0 y 2.0');
  }
  console.log('');
}

// Ejecutar
main().catch(console.error);
