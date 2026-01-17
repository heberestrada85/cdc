/**
 * Script de Comparación de Estructuras - Solo Tablas con Funcionalidad Compartida
 *
 * Este script compara ÚNICAMENTE las tablas que comparten funcionalidad entre
 * TadaNomina 1.0 y TadaNomina 2.0, excluyendo las tablas de empleados/personas
 * que fueron rediseñadas para TADÁ 2.0.
 *
 * IMPORTANTE: Respeta la estructura de empleados/personas generada para TADÁ 2.0
 *
 * Uso: node scripts/compare-shared-tables.js
 */

require('dotenv').config();
const { Connection, Request } = require('tedious');
const ExcelJS = require('exceljs');
const path = require('path');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════════════════
// TABLAS EXCLUIDAS - Estructura de Empleados/Personas rediseñada para TADÁ 2.0
// ═══════════════════════════════════════════════════════════════════════════

const EXCLUDED_TABLES = [
  // Tablas de empleados rediseñadas para TADÁ 2.0
  'Empleados',
  'Empleados_B',
  'EmpleadoInformacionComplementaria',
  'EmpleadoInformacionComplementaria_B',
  'EmpleadosAccionDisciplinaria',
  'EmpleadosPTU',
  'InfoEmpleado',
  'EAM_Empleados',
  'HistorialEmpleados',
  'Historial_Estatus_Empleado',
  'Cat_EstatusEmpleado',
  'Cat_EmpleadoSolicitud',
  'Cat_EmpleadosServicio',
  'Cat_CamposModificaEmp',
  'Cat_CuentaEmp',
  'Cat_TipoCuentaEmp',
  'ModuloEmpleadosNotificaciones',
  'MotivosBajaEmpleado',
  'Cat_MotivosBajaEmpleado',
  'Cat_MotivosBajaEmpleadoInterno',

  // Tablas de log/errores de migración (no son parte del sistema)
  'LogErrores_MaestroPersonaDatosTada20',
  'LogErrores_personasTada20',

  // Tablas temporales y de sistema
  'CDC_SyncLog',
  'sysdiagrams',
  'systranschemas',
  'dbo.tmp_ag_siem',
  'tmp_BajaCliente',
  'tmp_BajaCliente_Wingstop',

  // Tablas de respaldo
  'RegistroPatronal_prt_2023_respaldo',
  'regs_patronales_32',
  'resp_unidades_truehome'
];

// ═══════════════════════════════════════════════════════════════════════════
// TABLAS A COMPARAR - Solo las que tienen funcionalidad compartida
// ═══════════════════════════════════════════════════════════════════════════

// Importar tablas de sincronización y filtrar las excluidas
const allTablesToSync = require('../src/config/tablesToSync.js');
const TABLES_TO_COMPARE = allTablesToSync
  .map(t => t.name)
  .filter(name => !EXCLUDED_TABLES.includes(name));

console.log(`Total tablas en tablesToSync: ${allTablesToSync.length}`);
console.log(`Tablas excluidas: ${EXCLUDED_TABLES.length}`);
console.log(`Tablas a comparar: ${TABLES_TO_COMPARE.length}`);

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURACIÓN DE BASES DE DATOS
// ═══════════════════════════════════════════════════════════════════════════

const DB_CONFIG = {
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
        requestTimeout: 120000
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
// QUERIES PARA ESTRUCTURA (filtradas por tablas específicas)
// ═══════════════════════════════════════════════════════════════════════════

function getTablesQuery(tableNames) {
  const tableList = tableNames.map(t => `'${t}'`).join(',');
  return `
    SELECT
      s.name AS schema_name,
      t.name AS table_name,
      t.create_date,
      t.modify_date,
      p.rows AS row_count
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
    WHERE t.type = 'U' AND t.name IN (${tableList})
    ORDER BY s.name, t.name
  `;
}

function getColumnsQuery(tableNames) {
  const tableList = tableNames.map(t => `'${t}'`).join(',');
  return `
    SELECT
      s.name AS schema_name,
      t.name AS table_name,
      c.name AS column_name,
      c.column_id,
      TYPE_NAME(c.user_type_id) AS data_type,
      c.max_length,
      c.precision,
      c.scale,
      c.is_nullable,
      c.is_identity,
      ISNULL(dc.definition, '') AS default_value
    FROM sys.columns c
    INNER JOIN sys.tables t ON c.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
    WHERE t.type = 'U' AND t.name IN (${tableList})
    ORDER BY s.name, t.name, c.column_id
  `;
}

function getIndexesQuery(tableNames) {
  const tableList = tableNames.map(t => `'${t}'`).join(',');
  return `
    SELECT
      s.name AS schema_name,
      t.name AS table_name,
      i.name AS index_name,
      i.type_desc AS index_type,
      i.is_unique,
      i.is_primary_key,
      STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
    FROM sys.indexes i
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    WHERE t.type = 'U' AND i.name IS NOT NULL AND t.name IN (${tableList})
    GROUP BY s.name, t.name, i.name, i.type_desc, i.is_unique, i.is_primary_key
    ORDER BY s.name, t.name, i.name
  `;
}

function getForeignKeysQuery(tableNames) {
  const tableList = tableNames.map(t => `'${t}'`).join(',');
  return `
    SELECT
      s.name AS schema_name,
      t.name AS table_name,
      fk.name AS fk_name,
      COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS column_name,
      OBJECT_SCHEMA_NAME(fkc.referenced_object_id) AS referenced_schema,
      OBJECT_NAME(fkc.referenced_object_id) AS referenced_table,
      COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS referenced_column
    FROM sys.foreign_keys fk
    INNER JOIN sys.tables t ON fk.parent_object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    WHERE t.name IN (${tableList})
    ORDER BY s.name, t.name, fk.name
  `;
}

// ═══════════════════════════════════════════════════════════════════════════
// OBTENER ESTRUCTURA FILTRADA
// ═══════════════════════════════════════════════════════════════════════════

async function getFilteredDbStructure(config, tableNames) {
  let connection;
  try {
    console.log(`  Conectando a ${config.database}...`);
    connection = await createConnection(config);

    console.log(`  Obteniendo tablas (${tableNames.length} esperadas)...`);
    const tables = await executeQuery(connection, getTablesQuery(tableNames));

    console.log(`  Obteniendo columnas...`);
    const columns = await executeQuery(connection, getColumnsQuery(tableNames));

    console.log(`  Obteniendo índices...`);
    const indexes = await executeQuery(connection, getIndexesQuery(tableNames));

    console.log(`  Obteniendo foreign keys...`);
    const foreignKeys = await executeQuery(connection, getForeignKeysQuery(tableNames));

    return {
      database: config.database,
      tables,
      columns,
      indexes,
      foreignKeys,
      error: null
    };
  } catch (error) {
    console.error(`  ERROR: ${error.message}`);
    return {
      database: config.database,
      tables: [],
      columns: [],
      indexes: [],
      foreignKeys: [],
      error: error.message
    };
  } finally {
    if (connection) {
      await closeConnection(connection);
    }
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// COMPARACIÓN DE ESTRUCTURAS
// ═══════════════════════════════════════════════════════════════════════════

function compareStructures(source, target) {
  const differences = {
    tablesOnlyInSource: [],
    tablesOnlyInTarget: [],
    tablesInBoth: [],
    columnDifferences: [],
    indexDifferences: [],
    fkDifferences: []
  };

  const sourceTableMap = new Map(source.tables.map(t => [`${t.schema_name}.${t.table_name}`, t]));
  const targetTableMap = new Map(target.tables.map(t => [`${t.schema_name}.${t.table_name}`, t]));

  // Tablas solo en origen (dentro de las que esperamos)
  for (const [key, table] of sourceTableMap) {
    if (!targetTableMap.has(key)) {
      differences.tablesOnlyInSource.push({
        schema: table.schema_name,
        table: table.table_name,
        rowCount: table.row_count
      });
    } else {
      differences.tablesInBoth.push({
        schema: table.schema_name,
        table: table.table_name,
        sourceRows: table.row_count,
        targetRows: targetTableMap.get(key).row_count
      });
    }
  }

  // Tablas solo en destino (dentro de las esperadas que faltan en origen)
  for (const [key, table] of targetTableMap) {
    if (!sourceTableMap.has(key)) {
      differences.tablesOnlyInTarget.push({
        schema: table.schema_name,
        table: table.table_name,
        rowCount: table.row_count
      });
    }
  }

  // Comparar columnas
  const sourceColumnMap = new Map();
  source.columns.forEach(c => {
    const key = `${c.schema_name}.${c.table_name}.${c.column_name}`;
    sourceColumnMap.set(key, c);
  });

  const targetColumnMap = new Map();
  target.columns.forEach(c => {
    const key = `${c.schema_name}.${c.table_name}.${c.column_name}`;
    targetColumnMap.set(key, c);
  });

  // Columnas solo en origen o diferentes
  for (const [key, srcCol] of sourceColumnMap) {
    const tableKey = `${srcCol.schema_name}.${srcCol.table_name}`;
    if (!targetTableMap.has(tableKey)) continue;

    const tgtCol = targetColumnMap.get(key);
    if (!tgtCol) {
      differences.columnDifferences.push({
        schema: srcCol.schema_name,
        table: srcCol.table_name,
        column: srcCol.column_name,
        type: 'SOLO_EN_ORIGEN',
        sourceValue: `${srcCol.data_type}(${srcCol.max_length})`,
        targetValue: '-'
      });
    } else {
      if (srcCol.data_type !== tgtCol.data_type) {
        differences.columnDifferences.push({
          schema: srcCol.schema_name,
          table: srcCol.table_name,
          column: srcCol.column_name,
          type: 'TIPO_DIFERENTE',
          sourceValue: srcCol.data_type,
          targetValue: tgtCol.data_type
        });
      }
      if (srcCol.max_length !== tgtCol.max_length) {
        differences.columnDifferences.push({
          schema: srcCol.schema_name,
          table: srcCol.table_name,
          column: srcCol.column_name,
          type: 'LONGITUD_DIFERENTE',
          sourceValue: String(srcCol.max_length),
          targetValue: String(tgtCol.max_length)
        });
      }
      if (srcCol.is_nullable !== tgtCol.is_nullable) {
        differences.columnDifferences.push({
          schema: srcCol.schema_name,
          table: srcCol.table_name,
          column: srcCol.column_name,
          type: 'NULLABLE_DIFERENTE',
          sourceValue: srcCol.is_nullable ? 'NULL' : 'NOT NULL',
          targetValue: tgtCol.is_nullable ? 'NULL' : 'NOT NULL'
        });
      }
    }
  }

  // Columnas solo en destino
  for (const [key, tgtCol] of targetColumnMap) {
    const tableKey = `${tgtCol.schema_name}.${tgtCol.table_name}`;
    if (!sourceTableMap.has(tableKey)) continue;

    if (!sourceColumnMap.has(key)) {
      differences.columnDifferences.push({
        schema: tgtCol.schema_name,
        table: tgtCol.table_name,
        column: tgtCol.column_name,
        type: 'SOLO_EN_DESTINO',
        sourceValue: '-',
        targetValue: `${tgtCol.data_type}(${tgtCol.max_length})`
      });
    }
  }

  // Comparar índices
  const sourceIndexMap = new Map();
  source.indexes.forEach(i => {
    const key = `${i.schema_name}.${i.table_name}.${i.index_name}`;
    sourceIndexMap.set(key, i);
  });

  const targetIndexMap = new Map();
  target.indexes.forEach(i => {
    const key = `${i.schema_name}.${i.table_name}.${i.index_name}`;
    targetIndexMap.set(key, i);
  });

  for (const [key, srcIdx] of sourceIndexMap) {
    const tableKey = `${srcIdx.schema_name}.${srcIdx.table_name}`;
    if (!targetTableMap.has(tableKey)) continue;

    const tgtIdx = targetIndexMap.get(key);
    if (!tgtIdx) {
      differences.indexDifferences.push({
        schema: srcIdx.schema_name,
        table: srcIdx.table_name,
        index: srcIdx.index_name,
        type: 'SOLO_EN_ORIGEN',
        sourceValue: `${srcIdx.index_type} (${srcIdx.columns})`,
        targetValue: '-'
      });
    } else if (srcIdx.columns !== tgtIdx.columns) {
      differences.indexDifferences.push({
        schema: srcIdx.schema_name,
        table: srcIdx.table_name,
        index: srcIdx.index_name,
        type: 'COLUMNAS_DIFERENTES',
        sourceValue: srcIdx.columns,
        targetValue: tgtIdx.columns
      });
    }
  }

  for (const [key, tgtIdx] of targetIndexMap) {
    const tableKey = `${tgtIdx.schema_name}.${tgtIdx.table_name}`;
    if (!sourceTableMap.has(tableKey)) continue;

    if (!sourceIndexMap.has(key)) {
      differences.indexDifferences.push({
        schema: tgtIdx.schema_name,
        table: tgtIdx.table_name,
        index: tgtIdx.index_name,
        type: 'SOLO_EN_DESTINO',
        sourceValue: '-',
        targetValue: `${tgtIdx.index_type} (${tgtIdx.columns})`
      });
    }
  }

  return differences;
}

// ═══════════════════════════════════════════════════════════════════════════
// GENERACIÓN DE REPORTE EXCEL CONSOLIDADO
// ═══════════════════════════════════════════════════════════════════════════

async function generateConsolidatedReport(result) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = 'CDC TADA Sync - Comparación Filtrada';
  workbook.created = new Date();

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);

  // ═══════════════════════════════════════════════════════════════════════
  // HOJA 1: RESUMEN EJECUTIVO
  // ═══════════════════════════════════════════════════════════════════════
  const summarySheet = workbook.addWorksheet('Resumen Ejecutivo', {
    properties: { tabColor: { argb: '4472C4' } }
  });

  // Título
  summarySheet.mergeCells('A1:G1');
  const titleCell = summarySheet.getCell('A1');
  titleCell.value = 'COMPARACIÓN DE ESTRUCTURAS - TABLAS CON FUNCIONALIDAD COMPARTIDA';
  titleCell.font = { bold: true, size: 16, color: { argb: 'FFFFFF' } };
  titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
  titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
  summarySheet.getRow(1).height = 35;

  // Subtítulo importante
  summarySheet.mergeCells('A2:G2');
  const subtitleCell = summarySheet.getCell('A2');
  subtitleCell.value = 'NOTA: Se excluyen tablas de Empleados/Personas rediseñadas para TADÁ 2.0';
  subtitleCell.font = { bold: true, size: 11, color: { argb: 'C00000' } };
  subtitleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFF2CC' } };
  subtitleCell.alignment = { horizontal: 'center' };
  summarySheet.getRow(2).height = 25;

  // Información del reporte
  summarySheet.getCell('A4').value = 'Fecha de Generación:';
  summarySheet.getCell('A4').font = { bold: true };
  summarySheet.getCell('B4').value = new Date().toLocaleString('es-MX');

  summarySheet.getCell('A5').value = 'Base de Datos Origen:';
  summarySheet.getCell('A5').font = { bold: true };
  summarySheet.getCell('B5').value = result.sourceStructure.database;

  summarySheet.getCell('A6').value = 'Base de Datos Destino:';
  summarySheet.getCell('A6').font = { bold: true };
  summarySheet.getCell('B6').value = result.targetStructure.database;

  summarySheet.getCell('A7').value = 'Tablas Analizadas:';
  summarySheet.getCell('A7').font = { bold: true };
  summarySheet.getCell('B7').value = TABLES_TO_COMPARE.length;

  summarySheet.getCell('A8').value = 'Tablas Excluidas:';
  summarySheet.getCell('A8').font = { bold: true };
  summarySheet.getCell('B8').value = EXCLUDED_TABLES.length;

  // Calcular totales
  const totalDiffs =
    result.differences.tablesOnlyInSource.length +
    result.differences.tablesOnlyInTarget.length +
    result.differences.columnDifferences.length +
    result.differences.indexDifferences.length;

  // Resumen de resultados
  summarySheet.getCell('A10').value = 'RESUMEN DE RESULTADOS';
  summarySheet.getCell('A10').font = { bold: true, size: 12 };

  const statsData = [
    ['Métrica', 'Cantidad', 'Estado'],
    ['Tablas en ambas BD', result.differences.tablesInBoth.length, 'OK'],
    ['Tablas solo en Origen', result.differences.tablesOnlyInSource.length, result.differences.tablesOnlyInSource.length > 0 ? 'REVISAR' : 'OK'],
    ['Tablas solo en Destino', result.differences.tablesOnlyInTarget.length, result.differences.tablesOnlyInTarget.length > 0 ? 'REVISAR' : 'OK'],
    ['Diferencias en Columnas', result.differences.columnDifferences.length, result.differences.columnDifferences.length > 0 ? 'REVISAR' : 'OK'],
    ['Diferencias en Índices', result.differences.indexDifferences.length, result.differences.indexDifferences.length > 0 ? 'REVISAR' : 'OK'],
    ['TOTAL DIFERENCIAS', totalDiffs, totalDiffs === 0 ? 'SIN DIFERENCIAS' : `${totalDiffs} DIFERENCIAS`]
  ];

  let rowNum = 11;
  for (let i = 0; i < statsData.length; i++) {
    const row = summarySheet.addRow(statsData[i]);
    if (i === 0) {
      row.eachCell((cell) => {
        cell.font = { bold: true, color: { argb: 'FFFFFF' } };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
      });
    } else {
      const statusCell = row.getCell(3);
      if (statsData[i][2] === 'OK') {
        statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'C6EFCE' } };
      } else if (statsData[i][2] === 'REVISAR') {
        statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFEB9C' } };
      } else if (statsData[i][2].includes('SIN DIFERENCIAS')) {
        statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '51CF66' } };
        statusCell.font = { bold: true };
      } else {
        statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFC7CE' } };
        statusCell.font = { bold: true };
      }
    }
    row.eachCell((cell) => {
      cell.border = {
        top: { style: 'thin' },
        left: { style: 'thin' },
        bottom: { style: 'thin' },
        right: { style: 'thin' }
      };
    });
    rowNum++;
  }

  // ═══════════════════════════════════════════════════════════════════════
  // SECCIÓN DE APROBACIÓN
  // ═══════════════════════════════════════════════════════════════════════
  rowNum += 2;
  summarySheet.getCell(`A${rowNum}`).value = 'SECCIÓN DE APROBACIÓN';
  summarySheet.getCell(`A${rowNum}`).font = { bold: true, size: 12 };
  rowNum++;

  const approvalHeaders = ['Rol', 'Nombre', 'Firma', 'Fecha', 'Aprobado (Sí/No)'];
  const approvalHeaderRow = summarySheet.addRow(approvalHeaders);
  approvalHeaderRow.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: 'FFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '2E7D32' } };
    cell.border = {
      top: { style: 'thin' },
      left: { style: 'thin' },
      bottom: { style: 'thin' },
      right: { style: 'thin' }
    };
  });

  const approvers = [
    ['DBA / Administrador de BD', '', '', '', ''],
    ['Líder Técnico', '', '', '', ''],
    ['Gerente de Proyecto', '', '', '', '']
  ];

  for (const approver of approvers) {
    const row = summarySheet.addRow(approver);
    row.eachCell((cell) => {
      cell.border = {
        top: { style: 'thin' },
        left: { style: 'thin' },
        bottom: { style: 'thin' },
        right: { style: 'thin' }
      };
    });
    row.height = 25;
  }

  summarySheet.columns = [
    { width: 30 }, { width: 25 }, { width: 20 }, { width: 15 }, { width: 18 }, { width: 15 }, { width: 15 }
  ];

  // ═══════════════════════════════════════════════════════════════════════
  // HOJA 2: TABLAS EXCLUIDAS (para referencia)
  // ═══════════════════════════════════════════════════════════════════════
  const excludedSheet = workbook.addWorksheet('Tablas Excluidas', {
    properties: { tabColor: { argb: 'FFB74D' } }
  });

  excludedSheet.mergeCells('A1:C1');
  const exTitle = excludedSheet.getCell('A1');
  exTitle.value = 'TABLAS EXCLUIDAS DE LA COMPARACIÓN';
  exTitle.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
  exTitle.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF6F00' } };
  exTitle.alignment = { horizontal: 'center' };

  excludedSheet.mergeCells('A2:C2');
  excludedSheet.getCell('A2').value = 'Estas tablas fueron rediseñadas para TADÁ 2.0 y no se comparan';
  excludedSheet.getCell('A2').font = { italic: true };

  excludedSheet.addRow([]);
  const exHeaders = excludedSheet.addRow(['#', 'Nombre de Tabla', 'Razón de Exclusión']);
  exHeaders.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: 'FFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
  });

  EXCLUDED_TABLES.forEach((table, idx) => {
    let reason = 'Rediseñada para TADÁ 2.0';
    if (table.includes('Log') || table.includes('tmp') || table.includes('sys')) {
      reason = 'Tabla de sistema/temporal';
    } else if (table.includes('respaldo') || table.includes('regs_')) {
      reason = 'Tabla de respaldo';
    }
    excludedSheet.addRow([idx + 1, table, reason]);
  });

  excludedSheet.columns = [{ width: 5 }, { width: 45 }, { width: 35 }];

  // ═══════════════════════════════════════════════════════════════════════
  // HOJA 3: DIFERENCIAS DETALLADAS
  // ═══════════════════════════════════════════════════════════════════════
  const diffSheet = workbook.addWorksheet('Diferencias Detalladas', {
    properties: { tabColor: { argb: totalDiffs === 0 ? '51CF66' : 'FFC078' } }
  });

  diffSheet.mergeCells('A1:F1');
  const diffTitle = diffSheet.getCell('A1');
  diffTitle.value = 'DIFERENCIAS ENCONTRADAS EN TABLAS COMPARTIDAS';
  diffTitle.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
  diffTitle.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
  diffTitle.alignment = { horizontal: 'center' };
  diffSheet.getRow(1).height = 25;

  rowNum = 3;

  if (totalDiffs === 0) {
    diffSheet.mergeCells(`A${rowNum}:F${rowNum}`);
    const noDiffCell = diffSheet.getCell(`A${rowNum}`);
    noDiffCell.value = 'SIN DIFERENCIAS - Las estructuras de las tablas compartidas son idénticas';
    noDiffCell.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
    noDiffCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '51CF66' } };
    noDiffCell.alignment = { horizontal: 'center' };
    diffSheet.getRow(rowNum).height = 30;
  } else {
    // Tablas solo en origen
    if (result.differences.tablesOnlyInSource.length > 0) {
      diffSheet.getCell(`A${rowNum}`).value = 'TABLAS SOLO EN ORIGEN (Faltan en Destino)';
      diffSheet.getCell(`A${rowNum}`).font = { bold: true, size: 11 };
      diffSheet.getCell(`A${rowNum}`).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE0B2' } };
      rowNum++;

      const headers = diffSheet.addRow(['Schema', 'Tabla', 'Registros en Origen']);
      headers.eachCell((cell) => {
        cell.font = { bold: true };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'E3F2FD' } };
      });
      rowNum++;

      for (const table of result.differences.tablesOnlyInSource) {
        diffSheet.addRow([table.schema, table.table, table.rowCount]);
        rowNum++;
      }
      rowNum++;
    }

    // Tablas solo en destino
    if (result.differences.tablesOnlyInTarget.length > 0) {
      diffSheet.getCell(`A${rowNum}`).value = 'TABLAS SOLO EN DESTINO (Faltan en Origen)';
      diffSheet.getCell(`A${rowNum}`).font = { bold: true, size: 11 };
      diffSheet.getCell(`A${rowNum}`).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'C8E6C9' } };
      rowNum++;

      const headers = diffSheet.addRow(['Schema', 'Tabla', 'Registros en Destino']);
      headers.eachCell((cell) => {
        cell.font = { bold: true };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'E3F2FD' } };
      });
      rowNum++;

      for (const table of result.differences.tablesOnlyInTarget) {
        diffSheet.addRow([table.schema, table.table, table.rowCount]);
        rowNum++;
      }
      rowNum++;
    }

    // Diferencias en columnas
    if (result.differences.columnDifferences.length > 0) {
      diffSheet.getCell(`A${rowNum}`).value = 'DIFERENCIAS EN COLUMNAS';
      diffSheet.getCell(`A${rowNum}`).font = { bold: true, size: 11 };
      diffSheet.getCell(`A${rowNum}`).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFCDD2' } };
      rowNum++;

      const headers = diffSheet.addRow(['Schema', 'Tabla', 'Columna', 'Tipo Diferencia', 'Valor Origen', 'Valor Destino']);
      headers.eachCell((cell) => {
        cell.font = { bold: true };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'E3F2FD' } };
      });
      rowNum++;

      for (const diff of result.differences.columnDifferences) {
        diffSheet.addRow([diff.schema, diff.table, diff.column, diff.type, diff.sourceValue, diff.targetValue]);
        rowNum++;
      }
      rowNum++;
    }

    // Diferencias en índices
    if (result.differences.indexDifferences.length > 0) {
      diffSheet.getCell(`A${rowNum}`).value = 'DIFERENCIAS EN ÍNDICES';
      diffSheet.getCell(`A${rowNum}`).font = { bold: true, size: 11 };
      diffSheet.getCell(`A${rowNum}`).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'D1C4E9' } };
      rowNum++;

      const headers = diffSheet.addRow(['Schema', 'Tabla', 'Índice', 'Tipo Diferencia', 'Valor Origen', 'Valor Destino']);
      headers.eachCell((cell) => {
        cell.font = { bold: true };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'E3F2FD' } };
      });
      rowNum++;

      for (const diff of result.differences.indexDifferences) {
        diffSheet.addRow([diff.schema, diff.table, diff.index, diff.type, diff.sourceValue, diff.targetValue]);
        rowNum++;
      }
    }
  }

  diffSheet.columns = [
    { width: 12 }, { width: 35 }, { width: 30 }, { width: 22 }, { width: 30 }, { width: 30 }
  ];

  // ═══════════════════════════════════════════════════════════════════════
  // HOJA 4: TABLAS COMPARADAS (las que coinciden)
  // ═══════════════════════════════════════════════════════════════════════
  const matchedSheet = workbook.addWorksheet('Tablas Comparadas', {
    properties: { tabColor: { argb: '81C784' } }
  });

  matchedSheet.mergeCells('A1:E1');
  const matchTitle = matchedSheet.getCell('A1');
  matchTitle.value = 'TABLAS CON FUNCIONALIDAD COMPARTIDA - COMPARACIÓN DE REGISTROS';
  matchTitle.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
  matchTitle.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '388E3C' } };
  matchTitle.alignment = { horizontal: 'center' };

  matchedSheet.addRow([]);
  const matchHeaders = matchedSheet.addRow(['Schema', 'Tabla', 'Registros Origen', 'Registros Destino', 'Diferencia']);
  matchHeaders.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: 'FFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
  });

  for (const table of result.differences.tablesInBoth) {
    const diff = table.targetRows - table.sourceRows;
    const row = matchedSheet.addRow([
      table.schema,
      table.table,
      table.sourceRows,
      table.targetRows,
      diff
    ]);

    const diffCell = row.getCell(5);
    if (diff === 0) {
      diffCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'C6EFCE' } };
    } else if (Math.abs(diff) > 0) {
      diffCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFEB9C' } };
    }
  }

  matchedSheet.columns = [
    { width: 12 }, { width: 40 }, { width: 18 }, { width: 18 }, { width: 15 }
  ];

  // Guardar archivo
  const reportsDir = path.join(__dirname, '..', 'reports');
  if (!fs.existsSync(reportsDir)) {
    fs.mkdirSync(reportsDir, { recursive: true });
  }

  const fileName = `shared-tables-comparison-${timestamp}.xlsx`;
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
  console.log('║  COMPARACIÓN DE ESTRUCTURAS - TABLAS CON FUNCIONALIDAD COMPARTIDA║');
  console.log('║                                                                  ║');
  console.log('║  NOTA: Se excluyen tablas de Empleados/Personas (TADÁ 2.0)       ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');

  console.log(`═══ Configuración ═══`);
  console.log(`  Tablas a comparar: ${TABLES_TO_COMPARE.length}`);
  console.log(`  Tablas excluidas: ${EXCLUDED_TABLES.length}`);
  console.log('');

  console.log('═══ Obteniendo estructura de ORIGEN ═══');
  const sourceStructure = await getFilteredDbStructure(DB_CONFIG.source, TABLES_TO_COMPARE);
  console.log(`  Tablas encontradas: ${sourceStructure.tables.length}`);

  console.log('');
  console.log('═══ Obteniendo estructura de DESTINO ═══');
  const targetStructure = await getFilteredDbStructure(DB_CONFIG.target, TABLES_TO_COMPARE);
  console.log(`  Tablas encontradas: ${targetStructure.tables.length}`);

  console.log('');
  console.log('═══ Comparando estructuras ═══');
  const differences = compareStructures(sourceStructure, targetStructure);

  const totalDiffs =
    differences.tablesOnlyInSource.length +
    differences.tablesOnlyInTarget.length +
    differences.columnDifferences.length +
    differences.indexDifferences.length;

  if (sourceStructure.error || targetStructure.error) {
    console.log('  ERROR DE CONEXIÓN');
    if (sourceStructure.error) console.log(`    Origen: ${sourceStructure.error}`);
    if (targetStructure.error) console.log(`    Destino: ${targetStructure.error}`);
  } else if (totalDiffs === 0) {
    console.log('  SIN DIFERENCIAS en tablas compartidas');
  } else {
    console.log(`  ${totalDiffs} diferencias encontradas:`);
    if (differences.tablesOnlyInSource.length > 0) {
      console.log(`    - ${differences.tablesOnlyInSource.length} tablas solo en origen`);
    }
    if (differences.tablesOnlyInTarget.length > 0) {
      console.log(`    - ${differences.tablesOnlyInTarget.length} tablas solo en destino`);
    }
    if (differences.columnDifferences.length > 0) {
      console.log(`    - ${differences.columnDifferences.length} diferencias en columnas`);
    }
    if (differences.indexDifferences.length > 0) {
      console.log(`    - ${differences.indexDifferences.length} diferencias en índices`);
    }
  }

  const result = {
    pairName: DB_CONFIG.name,
    sourceStructure,
    targetStructure,
    differences
  };

  console.log('');
  console.log('═══ Generando reporte consolidado ═══');
  const reportPath = await generateConsolidatedReport(result);
  console.log(`  Reporte generado: ${reportPath}`);

  // Guardar JSON de respaldo
  const jsonPath = reportPath.replace('.xlsx', '.json');
  fs.writeFileSync(jsonPath, JSON.stringify({
    metadata: {
      generatedAt: new Date().toISOString(),
      tablesToCompare: TABLES_TO_COMPARE.length,
      tablesExcluded: EXCLUDED_TABLES.length,
      excludedTables: EXCLUDED_TABLES
    },
    result
  }, null, 2));
  console.log(`  Respaldo JSON: ${jsonPath}`);

  console.log('');
  console.log('╔══════════════════════════════════════════════════════════════════╗');
  console.log('║                    PROCESO COMPLETADO                            ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');
  console.log('IMPORTANTE:');
  console.log('  - Se compararon SOLO tablas con funcionalidad compartida');
  console.log('  - Las tablas de Empleados/Personas de TADÁ 2.0 fueron EXCLUIDAS');
  console.log('  - Revisar el reporte Excel para detalles completos');
  console.log('');
}

// Ejecutar
main().catch(console.error);
