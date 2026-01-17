/**
 * Script de Comparación de Estructuras de Bases de Datos
 *
 * Genera un reporte Excel detallado comparando las estructuras de los 3 pares de BD:
 * - TadaNomina ↔ TadaNomina-2.0
 * - TadaModuloSeguridad ↔ TadaModuloSeguridad-2.0
 * - TadaChecador ↔ TadaChecador-2.0
 *
 * Criterios de Aceptación:
 * - Reporte en formato Excel con diferencias detalladas
 * - Incluye estructuras actuales de cada base
 * - Marca explícitamente "SIN DIFERENCIAS" si un par coincide
 * - Sección de aprobación para revisión del equipo
 *
 * Uso: node scripts/compare-db-structures.js
 */

require('dotenv').config();
const { Connection, Request, TYPES } = require('tedious');
const ExcelJS = require('exceljs');
const path = require('path');
const fs = require('fs');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURACIÓN DE PARES DE BASES DE DATOS
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
        requestTimeout: 60000
      }
    });

    connection.on('connect', (err) => {
      if (err) {
        reject(err);
      } else {
        resolve(connection);
      }
    });

    connection.connect();
  });
}

function executeQuery(connection, sql) {
  return new Promise((resolve, reject) => {
    const results = [];
    const request = new Request(sql, (err, rowCount) => {
      if (err) {
        reject(err);
      } else {
        resolve(results);
      }
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
// QUERIES PARA OBTENER ESTRUCTURA
// ═══════════════════════════════════════════════════════════════════════════

const QUERIES = {
  // Obtener todas las tablas con sus esquemas
  tables: `
    SELECT
      s.name AS schema_name,
      t.name AS table_name,
      t.create_date,
      t.modify_date,
      p.rows AS row_count
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
    WHERE t.type = 'U'
    ORDER BY s.name, t.name
  `,

  // Obtener columnas de todas las tablas
  columns: `
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
    WHERE t.type = 'U'
    ORDER BY s.name, t.name, c.column_id
  `,

  // Obtener índices
  indexes: `
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
    WHERE t.type = 'U' AND i.name IS NOT NULL
    GROUP BY s.name, t.name, i.name, i.type_desc, i.is_unique, i.is_primary_key
    ORDER BY s.name, t.name, i.name
  `,

  // Obtener foreign keys
  foreignKeys: `
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
    ORDER BY s.name, t.name, fk.name
  `,

  // Obtener constraints (CHECK, UNIQUE)
  constraints: `
    SELECT
      s.name AS schema_name,
      t.name AS table_name,
      cc.name AS constraint_name,
      cc.type_desc AS constraint_type,
      cc.definition
    FROM sys.check_constraints cc
    INNER JOIN sys.tables t ON cc.parent_object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    UNION ALL
    SELECT
      s.name AS schema_name,
      t.name AS table_name,
      dc.name AS constraint_name,
      'DEFAULT' AS constraint_type,
      dc.definition
    FROM sys.default_constraints dc
    INNER JOIN sys.tables t ON dc.parent_object_id = t.object_id
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    ORDER BY schema_name, table_name, constraint_name
  `
};

// ═══════════════════════════════════════════════════════════════════════════
// FUNCIONES DE COMPARACIÓN
// ═══════════════════════════════════════════════════════════════════════════

async function getDbStructure(config) {
  let connection;
  try {
    console.log(`  Conectando a ${config.database}...`);
    connection = await createConnection(config);

    console.log(`  Obteniendo tablas...`);
    const tables = await executeQuery(connection, QUERIES.tables);

    console.log(`  Obteniendo columnas...`);
    const columns = await executeQuery(connection, QUERIES.columns);

    console.log(`  Obteniendo índices...`);
    const indexes = await executeQuery(connection, QUERIES.indexes);

    console.log(`  Obteniendo foreign keys...`);
    const foreignKeys = await executeQuery(connection, QUERIES.foreignKeys);

    console.log(`  Obteniendo constraints...`);
    const constraints = await executeQuery(connection, QUERIES.constraints);

    return {
      database: config.database,
      tables,
      columns,
      indexes,
      foreignKeys,
      constraints,
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
      constraints: [],
      error: error.message
    };
  } finally {
    if (connection) {
      await closeConnection(connection);
    }
  }
}

function compareStructures(source, target) {
  const differences = {
    tablesOnlyInSource: [],
    tablesOnlyInTarget: [],
    columnDifferences: [],
    indexDifferences: [],
    fkDifferences: [],
    constraintDifferences: []
  };

  // Crear mapas para búsqueda rápida
  const sourceTableMap = new Map(source.tables.map(t => [`${t.schema_name}.${t.table_name}`, t]));
  const targetTableMap = new Map(target.tables.map(t => [`${t.schema_name}.${t.table_name}`, t]));

  // Tablas solo en origen
  for (const [key, table] of sourceTableMap) {
    if (!targetTableMap.has(key)) {
      differences.tablesOnlyInSource.push({
        schema: table.schema_name,
        table: table.table_name,
        rowCount: table.row_count
      });
    }
  }

  // Tablas solo en destino
  for (const [key, table] of targetTableMap) {
    if (!sourceTableMap.has(key)) {
      differences.tablesOnlyInTarget.push({
        schema: table.schema_name,
        table: table.table_name,
        rowCount: table.row_count
      });
    }
  }

  // Comparar columnas de tablas comunes
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
    if (!targetTableMap.has(tableKey)) continue; // Tabla no existe en destino

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
      // Comparar tipo de dato
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
      // Comparar longitud
      if (srcCol.max_length !== tgtCol.max_length) {
        differences.columnDifferences.push({
          schema: srcCol.schema_name,
          table: srcCol.table_name,
          column: srcCol.column_name,
          type: 'LONGITUD_DIFERENTE',
          sourceValue: srcCol.max_length,
          targetValue: tgtCol.max_length
        });
      }
      // Comparar nullable
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
    if (!sourceTableMap.has(tableKey)) continue; // Tabla no existe en origen

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
// GENERACIÓN DE REPORTE EXCEL
// ═══════════════════════════════════════════════════════════════════════════

async function generateExcelReport(allResults) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = 'CDC TADA Sync';
  workbook.created = new Date();

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);

  // ═══════════════════════════════════════════════════════════════════════
  // HOJA 1: RESUMEN EJECUTIVO
  // ═══════════════════════════════════════════════════════════════════════
  const summarySheet = workbook.addWorksheet('Resumen Ejecutivo', {
    properties: { tabColor: { argb: '4472C4' } }
  });

  // Título principal
  summarySheet.mergeCells('A1:G1');
  const titleCell = summarySheet.getCell('A1');
  titleCell.value = 'REPORTE DE COMPARACIÓN DE ESTRUCTURAS DE BASES DE DATOS';
  titleCell.font = { bold: true, size: 16, color: { argb: 'FFFFFF' } };
  titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
  titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
  summarySheet.getRow(1).height = 30;

  // Información del reporte
  summarySheet.getCell('A3').value = 'Fecha de Generación:';
  summarySheet.getCell('B3').value = new Date().toLocaleString('es-MX');
  summarySheet.getCell('A4').value = 'Generado por:';
  summarySheet.getCell('B4').value = 'CDC TADA Sync - Script de Comparación';

  // Tabla de resumen
  summarySheet.getCell('A6').value = 'RESUMEN POR PAR DE BASES DE DATOS';
  summarySheet.getCell('A6').font = { bold: true, size: 12 };

  const summaryHeaders = ['Par de BD', 'BD Origen', 'BD Destino', 'Estado', 'Tablas Origen', 'Tablas Destino', 'Total Diferencias'];
  summarySheet.addRow([]);
  const headerRow = summarySheet.addRow(summaryHeaders);
  headerRow.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: 'FFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
    cell.border = {
      top: { style: 'thin' },
      left: { style: 'thin' },
      bottom: { style: 'thin' },
      right: { style: 'thin' }
    };
  });

  // Agregar datos de resumen
  for (const result of allResults) {
    const totalDiffs =
      result.differences.tablesOnlyInSource.length +
      result.differences.tablesOnlyInTarget.length +
      result.differences.columnDifferences.length +
      result.differences.indexDifferences.length;

    const status = result.sourceStructure.error || result.targetStructure.error
      ? 'ERROR DE CONEXIÓN'
      : totalDiffs === 0
        ? 'SIN DIFERENCIAS'
        : `${totalDiffs} DIFERENCIAS`;

    const row = summarySheet.addRow([
      result.pairName,
      result.sourceStructure.database,
      result.targetStructure.database,
      status,
      result.sourceStructure.tables.length,
      result.targetStructure.tables.length,
      totalDiffs
    ]);

    // Color según estado
    const statusCell = row.getCell(4);
    if (result.sourceStructure.error || result.targetStructure.error) {
      statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF6B6B' } };
    } else if (totalDiffs === 0) {
      statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '51CF66' } };
    } else {
      statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFC078' } };
    }

    row.eachCell((cell) => {
      cell.border = {
        top: { style: 'thin' },
        left: { style: 'thin' },
        bottom: { style: 'thin' },
        right: { style: 'thin' }
      };
    });
  }

  // Ajustar anchos
  summarySheet.columns = [
    { width: 25 }, { width: 25 }, { width: 25 }, { width: 20 },
    { width: 15 }, { width: 15 }, { width: 18 }
  ];

  // ═══════════════════════════════════════════════════════════════════════
  // SECCIÓN DE APROBACIÓN
  // ═══════════════════════════════════════════════════════════════════════
  const currentRow = summarySheet.lastRow.number + 3;
  summarySheet.getCell(`A${currentRow}`).value = 'SECCIÓN DE APROBACIÓN';
  summarySheet.getCell(`A${currentRow}`).font = { bold: true, size: 12 };

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

  // Notas
  const notesRow = summarySheet.lastRow.number + 2;
  summarySheet.getCell(`A${notesRow}`).value = 'Observaciones:';
  summarySheet.getCell(`A${notesRow}`).font = { bold: true };
  summarySheet.mergeCells(`A${notesRow + 1}:G${notesRow + 4}`);
  const notesCell = summarySheet.getCell(`A${notesRow + 1}`);
  notesCell.border = {
    top: { style: 'thin' },
    left: { style: 'thin' },
    bottom: { style: 'thin' },
    right: { style: 'thin' }
  };

  // ═══════════════════════════════════════════════════════════════════════
  // HOJAS POR CADA PAR DE BD
  // ═══════════════════════════════════════════════════════════════════════
  for (const result of allResults) {
    // Hoja de diferencias
    const diffSheet = workbook.addWorksheet(`${result.pairName} - Diferencias`, {
      properties: {
        tabColor: {
          argb: result.sourceStructure.error || result.targetStructure.error
            ? 'FF6B6B'
            : result.differences.tablesOnlyInSource.length +
              result.differences.tablesOnlyInTarget.length +
              result.differences.columnDifferences.length === 0
              ? '51CF66'
              : 'FFC078'
        }
      }
    });

    // Título
    diffSheet.mergeCells('A1:F1');
    const diffTitle = diffSheet.getCell('A1');
    diffTitle.value = `DIFERENCIAS: ${result.pairName}`;
    diffTitle.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
    diffTitle.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
    diffTitle.alignment = { horizontal: 'center' };
    diffSheet.getRow(1).height = 25;

    // Info
    diffSheet.getCell('A3').value = `Origen: ${result.sourceStructure.database}`;
    diffSheet.getCell('A4').value = `Destino: ${result.targetStructure.database}`;

    if (result.sourceStructure.error) {
      diffSheet.getCell('A6').value = `ERROR EN ORIGEN: ${result.sourceStructure.error}`;
      diffSheet.getCell('A6').font = { color: { argb: 'FF0000' }, bold: true };
    }
    if (result.targetStructure.error) {
      diffSheet.getCell('A7').value = `ERROR EN DESTINO: ${result.targetStructure.error}`;
      diffSheet.getCell('A7').font = { color: { argb: 'FF0000' }, bold: true };
    }

    let rowNum = 9;

    // Verificar si hay diferencias
    const totalDiffs =
      result.differences.tablesOnlyInSource.length +
      result.differences.tablesOnlyInTarget.length +
      result.differences.columnDifferences.length +
      result.differences.indexDifferences.length;

    if (totalDiffs === 0 && !result.sourceStructure.error && !result.targetStructure.error) {
      diffSheet.mergeCells(`A${rowNum}:F${rowNum}`);
      const noDiffCell = diffSheet.getCell(`A${rowNum}`);
      noDiffCell.value = 'SIN DIFERENCIAS - Las estructuras son idénticas';
      noDiffCell.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
      noDiffCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '51CF66' } };
      noDiffCell.alignment = { horizontal: 'center' };
      diffSheet.getRow(rowNum).height = 30;
    } else {
      // Tablas solo en origen
      if (result.differences.tablesOnlyInSource.length > 0) {
        diffSheet.getCell(`A${rowNum}`).value = 'TABLAS SOLO EN ORIGEN';
        diffSheet.getCell(`A${rowNum}`).font = { bold: true, size: 11 };
        diffSheet.getCell(`A${rowNum}`).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFE0B2' } };
        rowNum++;

        const headers = diffSheet.addRow(['Schema', 'Tabla', 'Registros']);
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
        diffSheet.getCell(`A${rowNum}`).value = 'TABLAS SOLO EN DESTINO';
        diffSheet.getCell(`A${rowNum}`).font = { bold: true, size: 11 };
        diffSheet.getCell(`A${rowNum}`).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'C8E6C9' } };
        rowNum++;

        const headers = diffSheet.addRow(['Schema', 'Tabla', 'Registros']);
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

      // Diferencias de columnas
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

      // Diferencias de índices
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

    // Ajustar anchos
    diffSheet.columns = [
      { width: 15 }, { width: 30 }, { width: 25 }, { width: 20 }, { width: 30 }, { width: 30 }
    ];

    // ═══════════════════════════════════════════════════════════════════
    // Hoja de estructura de origen
    // ═══════════════════════════════════════════════════════════════════
    if (!result.sourceStructure.error && result.sourceStructure.tables.length > 0) {
      const srcSheet = workbook.addWorksheet(`${result.pairName} - Origen`, {
        properties: { tabColor: { argb: 'B3E5FC' } }
      });

      srcSheet.mergeCells('A1:F1');
      const srcTitle = srcSheet.getCell('A1');
      srcTitle.value = `ESTRUCTURA: ${result.sourceStructure.database} (ORIGEN)`;
      srcTitle.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
      srcTitle.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '0277BD' } };
      srcTitle.alignment = { horizontal: 'center' };

      srcSheet.getCell('A3').value = `Total de tablas: ${result.sourceStructure.tables.length}`;

      // Tabla de estructura
      const srcHeaders = srcSheet.addRow(['Schema', 'Tabla', 'Columnas', 'Índices', 'Registros', 'Fecha Creación']);
      srcHeaders.eachCell((cell) => {
        cell.font = { bold: true, color: { argb: 'FFFFFF' } };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
      });

      // Agrupar columnas por tabla
      const columnsByTable = new Map();
      result.sourceStructure.columns.forEach(c => {
        const key = `${c.schema_name}.${c.table_name}`;
        if (!columnsByTable.has(key)) columnsByTable.set(key, []);
        columnsByTable.get(key).push(c);
      });

      const indexesByTable = new Map();
      result.sourceStructure.indexes.forEach(i => {
        const key = `${i.schema_name}.${i.table_name}`;
        if (!indexesByTable.has(key)) indexesByTable.set(key, []);
        indexesByTable.get(key).push(i);
      });

      for (const table of result.sourceStructure.tables) {
        const key = `${table.schema_name}.${table.table_name}`;
        const cols = columnsByTable.get(key) || [];
        const idxs = indexesByTable.get(key) || [];
        srcSheet.addRow([
          table.schema_name,
          table.table_name,
          cols.length,
          idxs.length,
          table.row_count,
          table.create_date ? new Date(table.create_date).toLocaleDateString('es-MX') : ''
        ]);
      }

      srcSheet.columns = [
        { width: 12 }, { width: 35 }, { width: 12 }, { width: 12 }, { width: 15 }, { width: 18 }
      ];
    }

    // ═══════════════════════════════════════════════════════════════════
    // Hoja de estructura de destino
    // ═══════════════════════════════════════════════════════════════════
    if (!result.targetStructure.error && result.targetStructure.tables.length > 0) {
      const tgtSheet = workbook.addWorksheet(`${result.pairName} - Destino`, {
        properties: { tabColor: { argb: 'C8E6C9' } }
      });

      tgtSheet.mergeCells('A1:F1');
      const tgtTitle = tgtSheet.getCell('A1');
      tgtTitle.value = `ESTRUCTURA: ${result.targetStructure.database} (DESTINO)`;
      tgtTitle.font = { bold: true, size: 14, color: { argb: 'FFFFFF' } };
      tgtTitle.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '2E7D32' } };
      tgtTitle.alignment = { horizontal: 'center' };

      tgtSheet.getCell('A3').value = `Total de tablas: ${result.targetStructure.tables.length}`;

      const tgtHeaders = tgtSheet.addRow(['Schema', 'Tabla', 'Columnas', 'Índices', 'Registros', 'Fecha Creación']);
      tgtHeaders.eachCell((cell) => {
        cell.font = { bold: true, color: { argb: 'FFFFFF' } };
        cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
      });

      const columnsByTable = new Map();
      result.targetStructure.columns.forEach(c => {
        const key = `${c.schema_name}.${c.table_name}`;
        if (!columnsByTable.has(key)) columnsByTable.set(key, []);
        columnsByTable.get(key).push(c);
      });

      const indexesByTable = new Map();
      result.targetStructure.indexes.forEach(i => {
        const key = `${i.schema_name}.${i.table_name}`;
        if (!indexesByTable.has(key)) indexesByTable.set(key, []);
        indexesByTable.get(key).push(i);
      });

      for (const table of result.targetStructure.tables) {
        const key = `${table.schema_name}.${table.table_name}`;
        const cols = columnsByTable.get(key) || [];
        const idxs = indexesByTable.get(key) || [];
        tgtSheet.addRow([
          table.schema_name,
          table.table_name,
          cols.length,
          idxs.length,
          table.row_count,
          table.create_date ? new Date(table.create_date).toLocaleDateString('es-MX') : ''
        ]);
      }

      tgtSheet.columns = [
        { width: 12 }, { width: 35 }, { width: 12 }, { width: 12 }, { width: 15 }, { width: 18 }
      ];
    }
  }

  // Guardar archivo
  const reportsDir = path.join(__dirname, '..', 'reports');
  if (!fs.existsSync(reportsDir)) {
    fs.mkdirSync(reportsDir, { recursive: true });
  }

  const fileName = `db-structure-comparison-${timestamp}.xlsx`;
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
  console.log('║     COMPARACIÓN DE ESTRUCTURAS DE BASES DE DATOS                 ║');
  console.log('║                    CDC TADA Sync                                 ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');

  const allResults = [];

  for (const pair of DB_PAIRS) {
    console.log(`\n═══ Procesando: ${pair.name} ═══`);
    console.log(`Origen: ${pair.source.database}`);
    console.log(`Destino: ${pair.target.database}`);
    console.log('');

    // Obtener estructura de origen
    console.log('Obteniendo estructura de ORIGEN:');
    const sourceStructure = await getDbStructure(pair.source);

    // Obtener estructura de destino
    console.log('\nObteniendo estructura de DESTINO:');
    const targetStructure = await getDbStructure(pair.target);

    // Comparar
    console.log('\nComparando estructuras...');
    const differences = compareStructures(sourceStructure, targetStructure);

    const totalDiffs =
      differences.tablesOnlyInSource.length +
      differences.tablesOnlyInTarget.length +
      differences.columnDifferences.length +
      differences.indexDifferences.length;

    if (sourceStructure.error || targetStructure.error) {
      console.log(`  ❌ ERROR DE CONEXIÓN`);
    } else if (totalDiffs === 0) {
      console.log(`  ✅ SIN DIFERENCIAS`);
    } else {
      console.log(`  ⚠️  ${totalDiffs} diferencias encontradas:`);
      if (differences.tablesOnlyInSource.length > 0) {
        console.log(`     - ${differences.tablesOnlyInSource.length} tablas solo en origen`);
      }
      if (differences.tablesOnlyInTarget.length > 0) {
        console.log(`     - ${differences.tablesOnlyInTarget.length} tablas solo en destino`);
      }
      if (differences.columnDifferences.length > 0) {
        console.log(`     - ${differences.columnDifferences.length} diferencias en columnas`);
      }
      if (differences.indexDifferences.length > 0) {
        console.log(`     - ${differences.indexDifferences.length} diferencias en índices`);
      }
    }

    allResults.push({
      pairName: pair.name,
      sourceStructure,
      targetStructure,
      differences
    });
  }

  // Generar reporte Excel
  console.log('\n═══ Generando reporte Excel ═══');
  const reportPath = await generateExcelReport(allResults);
  console.log(`✅ Reporte generado: ${reportPath}`);

  // Generar también JSON para respaldo
  const jsonPath = reportPath.replace('.xlsx', '.json');
  fs.writeFileSync(jsonPath, JSON.stringify(allResults, null, 2));
  console.log(`✅ Respaldo JSON: ${jsonPath}`);

  console.log('\n╔══════════════════════════════════════════════════════════════════╗');
  console.log('║                    PROCESO COMPLETADO                            ║');
  console.log('╚══════════════════════════════════════════════════════════════════╝');
  console.log('');
  console.log('CRITERIOS DE ACEPTACIÓN:');
  console.log('  ✅ Reporte en formato Excel generado');
  console.log('  ✅ Incluye estructuras de cada base de datos');
  console.log('  ✅ Marca "SIN DIFERENCIAS" si el par coincide');
  console.log('  ✅ Sección de aprobación incluida');
  console.log('');
  console.log('SIGUIENTE PASO:');
  console.log('  → Revisar el reporte con al menos un miembro del equipo');
  console.log('  → Firmar la sección de aprobación antes de proceder');
  console.log('');
}

// Ejecutar
main().catch(console.error);
