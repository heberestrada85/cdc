/**
 * compare-db-structures.js
 *
 * Utilería para comparar estructuras de bases de datos SQL Server
 * y generar un reporte consolidado de diferencias.
 *
 * Uso: node utils_scripts/compare-db-structures.js
 *
 * Genera:
 *   - reports/db-comparison-YYYY-MM-DD_HH-mm-ss.xlsx (Excel)
 *   - reports/db-comparison-YYYY-MM-DD_HH-mm-ss.json (JSON detallado)
 */

require('dotenv').config();
const { Connection, Request } = require('tedious');
const ExcelJS = require('exceljs');
const fs = require('fs');
const path = require('path');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURACIÓN DE CONEXIÓN (usa mismas credenciales que el CDC sync)
// ═══════════════════════════════════════════════════════════════════════════
const DB_SERVER = process.env.SOURCE_DB_SERVER || '172.17.0.247';
const DB_USER = process.env.SOURCE_DB_USER || 'haestr4d4';
const DB_PASSWORD = "HCq$9ynmF@V!%04P0u6#"; // Password hardcodeado como en index.js

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURACIÓN DE PARES DE BASES DE DATOS A COMPARAR
// ═══════════════════════════════════════════════════════════════════════════
const DB_PAIRS = [
  {
    name: 'TadaNomina',
    source: {
      server: DB_SERVER,
      database: 'TadaNomina',
      user: DB_USER,
      password: DB_PASSWORD
    },
    target: {
      server: DB_SERVER,
      database: 'TadaNomina-2.0',
      user: DB_USER,
      password: DB_PASSWORD
    }
  // },
  // {
  //   name: 'TadaModuloSeguridad',
  //   source: {
  //     server: DB_SERVER,
  //     database: 'TadaModuloSeguridad',
  //     user: DB_USER,
  //     password: DB_PASSWORD
  //   },
  //   target: {
  //     server: DB_SERVER,
  //     database: 'TadaModuloSeguridad-2.0',
  //     user: DB_USER,
  //     password: DB_PASSWORD
  //   }
  // },
  // {
  //   name: 'TadaChecador',
  //   source: {
  //     server: DB_SERVER,
  //     database: 'TadaChecador',
  //     user: DB_USER,
  //     password: DB_PASSWORD
  //   },
  //   target: {
  //     server: DB_SERVER,
  //     database: 'TadaChecador-2.0',
  //     user: DB_USER,
  //     password: DB_PASSWORD
  //   }
  // }
];

// Tablas específicas de TADÁ 2.0 que NO deben compararse (nueva estructura de personas)
const TABLES_TO_EXCLUDE = [
  // Estructura nueva de personas en TADÁ 2.0
  'Personas',
  'PersonasDirecciones',
  'PersonasContactos',
  'PersonasDocumentos',
  'PersonasBeneficiarios',
  'PersonasEmergencia',
  'PersonasFamiliares',
  'MaestroPersonaDatosTada20',
  'personasTada20',
  // Tablas de sistema/temporales
  'sysdiagrams',
  'systranschemas',
  '__EFMigrationsHistory',
  // Tablas CDC
  'CDC_SyncLog'
];

// ═══════════════════════════════════════════════════════════════════════════
// CLASE PRINCIPAL
// ═══════════════════════════════════════════════════════════════════════════
class DatabaseStructureComparator {
  constructor() {
    this.results = [];
    this.timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  }

  /**
   * Crea una conexión a SQL Server
   */
  createConnection(config) {
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
          encrypt: false,
          trustServerCertificate: true,
          connectTimeout: 30000,
          requestTimeout: 120000
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

  /**
   * Ejecuta una query y devuelve los resultados
   */
  executeQuery(connection, sql) {
    return new Promise((resolve, reject) => {
      const rows = [];
      const request = new Request(sql, (err) => {
        if (err) reject(err);
        else resolve(rows);
      });

      request.on('row', (columns) => {
        const row = {};
        columns.forEach(col => {
          row[col.metadata.colName] = col.value;
        });
        rows.push(row);
      });

      connection.execSql(request);
    });
  }

  /**
   * Obtiene todas las tablas de una base de datos
   */
  async getTables(connection) {
    const sql = `
      SELECT
        t.TABLE_SCHEMA as [schema],
        t.TABLE_NAME as [name],
        (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS c
         WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME) as column_count
      FROM INFORMATION_SCHEMA.TABLES t
      WHERE t.TABLE_TYPE = 'BASE TABLE'
        AND t.TABLE_SCHEMA NOT IN ('cdc', 'sys')
      ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
    `;
    return await this.executeQuery(connection, sql);
  }

  /**
   * Obtiene las columnas de una tabla con detalles completos
   */
  async getTableColumns(connection, schema, tableName) {
    const sql = `
      SELECT
        c.COLUMN_NAME as column_name,
        c.DATA_TYPE as data_type,
        c.CHARACTER_MAXIMUM_LENGTH as max_length,
        c.NUMERIC_PRECISION as precision,
        c.NUMERIC_SCALE as scale,
        c.IS_NULLABLE as is_nullable,
        c.COLUMN_DEFAULT as default_value,
        CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END as is_identity,
        c.ORDINAL_POSITION as ordinal
      FROM INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN sys.columns sc
        ON sc.object_id = OBJECT_ID('${schema}.${tableName}')
        AND sc.name = c.COLUMN_NAME
      LEFT JOIN sys.identity_columns ic
        ON ic.object_id = sc.object_id AND ic.column_id = sc.column_id
      WHERE c.TABLE_SCHEMA = '${schema}' AND c.TABLE_NAME = '${tableName}'
      ORDER BY c.ORDINAL_POSITION
    `;
    return await this.executeQuery(connection, sql);
  }

  /**
   * Obtiene los índices de una tabla
   */
  async getTableIndexes(connection, schema, tableName) {
    const sql = `
      SELECT
        i.name as index_name,
        i.type_desc as index_type,
        i.is_unique,
        i.is_primary_key,
        STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) as columns
      FROM sys.indexes i
      INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
      INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
      WHERE i.object_id = OBJECT_ID('${schema}.${tableName}')
        AND i.name IS NOT NULL
      GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
      ORDER BY i.name
    `;
    try {
      return await this.executeQuery(connection, sql);
    } catch (e) {
      return []; // Algunas tablas pueden no tener índices
    }
  }

  /**
   * Obtiene las foreign keys de una tabla
   */
  async getTableForeignKeys(connection, schema, tableName) {
    const sql = `
      SELECT
        fk.name as fk_name,
        OBJECT_NAME(fk.parent_object_id) as table_name,
        COL_NAME(fkc.parent_object_id, fkc.parent_column_id) as column_name,
        OBJECT_SCHEMA_NAME(fk.referenced_object_id) as ref_schema,
        OBJECT_NAME(fk.referenced_object_id) as ref_table,
        COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) as ref_column
      FROM sys.foreign_keys fk
      INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
      WHERE fk.parent_object_id = OBJECT_ID('${schema}.${tableName}')
      ORDER BY fk.name
    `;
    try {
      return await this.executeQuery(connection, sql);
    } catch (e) {
      return [];
    }
  }

  /**
   * Compara dos conjuntos de columnas
   */
  compareColumns(sourceColumns, targetColumns, tableName) {
    const differences = [];
    const sourceMap = new Map(sourceColumns.map(c => [c.column_name, c]));
    const targetMap = new Map(targetColumns.map(c => [c.column_name, c]));

    // Columnas solo en origen
    for (const [name, col] of sourceMap) {
      if (!targetMap.has(name)) {
        differences.push({
          type: 'COLUMN_MISSING_IN_TARGET',
          table: tableName,
          column: name,
          detail: `Columna '${name}' existe en origen pero NO en destino`,
          sourceValue: this.formatColumnType(col),
          targetValue: '-'
        });
      }
    }

    // Columnas solo en destino
    for (const [name, col] of targetMap) {
      if (!sourceMap.has(name)) {
        differences.push({
          type: 'COLUMN_MISSING_IN_SOURCE',
          table: tableName,
          column: name,
          detail: `Columna '${name}' existe en destino pero NO en origen`,
          sourceValue: '-',
          targetValue: this.formatColumnType(col)
        });
      }
    }

    // Columnas con diferencias
    for (const [name, sourceCol] of sourceMap) {
      const targetCol = targetMap.get(name);
      if (!targetCol) continue;

      // Comparar tipo de dato
      if (sourceCol.data_type !== targetCol.data_type) {
        differences.push({
          type: 'COLUMN_TYPE_MISMATCH',
          table: tableName,
          column: name,
          detail: `Tipo de dato diferente`,
          sourceValue: sourceCol.data_type,
          targetValue: targetCol.data_type
        });
      }

      // Comparar longitud
      if (sourceCol.max_length !== targetCol.max_length) {
        differences.push({
          type: 'COLUMN_LENGTH_MISMATCH',
          table: tableName,
          column: name,
          detail: `Longitud diferente`,
          sourceValue: sourceCol.max_length || 'MAX',
          targetValue: targetCol.max_length || 'MAX'
        });
      }

      // Comparar nullable
      if (sourceCol.is_nullable !== targetCol.is_nullable) {
        differences.push({
          type: 'COLUMN_NULLABLE_MISMATCH',
          table: tableName,
          column: name,
          detail: `Nullable diferente`,
          sourceValue: sourceCol.is_nullable,
          targetValue: targetCol.is_nullable
        });
      }

      // Comparar identity
      if (sourceCol.is_identity !== targetCol.is_identity) {
        differences.push({
          type: 'COLUMN_IDENTITY_MISMATCH',
          table: tableName,
          column: name,
          detail: `Identity diferente`,
          sourceValue: sourceCol.is_identity ? 'SI' : 'NO',
          targetValue: targetCol.is_identity ? 'SI' : 'NO'
        });
      }
    }

    return differences;
  }

  /**
   * Formatea el tipo de columna para mostrar
   */
  formatColumnType(col) {
    let type = col.data_type.toUpperCase();
    if (['varchar', 'nvarchar', 'char', 'nchar', 'varbinary', 'binary'].includes(col.data_type.toLowerCase())) {
      type += `(${col.max_length > 0 ? col.max_length : 'MAX'})`;
    } else if (['decimal', 'numeric'].includes(col.data_type.toLowerCase())) {
      type += `(${col.precision}, ${col.scale})`;
    }
    if (col.is_identity) type += ' IDENTITY';
    type += col.is_nullable === 'YES' ? ' NULL' : ' NOT NULL';
    return type;
  }

  /**
   * Compara índices entre origen y destino
   */
  compareIndexes(sourceIndexes, targetIndexes, tableName) {
    const differences = [];
    const sourceMap = new Map(sourceIndexes.map(i => [i.index_name, i]));
    const targetMap = new Map(targetIndexes.map(i => [i.index_name, i]));

    for (const [name, idx] of sourceMap) {
      if (!targetMap.has(name)) {
        differences.push({
          type: 'INDEX_MISSING_IN_TARGET',
          table: tableName,
          column: name,
          detail: `Índice '${name}' falta en destino`,
          sourceValue: `${idx.index_type} (${idx.columns})`,
          targetValue: '-'
        });
      }
    }

    for (const [name, idx] of targetMap) {
      if (!sourceMap.has(name)) {
        differences.push({
          type: 'INDEX_MISSING_IN_SOURCE',
          table: tableName,
          column: name,
          detail: `Índice '${name}' falta en origen`,
          sourceValue: '-',
          targetValue: `${idx.index_type} (${idx.columns})`
        });
      }
    }

    return differences;
  }

  /**
   * Compara foreign keys
   */
  compareForeignKeys(sourceFKs, targetFKs, tableName) {
    const differences = [];
    const sourceMap = new Map(sourceFKs.map(f => [f.fk_name, f]));
    const targetMap = new Map(targetFKs.map(f => [f.fk_name, f]));

    for (const [name, fk] of sourceMap) {
      if (!targetMap.has(name)) {
        differences.push({
          type: 'FK_MISSING_IN_TARGET',
          table: tableName,
          column: name,
          detail: `FK '${name}' falta en destino`,
          sourceValue: `${fk.column_name} -> ${fk.ref_schema}.${fk.ref_table}.${fk.ref_column}`,
          targetValue: '-'
        });
      }
    }

    for (const [name, fk] of targetMap) {
      if (!sourceMap.has(name)) {
        differences.push({
          type: 'FK_MISSING_IN_SOURCE',
          table: tableName,
          column: name,
          detail: `FK '${name}' falta en origen`,
          sourceValue: '-',
          targetValue: `${fk.column_name} -> ${fk.ref_schema}.${fk.ref_table}.${fk.ref_column}`
        });
      }
    }

    return differences;
  }

  /**
   * Compara un par de bases de datos
   */
  async comparePair(pair) {
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`Comparando: ${pair.source.database} vs ${pair.target.database}`);
    console.log('═'.repeat(60));

    let sourceConn, targetConn;
    const pairResult = {
      pairName: pair.name,
      sourceDb: pair.source.database,
      targetDb: pair.target.database,
      timestamp: new Date().toISOString(),
      tables: {
        onlyInSource: [],
        onlyInTarget: [],
        inBoth: []
      },
      differences: [],
      summary: {
        totalTables: { source: 0, target: 0, common: 0 },
        totalDifferences: 0,
        hasDifferences: false
      }
    };

    try {
      // Conectar a ambas bases
      console.log(`  Conectando a ${pair.source.database}...`);
      sourceConn = await this.createConnection(pair.source);

      console.log(`  Conectando a ${pair.target.database}...`);
      targetConn = await this.createConnection(pair.target);

      // Obtener tablas
      console.log('  Obteniendo lista de tablas...');
      const sourceTables = await this.getTables(sourceConn);
      const targetTables = await this.getTables(targetConn);

      // Filtrar tablas excluidas
      const filterTables = (tables) => tables.filter(t =>
        !TABLES_TO_EXCLUDE.includes(t.name) &&
        !t.name.startsWith('cdc_') &&
        !t.name.startsWith('tmp_') &&
        !t.name.startsWith('bak_')
      );

      const filteredSourceTables = filterTables(sourceTables);
      const filteredTargetTables = filterTables(targetTables);

      const sourceTableNames = new Set(filteredSourceTables.map(t => `${t.schema}.${t.name}`));
      const targetTableNames = new Set(filteredTargetTables.map(t => `${t.schema}.${t.name}`));

      // Tablas solo en origen
      for (const t of filteredSourceTables) {
        const fullName = `${t.schema}.${t.name}`;
        if (!targetTableNames.has(fullName)) {
          pairResult.tables.onlyInSource.push({ schema: t.schema, name: t.name, columns: t.column_count });
          pairResult.differences.push({
            type: 'TABLE_MISSING_IN_TARGET',
            table: fullName,
            column: '-',
            detail: `Tabla completa falta en destino (${t.column_count} columnas)`,
            sourceValue: 'EXISTE',
            targetValue: '-'
          });
        }
      }

      // Tablas solo en destino
      for (const t of filteredTargetTables) {
        const fullName = `${t.schema}.${t.name}`;
        if (!sourceTableNames.has(fullName)) {
          pairResult.tables.onlyInTarget.push({ schema: t.schema, name: t.name, columns: t.column_count });
          pairResult.differences.push({
            type: 'TABLE_MISSING_IN_SOURCE',
            table: fullName,
            column: '-',
            detail: `Tabla completa falta en origen (${t.column_count} columnas)`,
            sourceValue: '-',
            targetValue: 'EXISTE'
          });
        }
      }

      // Comparar tablas comunes
      const commonTables = filteredSourceTables.filter(t =>
        targetTableNames.has(`${t.schema}.${t.name}`)
      );

      console.log(`  Comparando ${commonTables.length} tablas comunes...`);

      for (let i = 0; i < commonTables.length; i++) {
        const table = commonTables[i];
        const fullName = `${table.schema}.${table.name}`;

        if ((i + 1) % 50 === 0) {
          console.log(`    Progreso: ${i + 1}/${commonTables.length} tablas`);
        }

        // Obtener estructura detallada
        const [sourceColumns, targetColumns] = await Promise.all([
          this.getTableColumns(sourceConn, table.schema, table.name),
          this.getTableColumns(targetConn, table.schema, table.name)
        ]);

        const [sourceIndexes, targetIndexes] = await Promise.all([
          this.getTableIndexes(sourceConn, table.schema, table.name),
          this.getTableIndexes(targetConn, table.schema, table.name)
        ]);

        const [sourceFKs, targetFKs] = await Promise.all([
          this.getTableForeignKeys(sourceConn, table.schema, table.name),
          this.getTableForeignKeys(targetConn, table.schema, table.name)
        ]);

        // Comparar
        const columnDiffs = this.compareColumns(sourceColumns, targetColumns, fullName);
        const indexDiffs = this.compareIndexes(sourceIndexes, targetIndexes, fullName);
        const fkDiffs = this.compareForeignKeys(sourceFKs, targetFKs, fullName);

        const tableDiffs = [...columnDiffs, ...indexDiffs, ...fkDiffs];

        pairResult.tables.inBoth.push({
          schema: table.schema,
          name: table.name,
          sourceColumns: sourceColumns.length,
          targetColumns: targetColumns.length,
          differences: tableDiffs.length
        });

        pairResult.differences.push(...tableDiffs);
      }

      // Resumen
      pairResult.summary.totalTables.source = filteredSourceTables.length;
      pairResult.summary.totalTables.target = filteredTargetTables.length;
      pairResult.summary.totalTables.common = commonTables.length;
      pairResult.summary.totalDifferences = pairResult.differences.length;
      pairResult.summary.hasDifferences = pairResult.differences.length > 0;

      console.log(`\n  Resumen para ${pair.name}:`);
      console.log(`    - Tablas en origen: ${pairResult.summary.totalTables.source}`);
      console.log(`    - Tablas en destino: ${pairResult.summary.totalTables.target}`);
      console.log(`    - Tablas comunes: ${pairResult.summary.totalTables.common}`);
      console.log(`    - Solo en origen: ${pairResult.tables.onlyInSource.length}`);
      console.log(`    - Solo en destino: ${pairResult.tables.onlyInTarget.length}`);
      console.log(`    - Total diferencias: ${pairResult.summary.totalDifferences}`);

    } catch (error) {
      console.error(`  ERROR: ${error.message}`);
      pairResult.error = error.message;
    } finally {
      if (sourceConn) sourceConn.close();
      if (targetConn) targetConn.close();
    }

    return pairResult;
  }

  /**
   * Genera reporte Excel
   */
  async generateExcelReport(results) {
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'CDC Database Comparator';
    workbook.created = new Date();

    // ═══════════════════════════════════════════════════════════════════════
    // HOJA 1: RESUMEN EJECUTIVO
    // ═══════════════════════════════════════════════════════════════════════
    const summarySheet = workbook.addWorksheet('Resumen Ejecutivo');

    // Título
    summarySheet.mergeCells('A1:G1');
    summarySheet.getCell('A1').value = 'REPORTE DE COMPARACIÓN DE ESTRUCTURAS DE BASES DE DATOS';
    summarySheet.getCell('A1').font = { bold: true, size: 16 };
    summarySheet.getCell('A1').alignment = { horizontal: 'center' };

    summarySheet.mergeCells('A2:G2');
    summarySheet.getCell('A2').value = `Generado: ${new Date().toLocaleString('es-MX')}`;
    summarySheet.getCell('A2').alignment = { horizontal: 'center' };

    // Encabezados
    const summaryHeaders = ['Par de BDs', 'BD Origen', 'BD Destino', 'Tablas Origen', 'Tablas Destino', 'Diferencias', 'Estado'];
    summarySheet.addRow([]);
    const headerRow = summarySheet.addRow(summaryHeaders);
    headerRow.eachCell(cell => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
      cell.font = { bold: true, color: { argb: 'FFFFFF' } };
      cell.border = { top: { style: 'thin' }, bottom: { style: 'thin' }, left: { style: 'thin' }, right: { style: 'thin' } };
    });

    // Datos
    for (const result of results) {
      const status = result.error ? 'ERROR' : (result.summary.hasDifferences ? 'CON DIFERENCIAS' : 'SIN DIFERENCIAS');
      const row = summarySheet.addRow([
        result.pairName,
        result.sourceDb,
        result.targetDb,
        result.summary.totalTables.source || 0,
        result.summary.totalTables.target || 0,
        result.summary.totalDifferences || 0,
        status
      ]);

      // Colorear estado
      const statusCell = row.getCell(7);
      if (status === 'SIN DIFERENCIAS') {
        statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '92D050' } };
      } else if (status === 'CON DIFERENCIAS') {
        statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFC000' } };
      } else {
        statusCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF0000' } };
        statusCell.font = { color: { argb: 'FFFFFF' } };
      }
    }

    summarySheet.columns.forEach(col => col.width = 20);

    // ═══════════════════════════════════════════════════════════════════════
    // HOJAS POR CADA PAR DE BDs
    // ═══════════════════════════════════════════════════════════════════════
    for (const result of results) {
      if (result.error) continue;

      // Hoja de diferencias detalladas
      const diffSheet = workbook.addWorksheet(`${result.pairName} - Diferencias`);

      diffSheet.mergeCells('A1:F1');
      diffSheet.getCell('A1').value = `Diferencias: ${result.sourceDb} vs ${result.targetDb}`;
      diffSheet.getCell('A1').font = { bold: true, size: 14 };

      if (result.differences.length === 0) {
        diffSheet.addRow([]);
        diffSheet.addRow(['SIN DIFERENCIAS - Las estructuras son idénticas']);
        diffSheet.getCell('A3').font = { bold: true, color: { argb: '00AA00' } };
      } else {
        const diffHeaders = ['Tipo', 'Tabla', 'Columna/Objeto', 'Detalle', 'Valor Origen', 'Valor Destino'];
        diffSheet.addRow([]);
        const hRow = diffSheet.addRow(diffHeaders);
        hRow.eachCell(cell => {
          cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '4472C4' } };
          cell.font = { bold: true, color: { argb: 'FFFFFF' } };
        });

        // Agrupar por tipo
        const grouped = {};
        for (const diff of result.differences) {
          if (!grouped[diff.type]) grouped[diff.type] = [];
          grouped[diff.type].push(diff);
        }

        for (const [type, diffs] of Object.entries(grouped)) {
          for (const diff of diffs) {
            const row = diffSheet.addRow([
              type,
              diff.table,
              diff.column,
              diff.detail,
              diff.sourceValue,
              diff.targetValue
            ]);

            // Colorear por tipo
            const typeCell = row.getCell(1);
            if (type.includes('MISSING_IN_TARGET')) {
              typeCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFCCCC' } };
            } else if (type.includes('MISSING_IN_SOURCE')) {
              typeCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'CCFFCC' } };
            } else if (type.includes('MISMATCH')) {
              typeCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFCC' } };
            }
          }
        }
      }

      diffSheet.columns = [
        { width: 25 }, { width: 35 }, { width: 25 }, { width: 40 }, { width: 25 }, { width: 25 }
      ];

      // Hoja de tablas
      const tablesSheet = workbook.addWorksheet(`${result.pairName} - Tablas`);

      tablesSheet.mergeCells('A1:D1');
      tablesSheet.getCell('A1').value = `Listado de Tablas: ${result.sourceDb}`;
      tablesSheet.getCell('A1').font = { bold: true, size: 14 };

      // Tablas solo en origen
      tablesSheet.addRow([]);
      tablesSheet.addRow(['TABLAS SOLO EN ORIGEN']);
      tablesSheet.lastRow.font = { bold: true };
      tablesSheet.addRow(['Schema', 'Tabla', 'Columnas']);
      if (result.tables.onlyInSource.length === 0) {
        tablesSheet.addRow(['-', 'Ninguna', '-']);
      } else {
        for (const t of result.tables.onlyInSource) {
          tablesSheet.addRow([t.schema, t.name, t.columns]);
        }
      }

      // Tablas solo en destino
      tablesSheet.addRow([]);
      tablesSheet.addRow(['TABLAS SOLO EN DESTINO']);
      tablesSheet.lastRow.font = { bold: true };
      tablesSheet.addRow(['Schema', 'Tabla', 'Columnas']);
      if (result.tables.onlyInTarget.length === 0) {
        tablesSheet.addRow(['-', 'Ninguna', '-']);
      } else {
        for (const t of result.tables.onlyInTarget) {
          tablesSheet.addRow([t.schema, t.name, t.columns]);
        }
      }

      // Tablas comunes con diferencias
      tablesSheet.addRow([]);
      tablesSheet.addRow(['TABLAS COMUNES CON DIFERENCIAS']);
      tablesSheet.lastRow.font = { bold: true };
      tablesSheet.addRow(['Schema', 'Tabla', 'Cols Origen', 'Cols Destino', 'Diferencias']);
      const tablesWithDiffs = result.tables.inBoth.filter(t => t.differences > 0);
      if (tablesWithDiffs.length === 0) {
        tablesSheet.addRow(['-', 'Ninguna', '-', '-', '-']);
      } else {
        for (const t of tablesWithDiffs) {
          tablesSheet.addRow([t.schema, t.name, t.sourceColumns, t.targetColumns, t.differences]);
        }
      }

      tablesSheet.columns = [{ width: 15 }, { width: 40 }, { width: 15 }, { width: 15 }, { width: 15 }];
    }

    return workbook;
  }

  /**
   * Ejecuta la comparación completa
   */
  async run() {
    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log('║     COMPARADOR DE ESTRUCTURAS DE BASES DE DATOS                ║');
    console.log('║     TADÁ - Análisis de Diferencias QA                          ║');
    console.log('╚════════════════════════════════════════════════════════════════╝\n');

    const results = [];

    for (const pair of DB_PAIRS) {
      const result = await this.comparePair(pair);
      results.push(result);
    }

    // Crear carpeta de reportes
    const reportsDir = path.join(__dirname, '..', 'reports');
    if (!fs.existsSync(reportsDir)) {
      fs.mkdirSync(reportsDir, { recursive: true });
    }

    // Guardar JSON detallado
    const jsonPath = path.join(reportsDir, `db-comparison-${this.timestamp}.json`);
    fs.writeFileSync(jsonPath, JSON.stringify(results, null, 2));
    console.log(`\n📄 JSON guardado: ${jsonPath}`);

    // Generar Excel
    console.log('\nGenerando reporte Excel...');
    const workbook = await this.generateExcelReport(results);
    const excelPath = path.join(reportsDir, `db-comparison-${this.timestamp}.xlsx`);
    await workbook.xlsx.writeFile(excelPath);
    console.log(`📊 Excel guardado: ${excelPath}`);

    // Resumen final
    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log('║                    RESUMEN FINAL                               ║');
    console.log('╠════════════════════════════════════════════════════════════════╣');

    let totalDiffs = 0;
    for (const r of results) {
      const status = r.error ? '❌ ERROR' : (r.summary.hasDifferences ? '⚠️  CON DIFERENCIAS' : '✅ SIN DIFERENCIAS');
      console.log(`║  ${r.pairName.padEnd(25)} ${status.padEnd(25)} ║`);
      totalDiffs += r.summary?.totalDifferences || 0;
    }

    console.log('╠════════════════════════════════════════════════════════════════╣');
    console.log(`║  Total diferencias encontradas: ${totalDiffs.toString().padEnd(28)} ║`);
    console.log('╚════════════════════════════════════════════════════════════════╝\n');

    return results;
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// EJECUCIÓN
// ═══════════════════════════════════════════════════════════════════════════
const comparator = new DatabaseStructureComparator();
comparator.run()
  .then(() => {
    console.log('Comparación completada exitosamente.\n');
    process.exit(0);
  })
  .catch(err => {
    console.error('Error fatal:', err);
    process.exit(1);
  });
