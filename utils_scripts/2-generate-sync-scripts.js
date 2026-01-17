/**
 * 2-generate-sync-scripts.js
 *
 * Genera scripts SQL para sincronizar estructuras entre bases de datos.
 * Solo cambios estructurales (DDL), NO migración de datos.
 *
 * Uso: node utils_scripts/2-generate-sync-scripts.js
 *
 * Genera:
 *   - scripts/sync-[DB_NAME]-YYYY-MM-DD_HH-mm-ss.sql (Script ejecutable)
 *   - reports/sync-changes-YYYY-MM-DD_HH-mm-ss.json (Log de cambios)
 *   - reports/sync-verification-YYYY-MM-DD_HH-mm-ss.xlsx (Verificación)
 */

require('dotenv').config();
const { Connection, Request } = require('tedious');
const ExcelJS = require('exceljs');
const fs = require('fs');
const path = require('path');

// ═══════════════════════════════════════════════════════════════════════════
// CONFIGURACIÓN
// ═══════════════════════════════════════════════════════════════════════════
const DB_SERVER = process.env.SOURCE_DB_SERVER || '172.17.0.247';
const DB_USER = process.env.SOURCE_DB_USER || 'haestr4d4';
const DB_PASSWORD = "HCq$9ynmF@V!%04P0u6#";

const DB_PAIRS = [
  {
    name: 'TadaNomina',
    source: { server: DB_SERVER, database: 'TadaNomina', user: DB_USER, password: DB_PASSWORD },
    target: { server: DB_SERVER, database: 'TadaNomina-2.0', user: DB_USER, password: DB_PASSWORD }
  },
  // {
  //   name: 'TadaModuloSeguridad',
  //   source: { server: DB_SERVER, database: 'TadaModuloSeguridad', user: DB_USER, password: DB_PASSWORD },
  //   target: { server: DB_SERVER, database: 'TadaModuloSeguridad-2.0', user: DB_USER, password: DB_PASSWORD }
  // },
  // {
  //   name: 'TadaChecador',
  //   source: { server: DB_SERVER, database: 'TadaChecador', user: DB_USER, password: DB_PASSWORD },
  //   target: { server: DB_SERVER, database: 'TadaChecador-2.0', user: DB_USER, password: DB_PASSWORD }
  // }
];

// Tablas a excluir (estructura nueva de TADÁ 2.0)
const TABLES_TO_EXCLUDE = [
  'Personas', 'PersonasDirecciones', 'PersonasContactos', 'PersonasDocumentos',
  'PersonasBeneficiarios', 'PersonasEmergencia', 'PersonasFamiliares',
  'MaestroPersonaDatosTada20', 'personasTada20',
  'sysdiagrams', 'systranschemas', '__EFMigrationsHistory', 'CDC_SyncLog'
];

// Tipos de cambios que NO requieren migración de datos
const ALLOWED_CHANGES = [
  'ADD_COLUMN_NULLABLE',      // Agregar columna nullable
  'ADD_COLUMN_WITH_DEFAULT',  // Agregar columna con default
  'MODIFY_COLUMN_LENGTH',     // Aumentar tamaño de columna
  'ADD_INDEX',                // Crear índice
  'DROP_INDEX',               // Eliminar índice (cuidado)
  'ADD_CONSTRAINT',           // Agregar constraint
  'DROP_CONSTRAINT'           // Eliminar constraint
];

// Cambios que REQUIEREN migración de datos (no permitidos)
const BLOCKED_CHANGES = [
  'DROP_COLUMN',              // Eliminar columna con datos
  'MODIFY_COLUMN_TYPE',       // Cambiar tipo incompatible
  'ADD_COLUMN_NOT_NULL',      // Agregar NOT NULL sin default
  'DROP_TABLE'                // Eliminar tabla con datos
];

// ═══════════════════════════════════════════════════════════════════════════
// CLASE PRINCIPAL
// ═══════════════════════════════════════════════════════════════════════════
class SyncScriptGenerator {
  constructor() {
    this.timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    this.changes = [];
    this.blockedChanges = [];
    this.scripts = {};
  }

  createConnection(config) {
    return new Promise((resolve, reject) => {
      const connection = new Connection({
        server: config.server,
        authentication: {
          type: 'default',
          options: { userName: config.user, password: config.password }
        },
        options: {
          database: config.database,
          encrypt: false,
          trustServerCertificate: true,
          connectTimeout: 30000,
          requestTimeout: 120000
        }
      });
      connection.on('connect', (err) => err ? reject(err) : resolve(connection));
      connection.connect();
    });
  }

  executeQuery(connection, sql) {
    return new Promise((resolve, reject) => {
      const rows = [];
      const request = new Request(sql, (err) => err ? reject(err) : resolve(rows));
      request.on('row', (columns) => {
        const row = {};
        columns.forEach(col => row[col.metadata.colName] = col.value);
        rows.push(row);
      });
      connection.execSql(request);
    });
  }

  async getTables(connection) {
    const sql = `
      SELECT t.TABLE_SCHEMA as [schema], t.TABLE_NAME as [name]
      FROM INFORMATION_SCHEMA.TABLES t
      WHERE t.TABLE_TYPE = 'BASE TABLE' AND t.TABLE_SCHEMA NOT IN ('cdc', 'sys')
      ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
    `;
    return await this.executeQuery(connection, sql);
  }

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
      LEFT JOIN sys.columns sc ON sc.object_id = OBJECT_ID('${schema}.${tableName}') AND sc.name = c.COLUMN_NAME
      LEFT JOIN sys.identity_columns ic ON ic.object_id = sc.object_id AND ic.column_id = sc.column_id
      WHERE c.TABLE_SCHEMA = '${schema}' AND c.TABLE_NAME = '${tableName}'
      ORDER BY c.ORDINAL_POSITION
    `;
    return await this.executeQuery(connection, sql);
  }

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
      WHERE i.object_id = OBJECT_ID('${schema}.${tableName}') AND i.name IS NOT NULL
      GROUP BY i.name, i.type_desc, i.is_unique, i.is_primary_key
    `;
    try { return await this.executeQuery(connection, sql); }
    catch (e) { return []; }
  }

  async getTableRowCount(connection, schema, tableName) {
    try {
      const result = await this.executeQuery(connection,
        `SELECT COUNT(*) as cnt FROM [${schema}].[${tableName}]`
      );
      return result[0]?.cnt || 0;
    } catch (e) {
      return -1; // Error al contar
    }
  }

  /**
   * Formatea el tipo de columna para DDL
   */
  formatColumnType(col) {
    let type = col.data_type.toUpperCase();
    const dt = col.data_type.toLowerCase();

    if (['varchar', 'nvarchar', 'char', 'nchar', 'varbinary', 'binary'].includes(dt)) {
      type += `(${col.max_length > 0 ? col.max_length : 'MAX'})`;
    } else if (['decimal', 'numeric'].includes(dt)) {
      type += `(${col.precision || 18}, ${col.scale || 0})`;
    } else if (['datetime2', 'time'].includes(dt)) {
      type += `(${col.scale || 7})`;
    }

    return type;
  }

  /**
   * Genera script para agregar columna
   */
  generateAddColumnScript(schema, tableName, col, dbName) {
    const colType = this.formatColumnType(col);
    const nullable = col.is_nullable === 'YES' ? 'NULL' : 'NOT NULL';
    const defaultVal = col.default_value ? ` DEFAULT ${col.default_value}` : '';

    // Si es NOT NULL sin default, es un cambio bloqueado
    if (nullable === 'NOT NULL' && !defaultVal) {
      return {
        blocked: true,
        reason: 'Columna NOT NULL sin valor default requiere migración de datos',
        script: `-- BLOQUEADO: ALTER TABLE [${schema}].[${tableName}] ADD [${col.column_name}] ${colType} ${nullable};`
      };
    }

    return {
      blocked: false,
      script: `ALTER TABLE [${schema}].[${tableName}] ADD [${col.column_name}] ${colType} ${nullable}${defaultVal};`,
      verification: `SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${tableName}' AND COLUMN_NAME = '${col.column_name}'`
    };
  }

  /**
   * Genera script para modificar longitud de columna
   */
  generateModifyLengthScript(schema, tableName, colName, sourceCol, targetCol) {
    const sourceLen = sourceCol.max_length || 0;
    const targetLen = targetCol.max_length || 0;

    // Solo permitir AUMENTAR el tamaño, nunca reducir
    if (sourceLen > targetLen || (sourceLen === -1 && targetLen !== -1)) {
      // Origen es más grande o MAX, necesitamos actualizar destino
      const colType = this.formatColumnType(sourceCol);
      const nullable = targetCol.is_nullable === 'YES' ? 'NULL' : 'NOT NULL';

      return {
        blocked: false,
        script: `ALTER TABLE [${schema}].[${tableName}] ALTER COLUMN [${colName}] ${colType} ${nullable};`,
        verification: `SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${schema}' AND TABLE_NAME = '${tableName}' AND COLUMN_NAME = '${colName}'`
      };
    } else {
      return {
        blocked: true,
        reason: 'Reducir tamaño de columna puede causar pérdida de datos',
        script: `-- BLOQUEADO: No se puede reducir [${colName}] de ${sourceLen} a ${targetLen}`
      };
    }
  }

  /**
   * Genera script para crear índice
   */
  generateCreateIndexScript(schema, tableName, idx) {
    const unique = idx.is_unique ? 'UNIQUE ' : '';
    const type = idx.index_type === 'NONCLUSTERED' ? 'NONCLUSTERED' : 'CLUSTERED';

    return {
      blocked: false,
      script: `CREATE ${unique}${type} INDEX [${idx.index_name}] ON [${schema}].[${tableName}] (${idx.columns});`,
      verification: `SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID('${schema}.${tableName}') AND name = '${idx.index_name}'`
    };
  }

  /**
   * Analiza diferencias y genera scripts
   */
  async analyzePair(pair) {
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`Analizando: ${pair.source.database} → ${pair.target.database}`);
    console.log('═'.repeat(60));

    let sourceConn, targetConn;
    const pairChanges = [];
    const pairBlocked = [];
    const scripts = [];

    try {
      console.log('  Conectando...');
      sourceConn = await this.createConnection(pair.source);
      targetConn = await this.createConnection(pair.target);

      // Obtener tablas
      const sourceTables = await this.getTables(sourceConn);
      const targetTables = await this.getTables(targetConn);

      const filterTables = (tables) => tables.filter(t =>
        !TABLES_TO_EXCLUDE.includes(t.name) &&
        !t.name.startsWith('cdc_') &&
        !t.name.startsWith('tmp_') &&
        !t.name.startsWith('bak_')
      );

      const filteredSource = filterTables(sourceTables);
      const filteredTarget = filterTables(targetTables);

      const sourceSet = new Set(filteredSource.map(t => `${t.schema}.${t.name}`));
      const targetSet = new Set(filteredTarget.map(t => `${t.schema}.${t.name}`));

      // ═══════════════════════════════════════════════════════════════════
      // 1. Tablas que faltan en DESTINO (existen en origen pero no en destino)
      // ═══════════════════════════════════════════════════════════════════
      console.log('  Analizando tablas faltantes en destino...');
      for (const t of filteredSource) {
        const fullName = `${t.schema}.${t.name}`;
        if (!targetSet.has(fullName)) {
          const rowCount = await this.getTableRowCount(sourceConn, t.schema, t.name);

          if (rowCount > 0) {
            pairBlocked.push({
              type: 'CREATE_TABLE',
              table: fullName,
              reason: `Tabla tiene ${rowCount} registros que requieren migración`,
              action: 'BLOCKED'
            });
          } else {
            // Tabla vacía, podemos crearla
            // Obtener estructura completa para CREATE TABLE
            const columns = await this.getTableColumns(sourceConn, t.schema, t.name);
            const colDefs = columns.map(c => {
              const colType = this.formatColumnType(c);
              const nullable = c.is_nullable === 'YES' ? 'NULL' : 'NOT NULL';
              const identity = c.is_identity ? ' IDENTITY(1,1)' : '';
              const defaultVal = c.default_value ? ` DEFAULT ${c.default_value}` : '';
              return `  [${c.column_name}] ${colType}${identity} ${nullable}${defaultVal}`;
            }).join(',\n');

            scripts.push({
              type: 'CREATE_TABLE',
              table: fullName,
              script: `-- Crear tabla ${fullName}\nCREATE TABLE [${t.schema}].[${t.name}] (\n${colDefs}\n);\nGO\n`,
              verification: `SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${t.schema}' AND TABLE_NAME = '${t.name}'`
            });

            pairChanges.push({
              type: 'CREATE_TABLE',
              table: fullName,
              action: 'WILL_CREATE'
            });
          }
        }
      }

      // ═══════════════════════════════════════════════════════════════════
      // 2. Tablas comunes - analizar diferencias de estructura
      // ═══════════════════════════════════════════════════════════════════
      console.log('  Analizando columnas de tablas comunes...');
      const commonTables = filteredSource.filter(t => targetSet.has(`${t.schema}.${t.name}`));

      for (const table of commonTables) {
        const fullName = `${table.schema}.${table.name}`;
        const [sourceColumns, targetColumns] = await Promise.all([
          this.getTableColumns(sourceConn, table.schema, table.name),
          this.getTableColumns(targetConn, table.schema, table.name)
        ]);

        const sourceColMap = new Map(sourceColumns.map(c => [c.column_name, c]));
        const targetColMap = new Map(targetColumns.map(c => [c.column_name, c]));

        // Columnas que faltan en DESTINO
        for (const [colName, sourceCol] of sourceColMap) {
          if (!targetColMap.has(colName)) {
            const result = this.generateAddColumnScript(table.schema, table.name, sourceCol, pair.target.database);

            if (result.blocked) {
              pairBlocked.push({
                type: 'ADD_COLUMN',
                table: fullName,
                column: colName,
                reason: result.reason,
                action: 'BLOCKED'
              });
            } else {
              scripts.push({
                type: 'ADD_COLUMN',
                table: fullName,
                column: colName,
                script: result.script + '\nGO\n',
                verification: result.verification
              });
              pairChanges.push({
                type: 'ADD_COLUMN',
                table: fullName,
                column: colName,
                action: 'WILL_ADD'
              });
            }
          }
        }

        // Diferencias de longitud en columnas existentes
        for (const [colName, sourceCol] of sourceColMap) {
          const targetCol = targetColMap.get(colName);
          if (!targetCol) continue;

          // Comparar longitud
          if (sourceCol.max_length !== targetCol.max_length) {
            const result = this.generateModifyLengthScript(
              table.schema, table.name, colName, sourceCol, targetCol
            );

            if (result.blocked) {
              pairBlocked.push({
                type: 'MODIFY_LENGTH',
                table: fullName,
                column: colName,
                reason: result.reason,
                sourceLength: sourceCol.max_length,
                targetLength: targetCol.max_length,
                action: 'BLOCKED'
              });
            } else {
              scripts.push({
                type: 'MODIFY_LENGTH',
                table: fullName,
                column: colName,
                script: result.script + '\nGO\n',
                verification: result.verification
              });
              pairChanges.push({
                type: 'MODIFY_LENGTH',
                table: fullName,
                column: colName,
                sourceLength: sourceCol.max_length,
                targetLength: targetCol.max_length,
                action: 'WILL_MODIFY'
              });
            }
          }
        }

        // Analizar índices
        const [sourceIndexes, targetIndexes] = await Promise.all([
          this.getTableIndexes(sourceConn, table.schema, table.name),
          this.getTableIndexes(targetConn, table.schema, table.name)
        ]);

        const targetIdxSet = new Set(targetIndexes.map(i => i.index_name));

        for (const idx of sourceIndexes) {
          if (!targetIdxSet.has(idx.index_name) && !idx.is_primary_key) {
            const result = this.generateCreateIndexScript(table.schema, table.name, idx);
            scripts.push({
              type: 'CREATE_INDEX',
              table: fullName,
              index: idx.index_name,
              script: result.script + '\nGO\n',
              verification: result.verification
            });
            pairChanges.push({
              type: 'CREATE_INDEX',
              table: fullName,
              index: idx.index_name,
              action: 'WILL_CREATE'
            });
          }
        }
      }

      // Guardar resultados
      this.changes.push(...pairChanges.map(c => ({ ...c, database: pair.name })));
      this.blockedChanges.push(...pairBlocked.map(c => ({ ...c, database: pair.name })));

      if (scripts.length > 0) {
        this.scripts[pair.name] = {
          targetDb: pair.target.database,
          scripts: scripts,
          header: this.generateScriptHeader(pair.target.database, scripts.length)
        };
      }

      console.log(`\n  Resumen ${pair.name}:`);
      console.log(`    - Cambios permitidos: ${pairChanges.length}`);
      console.log(`    - Cambios bloqueados: ${pairBlocked.length}`);

    } catch (error) {
      console.error(`  ERROR: ${error.message}`);
    } finally {
      if (sourceConn) sourceConn.close();
      if (targetConn) targetConn.close();
    }
  }

  /**
   * Genera encabezado del script SQL
   */
  generateScriptHeader(dbName, changeCount) {
    return `/*
╔══════════════════════════════════════════════════════════════════════════════╗
║  SCRIPT DE SINCRONIZACIÓN ESTRUCTURAL                                        ║
║  Base de datos: ${dbName.padEnd(58)}║
║  Generado: ${new Date().toLocaleString('es-MX').padEnd(63)}║
║  Total de cambios: ${changeCount.toString().padEnd(55)}║
╠══════════════════════════════════════════════════════════════════════════════╣
║  IMPORTANTE:                                                                 ║
║  - Este script solo contiene cambios estructurales (DDL)                     ║
║  - NO incluye migración de datos                                             ║
║  - Ejecutar en ambiente de QA antes de producción                            ║
║  - Verificar cada cambio después de ejecutar                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
*/

USE [${dbName}];
GO

SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

-- ═══════════════════════════════════════════════════════════════════════════
-- INICIO DE CAMBIOS ESTRUCTURALES
-- ═══════════════════════════════════════════════════════════════════════════

`;
  }

  /**
   * Genera reporte de verificación Excel
   */
  async generateVerificationReport() {
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'Sync Script Generator';
    workbook.created = new Date();

    // ═══════════════════════════════════════════════════════════════════════
    // HOJA 1: RESUMEN
    // ═══════════════════════════════════════════════════════════════════════
    const summarySheet = workbook.addWorksheet('Resumen');

    summarySheet.mergeCells('A1:F1');
    summarySheet.getCell('A1').value = 'REPORTE DE SCRIPTS DE SINCRONIZACIÓN';
    summarySheet.getCell('A1').font = { bold: true, size: 16 };
    summarySheet.getCell('A1').alignment = { horizontal: 'center' };

    summarySheet.addRow([]);
    summarySheet.addRow(['Generado:', new Date().toLocaleString('es-MX')]);
    summarySheet.addRow(['Total cambios permitidos:', this.changes.length]);
    summarySheet.addRow(['Total cambios bloqueados:', this.blockedChanges.length]);

    summarySheet.addRow([]);
    summarySheet.addRow(['BASE DE DATOS', 'CAMBIOS PERMITIDOS', 'CAMBIOS BLOQUEADOS', 'SCRIPT GENERADO']);
    summarySheet.lastRow.font = { bold: true };

    const dbGroups = {};
    for (const c of this.changes) {
      dbGroups[c.database] = dbGroups[c.database] || { allowed: 0, blocked: 0 };
      dbGroups[c.database].allowed++;
    }
    for (const c of this.blockedChanges) {
      dbGroups[c.database] = dbGroups[c.database] || { allowed: 0, blocked: 0 };
      dbGroups[c.database].blocked++;
    }

    for (const [db, counts] of Object.entries(dbGroups)) {
      const hasScript = this.scripts[db] ? 'SI' : 'NO';
      summarySheet.addRow([db, counts.allowed, counts.blocked, hasScript]);
    }

    summarySheet.columns = [{ width: 25 }, { width: 20 }, { width: 20 }, { width: 20 }];

    // ═══════════════════════════════════════════════════════════════════════
    // HOJA 2: CAMBIOS PERMITIDOS
    // ═══════════════════════════════════════════════════════════════════════
    const allowedSheet = workbook.addWorksheet('Cambios Permitidos');

    allowedSheet.addRow(['BASE', 'TIPO', 'TABLA', 'COLUMNA/OBJETO', 'ACCIÓN', 'DETALLE']);
    allowedSheet.lastRow.font = { bold: true };
    allowedSheet.lastRow.eachCell(cell => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: '92D050' } };
    });

    for (const c of this.changes) {
      allowedSheet.addRow([
        c.database,
        c.type,
        c.table,
        c.column || c.index || '-',
        c.action,
        c.sourceLength ? `${c.sourceLength} → ${c.targetLength}` : ''
      ]);
    }

    allowedSheet.columns = [
      { width: 20 }, { width: 20 }, { width: 35 }, { width: 25 }, { width: 15 }, { width: 20 }
    ];

    // ═══════════════════════════════════════════════════════════════════════
    // HOJA 3: CAMBIOS BLOQUEADOS
    // ═══════════════════════════════════════════════════════════════════════
    const blockedSheet = workbook.addWorksheet('Cambios Bloqueados');

    blockedSheet.addRow(['BASE', 'TIPO', 'TABLA', 'COLUMNA/OBJETO', 'RAZÓN DEL BLOQUEO']);
    blockedSheet.lastRow.font = { bold: true };
    blockedSheet.lastRow.eachCell(cell => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF6B6B' } };
      cell.font = { bold: true, color: { argb: 'FFFFFF' } };
    });

    for (const c of this.blockedChanges) {
      allowedSheet.addRow([
        c.database,
        c.type,
        c.table,
        c.column || '-',
        c.reason
      ]);
    }

    blockedSheet.columns = [
      { width: 20 }, { width: 20 }, { width: 35 }, { width: 25 }, { width: 50 }
    ];

    return workbook;
  }

  /**
   * Guarda los scripts SQL generados
   */
  saveScripts() {
    const scriptsDir = path.join(__dirname, '..', 'scripts', 'sync');
    if (!fs.existsSync(scriptsDir)) {
      fs.mkdirSync(scriptsDir, { recursive: true });
    }

    const savedFiles = [];

    for (const [dbName, data] of Object.entries(this.scripts)) {
      let fullScript = data.header;

      // Agrupar por tipo
      const byType = {};
      for (const s of data.scripts) {
        byType[s.type] = byType[s.type] || [];
        byType[s.type].push(s);
      }

      for (const [type, scripts] of Object.entries(byType)) {
        fullScript += `\n-- ═══════════════════════════════════════════════════════════════════════════\n`;
        fullScript += `-- ${type} (${scripts.length} cambios)\n`;
        fullScript += `-- ═══════════════════════════════════════════════════════════════════════════\n\n`;

        for (const s of scripts) {
          fullScript += `-- ${s.table}${s.column ? '.' + s.column : ''}${s.index ? ' [' + s.index + ']' : ''}\n`;
          fullScript += s.script + '\n';
        }
      }

      fullScript += `\n-- ═══════════════════════════════════════════════════════════════════════════\n`;
      fullScript += `-- FIN DEL SCRIPT\n`;
      fullScript += `-- ═══════════════════════════════════════════════════════════════════════════\n`;
      fullScript += `\nPRINT 'Script ejecutado exitosamente para ${data.targetDb}';\nGO\n`;

      const fileName = `sync-${dbName}-${this.timestamp}.sql`;
      const filePath = path.join(scriptsDir, fileName);
      fs.writeFileSync(filePath, fullScript);
      savedFiles.push(filePath);

      console.log(`  📄 Script guardado: ${filePath}`);
    }

    return savedFiles;
  }

  /**
   * Ejecuta el proceso completo
   */
  async run() {
    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log('║     GENERADOR DE SCRIPTS DE SINCRONIZACIÓN                     ║');
    console.log('║     Solo cambios estructurales - Sin migración de datos        ║');
    console.log('╚════════════════════════════════════════════════════════════════╝\n');

    // Analizar cada par
    for (const pair of DB_PAIRS) {
      await this.analyzePair(pair);
    }

    // Crear directorios
    const reportsDir = path.join(__dirname, '..', 'reports');
    if (!fs.existsSync(reportsDir)) {
      fs.mkdirSync(reportsDir, { recursive: true });
    }

    // Guardar JSON de cambios
    const changesLog = {
      timestamp: new Date().toISOString(),
      summary: {
        allowedChanges: this.changes.length,
        blockedChanges: this.blockedChanges.length
      },
      allowed: this.changes,
      blocked: this.blockedChanges
    };

    const jsonPath = path.join(reportsDir, `sync-changes-${this.timestamp}.json`);
    fs.writeFileSync(jsonPath, JSON.stringify(changesLog, null, 2));
    console.log(`\n📄 Log de cambios: ${jsonPath}`);

    // Guardar scripts SQL
    console.log('\nGuardando scripts SQL...');
    const savedScripts = this.saveScripts();

    // Generar Excel de verificación
    console.log('\nGenerando reporte de verificación...');
    const workbook = await this.generateVerificationReport();
    const excelPath = path.join(reportsDir, `sync-verification-${this.timestamp}.xlsx`);
    await workbook.xlsx.writeFile(excelPath);
    console.log(`📊 Reporte Excel: ${excelPath}`);

    // Resumen final
    console.log('\n╔════════════════════════════════════════════════════════════════╗');
    console.log('║                    RESUMEN FINAL                               ║');
    console.log('╠════════════════════════════════════════════════════════════════╣');
    console.log(`║  Cambios permitidos:  ${this.changes.length.toString().padEnd(40)}║`);
    console.log(`║  Cambios bloqueados:  ${this.blockedChanges.length.toString().padEnd(40)}║`);
    console.log(`║  Scripts generados:   ${savedScripts.length.toString().padEnd(40)}║`);
    console.log('╠════════════════════════════════════════════════════════════════╣');

    if (this.blockedChanges.length > 0) {
      console.log('║  ⚠️  HAY CAMBIOS BLOQUEADOS - Revisar reporte                  ║');
    } else {
      console.log('║  ✅ Todos los cambios son estructurales                        ║');
    }

    console.log('╚════════════════════════════════════════════════════════════════╝\n');

    return {
      changes: this.changes,
      blocked: this.blockedChanges,
      scripts: savedScripts
    };
  }
}

// ═══════════════════════════════════════════════════════════════════════════
// EJECUCIÓN
// ═══════════════════════════════════════════════════════════════════════════
const generator = new SyncScriptGenerator();
generator.run()
  .then(() => {
    console.log('Generación completada.\n');
    process.exit(0);
  })
  .catch(err => {
    console.error('Error fatal:', err);
    process.exit(1);
  });
