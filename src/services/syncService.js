// src/services/syncService.js
const logger = require('../utils/logger');
const BusinessRules = require('./businessRules');
const { Connection, Request, TYPES } = require('tedious');

class SyncService {
  constructor(source, target) {
    this.sourceConnection = new ConnectionRunner(source.conn, source.config, logger, 'source');
    this.targetConnection = new ConnectionRunner(target.conn, target.config, logger, 'target');
    this.businessRules = new BusinessRules();
    this.syncState = new Map();
    this.syncLocks = new Map(); // Lock para evitar MERGE simultáneos
    this.processId = `PID-${process.pid}-${Date.now()}`; // ID único para este proceso
    this.nonExistentSourceTables = new Set(); // Cache de tablas que no existen en origen
    this.tablesWithoutPrimaryKey = new Set(); // Cache de tablas sin clave primaria
    this.tablesColumnsVerified = new Set(); // Cache de tablas cuyas columnas ya fueron verificadas
  }

  /**
   * Registra una operación en la tabla CDC_SyncLog
   */
  async writeLog(tableName, tipoOperacion, registroId, estado, mensaje = null, datosAntes = null, datosDespues = null, tiempoMs = null) {
    try {
      // Escapar strings para SQL
      const escapeSql = (str) => {
        if (str === null || str === undefined) return 'NULL';
        return `'${String(str).replace(/'/g, "''")}'`;
      };

      const datosAntesJson = datosAntes ? JSON.stringify(datosAntes).substring(0, 8000) : null; // Limitar tamaño
      const datosDespuesJson = datosDespues ? JSON.stringify(datosDespues).substring(0, 8000) : null;

      const query = `
        INSERT INTO dbo.CDC_SyncLog
        (TablaNombre, TipoOperacion, RegistroId, Estado, Mensaje, DatosAntes, DatosDespues, TiempoEjecucionMs, ProcesoId)
        VALUES
        (${escapeSql(tableName)}, ${escapeSql(tipoOperacion)}, ${escapeSql(registroId)}, ${escapeSql(estado)},
         ${escapeSql(mensaje)}, ${escapeSql(datosAntesJson)}, ${escapeSql(datosDespuesJson)},
         ${tiempoMs || 'NULL'}, ${escapeSql(this.processId)})
      `;

      await this.sourceConnection.exec(query);
    } catch (error) {
      // No fallar si el log falla, solo registrar el error
      logger.debug(`Error escribiendo en CDC_SyncLog: ${error.message}`);
    }
  }

  /**
   * Verifica si la tabla existe en la base de datos origen
   */
  async checkSourceTableExists(tableName, schemaName) {
    const query = `
      SELECT COUNT(*) as count
      FROM sys.tables
      WHERE schema_id = SCHEMA_ID('${schemaName}')
        AND name = '${tableName}'
    `;
    const result = await this.sourceConnection.query(query);
    return result[0]?.count > 0;
  }

  async ensureCDCEnabled(tableName, schemaName) {
    try {
      // Verificar si CDC está habilitado en la base de datos
      const dbCDCCheck = await this.sourceConnection.query(`
        SELECT is_cdc_enabled
        FROM sys.databases
        WHERE name = DB_NAME()
      `);

      if (dbCDCCheck[0].is_cdc_enabled === 0) {
        logger.info(`Habilitando CDC en la base de datos...`);
        await this.sourceConnection.exec(`EXEC sys.sp_cdc_enable_db`);
        logger.info(`CDC habilitado en la base de datos`);
      }

      // Verificar si CDC está habilitado en la tabla
      const tableCDCCheck = await this.sourceConnection.query(`
        SELECT is_tracked_by_cdc
        FROM sys.tables
        WHERE schema_id = SCHEMA_ID('${schemaName}')
        AND name = '${tableName}'
      `);

      if (tableCDCCheck.length === 0) {
        throw new Error(`Tabla ${schemaName}.${tableName} no existe en la base de datos origen`);
      }

      if (tableCDCCheck[0].is_tracked_by_cdc === 0) {
        logger.info(`Habilitando CDC en la tabla ${schemaName}.${tableName}...`);
        await this.sourceConnection.exec(`
          EXEC sys.sp_cdc_enable_table
            @source_schema = N'${schemaName}',
            @source_name = N'${tableName}',
            @role_name = NULL,
            @supports_net_changes = 1
        `);
        logger.info(`CDC habilitado en la tabla ${schemaName}.${tableName}`);
      } else {
        logger.debug(`CDC ya está habilitado en ${schemaName}.${tableName}`);
      }
    } catch (error) {
      logger.error(`Error habilitando CDC en ${schemaName}.${tableName}:`, error);
      throw error;
    }
  }

  async isTargetTableEmpty(tableName, schemaName) {
    const query = `SELECT TOP 1 1 as existsFlag FROM ${schemaName}.${tableName}`;
    const result = await this.targetConnection.query(query);
    return result.length === 0;
  }

  /**
   * Verifica si la tabla destino existe
   */
  async checkTargetTableExists(tableName, schemaName) {
    const query = `
      SELECT COUNT(*) as count
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '${schemaName}' AND TABLE_NAME = '${tableName}'
    `;
    const result = await this.targetConnection.query(query);
    return result[0]?.count > 0;
  }

  /**
   * Verifica si la función CDC existe para una tabla
   */
  async checkCDCFunctionExists(tableName, schemaName) {
    const query = `
      SELECT COUNT(*) as count
      FROM sys.objects
      WHERE name = 'fn_cdc_get_all_changes_${schemaName}_${tableName}'
        AND type = 'IF'
        AND schema_id = SCHEMA_ID('cdc')
    `;
    const result = await this.sourceConnection.query(query);
    return result[0]?.count > 0;
  }


  async snapshotInitial(tableName, schemaName) {
    const startTime = Date.now();
    logger.info(`Realizando snapshot inicial para ${schemaName}.${tableName}...`);

    // Copiar todos los datos de origen a destino
    await this.bulkInsertAll(tableName, schemaName);

    const totalTime = Date.now() - startTime;
    logger.info(`Snapshot inicial completado para ${schemaName}.${tableName} en ${totalTime}ms`);
  }

  /**
   * Bulk insert masivo - Copia datos de origen a destino usando paginación
   * para evitar problemas de memoria
   */
  async bulkInsertAll(tableName, schemaName) {
    const startTime = Date.now();
    logger.info(`Ejecutando BULK INSERT masivo para ${schemaName}.${tableName}...`);

    // Obtener la clave primaria
    const primaryKey = await this.getPrimaryKey(tableName, schemaName);

    if (!primaryKey) {
      logger.warn(`No se encontró clave primaria para ${schemaName}.${tableName}, usando inserción individual`);
      return this.mergeInitial(tableName, schemaName);
    }

    // Obtener el total de registros
    const countResult = await this.sourceConnection.query(
      `SELECT COUNT(*) as total FROM ${schemaName}.${tableName}`
    );
    const totalRows = countResult[0]?.total || 0;

    if (totalRows === 0) {
      logger.info(`No hay registros en origen para ${schemaName}.${tableName}`);
      return;
    }

    logger.info(`Total de registros a copiar ${schemaName}.${tableName}: ${totalRows}`);

    // Obtener columnas de la tabla
    const columnsResult = await this.sourceConnection.query(`
      SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schemaName}' AND TABLE_NAME = '${tableName}'
    `);
    const columns = columnsResult.map(c => c.COLUMN_NAME);

    // Lista de columnas binarias conocidas
    const binaryCols = new Set([
      'PassKiosko', 'Contrasena', 'Password', 'Foto', 'ImagenPerfil',
      'CardChecador', 'passChecador'
    ]);

    // Función para formatear un valor SQL
    const formatValue = (col, v) => {
      if (binaryCols.has(col)) {
        if (Buffer.isBuffer(v)) {
          if (v.length === 0) return 'NULL';
          return `CONVERT(varbinary(max), '0x${v.toString('hex')}', 1)`;
        } else if (typeof v === 'string') {
          if (!v || v === '' || v === '0x') return 'NULL';
          let hexStr = v.startsWith('0x') ? v.substring(2) : v;
          if (!/^[0-9a-fA-F]*$/.test(hexStr)) return 'NULL';
          if (hexStr.length % 2 !== 0) hexStr = '0' + hexStr;
          return `CONVERT(varbinary(max), '0x${hexStr}', 1)`;
        } else if (v == null) {
          return 'NULL';
        }
      }

      if (v === null || v === undefined) return 'NULL';
      if (v instanceof Date) return `'${v.toISOString()}'`;
      if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
      if (typeof v === 'number') return v;
      if (typeof v === 'boolean') return v ? '1' : '0';
      return `'${v}'`;
    };

    // Procesar en páginas para no cargar todo en memoria
    const PAGE_SIZE = 500;   // Registros por página de lectura (reducido para evitar OOM)
    const BATCH_SIZE = 100;  // Registros por INSERT (reducido para evitar OOM)
    let insertedCount = 0;
    let errorCount = 0;
    let offset = 0;

    while (offset < totalRows) {
      // Obtener una página de datos
      const pageQuery = `
        SELECT * FROM ${schemaName}.${tableName}
        ORDER BY ${primaryKey}
        OFFSET ${offset} ROWS FETCH NEXT ${PAGE_SIZE} ROWS ONLY
      `;

      const rows = await this.sourceConnection.query(pageQuery);

      if (rows.length === 0) break;

      // Procesar la página en batches
      for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);

        // Aplicar reglas de negocio y filtrar
        const processedBatch = [];
        for (const row of batch) {
          const processedData = await this.businessRules.applyRules(row, tableName, 'INSERT');
          if (processedData) {
            processedBatch.push(processedData);
          }
        }

        if (processedBatch.length === 0) continue;

        // Construir VALUES para el batch
        const valueRows = processedBatch.map(row => {
          const values = columns.map(col => formatValue(col, row[col]));
          return `(${values.join(', ')})`;
        });

        const query = `
          SET IDENTITY_INSERT ${schemaName}.${tableName} ON;
          INSERT INTO ${schemaName}.${tableName} (${columns.join(', ')})
          VALUES ${valueRows.join(',\n')};
          SET IDENTITY_INSERT ${schemaName}.${tableName} OFF;
        `;

        try {
          await this.targetConnection.exec(query);
          insertedCount += processedBatch.length;
        } catch (error) {
          logger.error(`Error en batch (offset ${offset + i}):`, error.message);

          // Fallback: insertar uno por uno
          for (const row of processedBatch) {
            try {
              await this.handleInsert(row, tableName, schemaName);
              insertedCount++;
            } catch (individualError) {
              errorCount++;
              logger.debug(`Error insertando registro ${row[primaryKey]}:`, individualError.message);
            }
          }
        }
      }

      logger.info(`Progreso ${schemaName}.${tableName}: ${Math.min(offset + PAGE_SIZE, totalRows)}/${totalRows} registros procesados`);
      offset += PAGE_SIZE;

      // Liberar memoria explícitamente
      if (global.gc) global.gc();
    }

    const totalTime = Date.now() - startTime;
    await this.writeLog(tableName, 'BULK_INSERT', null, 'SUCCESS',
      `Bulk insert completado: ${insertedCount} insertados, ${errorCount} errores en ${totalTime}ms`,
      null, { totalRows, inserted: insertedCount, errors: errorCount }, totalTime);

    logger.info(`BULK INSERT completado para ${schemaName}.${tableName}: ${insertedCount} registros en ${totalTime}ms`);
  }

  /**
   * Merge inicial - Para cuando la tabla destino ya tiene datos
   */
  async mergeInitial(tableName, schemaName) {
    logger.info(`Ejecutando MERGE inicial para ${schemaName}.${tableName} (tabla con datos existentes)...`);

    const primaryKey = await this.getPrimaryKey(tableName, schemaName);

    if (!primaryKey) {
      logger.warn(`⚠️  Tabla ${schemaName}.${tableName} no tiene clave primaria (IDENTITY). Omitiendo sincronización...`);
      return;
    }

    const rows = await this.sourceConnection.query(`SELECT * FROM ${schemaName}.${tableName}`);

    logger.info(`Total de registros en origen: ${rows.length}`);

    let insertCount = 0;
    let updateCount = 0;
    let skipCount = 0;
    let errorCount = 0;

    for (const row of rows) {
      let pkValue = null;

      try {
        const processedData = await this.businessRules.applyRules(row, tableName, 'INSERT');
        if (!processedData) {
          skipCount++;
          continue;
        }

        pkValue = processedData[primaryKey];
        const existsQuery = `SELECT COUNT(*) as count FROM ${schemaName}.${tableName} WHERE ${primaryKey} = ${pkValue}`;
        const existsResult = await this.targetConnection.query(existsQuery);

        if (existsResult[0]?.count > 0) {
          const currentQuery = `SELECT * FROM ${schemaName}.${tableName} WHERE ${primaryKey} = ${pkValue}`;
          const currentResult = await this.targetConnection.query(currentQuery);

          if (!currentResult || currentResult.length === 0) {
            await this.handleInsert(processedData, tableName, schemaName);
            insertCount++;
            continue;
          }

          const currentData = currentResult[0];
          const needsUpdate = this.compareRecords(processedData, currentData, [primaryKey, 'modifica', 'estatus']);

          if (needsUpdate) {
            await this.handleUpdate(processedData, tableName, schemaName);
            updateCount++;
          } else {
            skipCount++;
          }
        } else {
          await this.handleInsert(processedData, tableName, schemaName);
          insertCount++;
        }
      } catch (error) {
        errorCount++;
        logger.error(`Error procesando registro ${pkValue || row[primaryKey]}:`, error.message);
      }
    }

    logger.info(`MERGE inicial completado: ${insertCount} insertados, ${updateCount} actualizados, ${skipCount} omitidos, ${errorCount} errores`);
  }

  // Método auxiliar para comparar dos registros
  compareRecords(newRecord, oldRecord, excludeFields = []) {
    const excludeSet = new Set(excludeFields.map(f => f.toLowerCase()));

    for (const key of Object.keys(newRecord)) {
      if (excludeSet.has(key.toLowerCase())) continue;

      const newValue = newRecord[key];
      const oldValue = oldRecord[key];

      // Comparar valores (considerar null/undefined como iguales)
      if (newValue == null && oldValue == null) continue;
      if (newValue == null || oldValue == null) return true;

      // Para buffers, comparar contenido
      if (Buffer.isBuffer(newValue) && Buffer.isBuffer(oldValue)) {
        if (!newValue.equals(oldValue)) return true;
        continue;
      }

      // Para fechas, comparar timestamps
      if (newValue instanceof Date && oldValue instanceof Date) {
        if (newValue.getTime() !== oldValue.getTime()) return true;
        continue;
      }

      // Comparación normal
      if (newValue !== oldValue) return true;
    }

    return false; // No hay diferencias
  }

  async syncTable(tableName, schemaName = 'dbo') {
    const tableKey = `${schemaName}.${tableName}`;

    // 🔒 Verificar si ya hay una sincronización en progreso para esta tabla
    if (this.syncLocks.get(tableKey)) {
      logger.debug(`Sincronización ya en progreso para ${tableKey}, omitiendo...`);
      return;
    }

    // ⏭️ Si ya sabemos que la tabla no existe en origen, omitir silenciosamente
    if (this.nonExistentSourceTables.has(tableKey)) {
      return;
    }

    // ⏭️ Si ya sabemos que la tabla no tiene clave primaria, omitir silenciosamente
    if (this.tablesWithoutPrimaryKey.has(tableKey)) {
      return;
    }

    // Establecer el lock
    this.syncLocks.set(tableKey, true);

    try {
      // 0️⃣ Verificar si la tabla existe en origen
      const sourceTableExists = await this.checkSourceTableExists(tableName, schemaName);
      if (!sourceTableExists) {
        // Agregar al cache para no repetir el mensaje
        this.nonExistentSourceTables.add(tableKey);
        logger.warn(`⚠️  Tabla ${tableKey} NO EXISTE en la base de datos origen. Omitiendo...`);
        return;
      }

      // 0.5️⃣ Verificar si la tabla tiene clave primaria (IDENTITY)
      const primaryKey = await this.getPrimaryKey(tableName, schemaName);
      if (!primaryKey) {
        // Agregar al cache para no repetir el mensaje
        this.tablesWithoutPrimaryKey.add(tableKey);
        logger.warn(`⚠️  Tabla ${tableKey} NO TIENE clave primaria (IDENTITY). Omitiendo...`);
        return;
      }

      // 1️⃣ Asegurar que CDC esté habilitado en la tabla origen
      await this.ensureCDCEnabled(tableName, schemaName);

      // 2️⃣ Verificar si la tabla destino existe
      const targetTableExists = await this.checkTargetTableExists(tableName, schemaName);

      if (!targetTableExists) {
        // La tabla NO existe → crearla y migrar TODOS los datos
        logger.info(`Tabla ${tableKey} no existe en destino → creando estructura y migrando datos`);
        await this.ensureTableAndColumnsExist(tableName, schemaName, {});
        await this.snapshotInitial(tableName, schemaName);

        // Marcar como sincronizado y guardar LSN actual
        this.syncState.set(`${tableKey}_initial_sync_done`, true);
        const CDCService = require('../config/cdcService');
        const cdcService = new CDCService(this.sourceConnection);
        const currentLSN = await cdcService.getLastLSN();
        if (currentLSN) {
          this.syncState.set(tableKey, currentLSN);
          logger.info(`Tabla ${tableKey} migrada. LSN guardado, CDC activo desde ahora`);
        }
        return;
      }

      // 3️⃣ La tabla existe → asegurar columnas y continuar con CDC
      await this.ensureTableAndColumnsExist(tableName, schemaName, {});

      // Marcar como sincronizado si no lo está (para tablas que ya existían)
      const initialSyncKey = `${tableKey}_initial_sync_done`;
      if (!this.syncState.get(initialSyncKey)) {
        this.syncState.set(initialSyncKey, true);
        logger.info(`Tabla ${tableKey} ya existe en destino → omitiendo snapshot, usando CDC`);

        // Guardar LSN actual para empezar a capturar cambios desde ahora
        const CDCService = require('../config/cdcService');
        const cdcService = new CDCService(this.sourceConnection);
        const currentLSN = await cdcService.getLastLSN();
        if (currentLSN) {
          this.syncState.set(tableKey, currentLSN);
          logger.info(`LSN inicial guardado para ${tableKey}`);
        }
      }

      // 4️⃣ Ciclos posteriores: procesar solo cambios CDC
      const CDCService = require('../config/cdcService');
      const cdcService = new CDCService(this.sourceConnection);

      // Verificar si la función CDC existe para esta tabla
      const cdcFunctionExists = await this.checkCDCFunctionExists(tableName, schemaName);
      if (!cdcFunctionExists) {
        logger.debug(`Función CDC no existe aún para ${tableKey}, omitiendo ciclo CDC`);
        return;
      }

      const lastProcessedLSN = this.syncState.get(tableKey) || null;

      try {
        const changes = await cdcService.getTableChanges(tableName, schemaName, lastProcessedLSN);

        if (changes.length === 0) {
          logger.debug(`No hay cambios CDC para ${tableKey}`);
          return;
        }

        logger.info(`Procesando ${changes.length} cambios CDC para ${tableKey}`);

        for (const change of changes) {
          await this.processChange(change, tableName, schemaName);
          this.syncState.set(tableKey, change.start_lsn);
        }

        logger.info(`Sincronización CDC completada para ${tableKey}`);
      } catch (cdcError) {
        // Si la función CDC no existe, solo logueamos y continuamos
        if (cdcError.message && cdcError.message.includes('Invalid object name')) {
          logger.warn(`Función CDC aún no disponible para ${tableKey}, se reintentará en el próximo ciclo`);
          return;
        }
        throw cdcError;
      }
    } catch (error) {
      logger.error(`Error sincronizando tabla ${schemaName}.${tableName}:`, error);
      throw error;
    } finally {
      // 🔓 Liberar el lock siempre, incluso si hubo error
      this.syncLocks.delete(tableKey);
    }
  }

  async processChange(change, tableName, schemaName) {
    const operation = this.interpretOperation(change.operation);

    // Aplicar reglas de negocio
    const processedData = await this.businessRules.applyRules(change, tableName, operation);

    if (!processedData) {
      logger.debug(`Cambio filtrado por reglas de negocio para ${tableName}`);
      return;
    }

    switch (operation) {
      case 'INSERT':
        await this.handleInsert(processedData, tableName, schemaName);
        break;
      case 'UPDATE_AFTER':
        await this.handleUpdate(processedData, tableName, schemaName);
        break;
      case 'DELETE':
        await this.handleDelete(processedData, tableName, schemaName);
        break;
      default:
        logger.debug(`Operación ${operation} no procesada para ${tableName}`);
    }
  }

  async handleInsert(data, tableName, schemaName) {
    try {
      await this.ensureTableAndColumnsExist(tableName, schemaName, data);
      const columns = Object.keys(data).filter(key => !key.startsWith('__$'));

      // Lista de columnas binarias conocidas
      const binaryCols = new Set([
        'PassKiosko', 'Contrasena', 'Password', 'Foto', 'ImagenPerfil',
        'CardChecador', 'passChecador'
      ]);

      // Construir VALUES con CONVERT para columnas varbinary
      const valuesList = columns.map(col => {
        const v = data[col];

        if (binaryCols.has(col)) {
          // Para columnas binarias, usar CONVERT
          if (Buffer.isBuffer(v)) {
            if (v.length === 0) return 'NULL';  // Buffer vacío
            return `CONVERT(varbinary(max), '0x${v.toString('hex')}', 1)`;
          } else if (typeof v === 'string') {
            if (!v || v === '' || v === '0x') return 'NULL';  // String vacío
            let hexStr = v.startsWith('0x') ? v.substring(2) : v;
            // Validar que solo contiene caracteres hex válidos
            if (!/^[0-9a-fA-F]*$/.test(hexStr)) return 'NULL';
            // Pad con 0 a la izquierda si la longitud es impar
            if (hexStr.length % 2 !== 0) {
              hexStr = '0' + hexStr;
            }
            return `CONVERT(varbinary(max), '0x${hexStr}', 1)`;
          } else if (v == null) {
            return 'NULL';
          }
        }

        // Para otros tipos, formatear normalmente
        if (v === null || v === undefined) return 'NULL';
        if (v instanceof Date) return `'${v.toISOString()}'`;
        if (typeof v === 'string') return `'${v.replace(/'/g, "''")}'`;
        if (typeof v === 'number') return v;
        if (typeof v === 'boolean') return v ? '1' : '0';
        return `'${v}'`;
      });

      const query = `
        SET IDENTITY_INSERT ${schemaName}.${tableName} ON;
        INSERT INTO ${schemaName}.${tableName} (${columns.join(', ')})
        VALUES (${valuesList.join(', ')});
        SET IDENTITY_INSERT ${schemaName}.${tableName} OFF;
      `;

      await this.executeTargetQuery(query);
      logger.debug(`INSERT ejecutado para ${tableName}`);
    } catch (error) {
      logger.error(`Error en INSERT para ${tableName}:`, error);
      throw error;
    }
  }

  async executeInsertWithIdentity(tableName, schemaName, query, values) {
    return new Promise((resolve, reject) => {
      const { Request, TYPES } = require('tedious');

      // Armamos un solo batch con los tres comandos
      let sqlBatch = `
        SET IDENTITY_INSERT ${schemaName}.${tableName} ON;
        ${query};
        SET IDENTITY_INSERT ${schemaName}.${tableName} OFF;
      `;

      const request = new Request(sqlBatch, (err, rowCount) => {
        if (err) reject(err);
        else resolve(rowCount);
      });

      // Agregar parámetros
      values.forEach((param, index) => {
        const val = param === null || param === undefined ? null : param;
        const type =
          typeof val === 'number'
            ? Number.isInteger(val)
              ? TYPES.Int
              : TYPES.Float
            : val instanceof Date
            ? TYPES.DateTime
            : TYPES.NVarChar;
        request.addParameter(`param${index}`, type, val);
      });

      this.targetConnection.execSqlBatch(request); // usar execSqlBatch, no execSql
    });
  }

  // async handleUpdate(data, tableName, schemaName) {
  //   try {
  //     // Obtener columnas reales de la tabla
  //     await this.ensureTableAndColumnsExist(tableName, schemaName, data);
  //     const allColumns = await this.getTableColumns(tableName, schemaName);

  //     // Filtrar solo las que existen en data
  //     const columns = allColumns.filter(col => col in data);

  //     const primaryKey = await this.getPrimaryKey(tableName, schemaName);
  //     const updateColumns = columns.filter(col => col !== primaryKey);

  //     const setClause = updateColumns.map((col, i) => `${col} = @param${i}`).join(', ');

  //     updateColumns.forEach((col, i) => {
  //       const value = row[col]; // tu valor de entrada

  //       if (col.toLowerCase() === 'passkiosko') {
  //         // Asume que recibes string tipo "0xABCD..." → conviértelo a Buffer
  //         const buffer = Buffer.from(value.replace(/^0x/, ''), 'hex');
  //         request.addParameter(`param${i}`, TYPES.VarBinary, buffer);
  //       } else {
  //         request.addParameter(`param${i}`, TYPES.NVarChar, value);
  //       }
  //     });
  //     const whereParamIndex = updateColumns.length;

  //     const query = `
  //       UPDATE ${schemaName}.${tableName}
  //       SET ${setClause}
  //       WHERE ${primaryKey} = @param${whereParamIndex}
  //     `;

  //     const values = updateColumns.map(col => data[col]);
  //     values.push(data[primaryKey]);

  //     // Log con valores reales
  //     const prettyQuery = (sql, vals) => {
  //       let q = sql;
  //       vals.forEach((v, i) => {
  //         let val = v;
  //         if (Buffer.isBuffer(v)) val = `'0x${v.toString('hex')}'`;
  //         else if (v instanceof Date) val = `'${v.toISOString()}'`;
  //         else if (typeof v === 'string') val = `'${v.replace(/'/g, "''")}'`;
  //         else if (v === null || v === undefined) val = 'NULL';
  //         q = q.replace(`@param${i}`, val);
  //       });
  //       return q;
  //     };

  //     console.log('Executing UPDATE:\n', prettyQuery(query, values));

  //     await this.executeTargetQuery(query, values);
  //     logger.debug(`UPDATE ejecutado para ${tableName}`);
  //   } catch (error) {
  //     logger.error(`Error en UPDATE para ${tableName}:`, error);
  //     throw error;
  //   }
  // }

  // async handleUpdate(data, tableName, schemaName) {
  //   try {
  //     // 1) Asegura tabla/columnas y obtén metadata
  //     await this.ensureTableAndColumnsExist(tableName, schemaName, data);
  //     const allColumns = await this.getTableColumns(tableName, schemaName);

  //     // 2) Filtra columnas realmente presentes en `data`
  //     const columns = allColumns.filter(col => col in data);

  //     // 3) PK y columnas a actualizar
  //     const primaryKey = await this.getPrimaryKey(tableName, schemaName);
  //     const updateColumns = columns.filter(col => col !== primaryKey);

  //     // 4) SET clause — usa CONVERT para PassKiosko (string '0x...' -> varbinary)

  //     const setClause = updateColumns.map((col, i) => {
  //       if (col.toLowerCase() === 'passkiosko') {
  //         // estilo 1 espera el prefijo 0x en el parámetro NVARCHAR
  //         return `${col} = CAST(@param${i} AS varbinary(max))`;
  //       }
  //       return `${col} = @param${i}`;
  //     }).join(', ');

  //     // 5) Query
  //     const whereParamIndex = updateColumns.length;
  //     const query = `
  //       UPDATE ${schemaName}.${tableName}
  //       SET ${setClause}
  //       WHERE ${primaryKey} = @param${whereParamIndex}
  //     `;

  //     // 6) Valores en el mismo orden de @paramN
  //     const values = updateColumns.map(col => {
  //       if (col.toLowerCase() === 'passkiosko' && Buffer.isBuffer(data[col])) {
  //         // Si por alguna razón llega como Buffer, lo convertimos a '0x...' string (estilo 1)
  //         return  data[col].toString('hex');
  //       }
  //       return data[col];
  //     });
  //     values.push(data[primaryKey]); // WHERE

  //     // 7) Log bonito
  //     const prettyQuery = (sql, vals) => {
  //       let q = sql;
  //       vals.forEach((v, i) => {
  //         let val = v;
  //         if (Buffer.isBuffer(v)) val = `'0x${v.toString('hex')}'`;
  //         else if (v instanceof Date) val = `'${v.toISOString()}'`;
  //         else if (typeof v === 'string') val = `'${v.replace(/'/g, "''")}'`;
  //         else if (v === null || v === undefined) val = 'NULL';
  //         q = q.replace(`@param${i}`, val);
  //       });
  //       return q;
  //     };

  //     console.log('Executing UPDATE:\n', prettyQuery(query, values));

  //     // 8) Ejecuta (tu método interno hoy asume todos NVARCHAR, por eso usamos CONVERT en SQL)
  //     await this.executeTargetQuery(query, values);
  //     logger.debug(`UPDATE ejecutado para ${tableName}`);
  //   } catch (error) {
  //     logger.error(`Error en UPDATE para ${tableName}:`, error);
  //     throw error;
  //   }
  // }

  async handleUpdate(data, tableName, schemaName) {
    try {
      // 1) Metadata
      await this.ensureTableAndColumnsExist(tableName, schemaName, data);
      const allColumns = await this.getTableColumns(tableName, schemaName);

      // 2) Columnas presentes
      const columns = allColumns.filter(col => col in data);

      // 3) PK y columnas a actualizar
      const primaryKey = await this.getPrimaryKey(tableName, schemaName);
      const updateColumns = columns.filter(col => col !== primaryKey);

      // 🚩 Lista de columnas VARBINARY reales (ajústala según tu tabla)
      const binaryCols = new Set([
        'PassKiosko',       // seguro
        'Contrasena',       // si es varbinary
        'Foto',             // si es varbinary
        'ImagenPerfil',     // si es varbinary
        'CardChecador',     // si es varbinary
        'passChecador'      // si es varbinary
      ]);

      // 4) SET: para binarios usa CONVERT(..., 1)  ← estilo 1 = espera '0x...'
      const setClause = updateColumns.map((col, i) => {
        if (binaryCols.has(col)) {
          return `${col} = CONVERT(varbinary(max), @param${i}, 1)`;
        }
        return `${col} = @param${i}`;
      }).join(', ');

      // 5) Query
      const whereParamIndex = updateColumns.length;
      const query = `
        UPDATE ${schemaName}.${tableName}
        SET ${setClause}
        WHERE ${primaryKey} = @param${whereParamIndex}
      `;

      // 6) Valores (en el orden de @paramN)
      const values = updateColumns.map(col => {
        const v = data[col];

        if (binaryCols.has(col)) {
          // Si viene Buffer -> convertir a '0x' + hex (estilo 1)
          if (Buffer.isBuffer(v)) {
            if (v.length === 0) return null;
            return '0x' + v.toString('hex');
          }

          // Si viene string -> asegurar prefijo '0x' y pad si es impar
          if (typeof v === 'string') {
            if (!v.length || v === '0x') return null;  // cadena vacía => NULL
            let hexStr = v.startsWith('0x') ? v.substring(2) : v;
            // Validar que solo contiene caracteres hex válidos
            if (!/^[0-9a-fA-F]*$/.test(hexStr)) return null;
            // Pad con 0 a la izquierda si la longitud es impar
            if (hexStr.length % 2 !== 0) {
              hexStr = '0' + hexStr;
            }
            return '0x' + hexStr;
          }

          // null/undefined -> NULL
          if (v == null) return null;

          // Cualquier otro tipo no es válido para binario
          throw new Error(`${col} debe ser Buffer o string hex (con/sin 0x)`);
        }

        // Sugerencia: si tus fechas llegan con 'Z', mejor pásalas como Date en data
        return v;
      });

      // WHERE
      values.push(data[primaryKey]);

      // 7) Log bonito (opcional)
      const prettyQuery = (sql, vals) => {
        let q = sql;
        vals.forEach((v, i) => {
          let val = v;
          if (Buffer.isBuffer(v)) val = `'0x' + v.toString('hex')`;
          else if (v instanceof Date) val = `'${v.toISOString()}'`;
          else if (typeof v === 'string') val = `'${v.replace(/'/g, "''")}'`;
          else if (v === null || v === undefined) val = 'NULL';
          q = q.replace(`@param${i}`, val);
        });
        return q;
      };
      // 8) Ejecuta (tu ejecutor enviará NVARCHAR, por eso usamos CONVERT en SQL)
      await this.executeTargetQuery(prettyQuery(query, values));
      logger.debug(`UPDATE ejecutado para ${tableName}`);
    } catch (error) {
      logger.error(`Error en UPDATE para ${tableName}:`, error);
      throw error;
    }
  }

  async getTableColumns(tableName, schemaName) {
    const query = `
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schemaName}'
        AND TABLE_NAME = '${tableName}'
    `;

    const result = await this.sourceConnection.query(query);
    return result.map(r => r.COLUMN_NAME);
  }

  async handleDelete(data, tableName, schemaName) {
    try {
      const primaryKey = await this.getPrimaryKey(tableName, schemaName);

      const query = `
        DELETE FROM ${schemaName}.${tableName}
        WHERE ${primaryKey} = @param0
      `;

      await this.executeTargetQuery(query, [data[primaryKey]]);
      logger.debug(`DELETE ejecutado para ${tableName}`);
    } catch (error) {
      logger.error(`Error en DELETE para ${tableName}:`, error);
      throw error;
    }
  }

  async getPrimaryKey(tableName, schemaName) {
    const query = `SELECT c.name AS COLUMN_NAME
      FROM sys.columns c
      JOIN sys.objects o ON o.object_id = c.object_id
      WHERE o.type = 'U' AND o.name = '${tableName}' and c.is_identity = 1;`;

    const result = await this.sourceConnection.query(query);
    return result.length ? result[0].COLUMN_NAME : null;
  }

  async executeTargetQuery(query, parameters = []) {
    // Si no hay parámetros, ejecutar directamente (query ya tiene valores interpolados)
    if (parameters.length === 0) {
      return await this.targetConnection.exec(query);
    }

    // Si hay parámetros, convertir a formato del ConnectionRunner
    const { TYPES } = require('tedious');
    const params = parameters.map((param, index) => ({
      name: `param${index}`,
      type: this._detectType(param),
      value: param
    }));

    return await this.targetConnection.exec(query, params);
  }

  _detectType(val) {
    const { TYPES } = require('tedious');
    if (val === null || val === undefined) return TYPES.NVarChar;
    if (typeof val === 'string') return TYPES.NVarChar;
    if (typeof val === 'number') {
      return Number.isInteger(val) ? TYPES.Int : TYPES.Float;
    }
    if (val instanceof Date) return TYPES.DateTime;
    if (Buffer.isBuffer(val)) return TYPES.VarBinary;
    if (typeof val === 'boolean') return TYPES.Bit;
    return TYPES.NVarChar;
  }

  async reconnectTarget() {
    return new Promise((resolve, reject) => {
      if (this.targetConnection.state === this.targetConnection.STATE.LoggedIn) {
        resolve();
        return;
      }

      // Si la conexión está en otro estado, cerrarla primero
      if (this.targetConnection.state !== this.targetConnection.STATE.Final) {
        this.targetConnection.close();
      }

      // Crear nueva conexión
      this.targetConnection.connect();

      this.targetConnection.on('connect', (err) => {
        if (err) {
          logger.error('Error reconnecting to target database:', err);
          reject(err);
        } else {
          logger.info('Successfully reconnected to target database');
          resolve();
        }
      });

      this.targetConnection.on('error', (err) => {
        console.error('Target connection error:', err);
        reject(err);
      });

      this.targetConnection.connect();
    });
  }

  async ensureTableAndColumnsExist(tableName, schemaName, data) {
    const tableKey = `${schemaName}.${tableName}`;

    // ⏭️ Si ya verificamos las columnas de esta tabla, saltar
    if (this.tablesColumnsVerified.has(tableKey)) {
      return;
    }

    // 1️⃣ Verificar si la tabla existe
    const tableCheckQuery = `
      SELECT COUNT(*) AS count
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '${schemaName}' AND TABLE_NAME = '${tableName}'
    `;
    const tableCheck = await this.targetConnection.query(tableCheckQuery);

    // if (tableCheck[0].count === 0) {
    //   logger.warn(`Tabla ${schemaName}.${tableName} no existe. Creando...`);
    //   const cdcServiceSource = new (require('../config/cdcService'))(this.sourceConnection);

    //   // Crear tabla con todas las columnas de data como NVARCHAR(MAX)
    //   const columnsDef = Object.keys(data)
    //     .filter(col => !col.startsWith('__$'))
    //     .map(col => `[${col}] NVARCHAR(MAX) NULL`)
    //     .join(',\n    ');

    //   const createTableQuery = `
    //     CREATE TABLE ${schemaName}.${tableName} (
    //       ${columnsDef}
    //     )
    //   `;

    //   console.log('Creating table with query:', createTableQuery);
    //   await cdcService.executeQuery(createTableQuery);
    //   logger.info(`Tabla ${schemaName}.${tableName} creada exitosamente`);
    //   return;
    // }


    // 2️⃣ Verificar y agregar columnas faltantes
    if (tableCheck[0].count === 0) {
      logger.warn(`Tabla ${schemaName}.${tableName} no existe. Creando...`);

      // Consultar columnas con tipos y si son identidad
      const columnInfoQuery = `
        SELECT
          c.COLUMN_NAME,
          c.DATA_TYPE,
          c.CHARACTER_MAXIMUM_LENGTH,
          c.NUMERIC_PRECISION,
          c.NUMERIC_SCALE,
          c.IS_NULLABLE,
          CASE
            WHEN ic.column_id IS NOT NULL THEN 1
            ELSE 0
          END AS IS_IDENTITY
        FROM INFORMATION_SCHEMA.COLUMNS c
        LEFT JOIN sys.columns sc
          ON sc.object_id = OBJECT_ID('${schemaName}.${tableName}')
          AND sc.name = c.COLUMN_NAME
        LEFT JOIN sys.identity_columns ic
          ON ic.object_id = sc.object_id AND ic.column_id = sc.column_id
        WHERE c.TABLE_SCHEMA = '${schemaName}' AND c.TABLE_NAME = '${tableName}'
      `;

      const sourceColumns = await this.sourceConnection.query(columnInfoQuery);

      if (!sourceColumns.length) {
        throw new Error(`No se encontraron columnas en la tabla origen ${schemaName}.${tableName}`);
      }

      // Mapear tipo de datos SQL con tamaños correctos
      const mapDataType = (col) => {
        const dataType = col.DATA_TYPE.toLowerCase();
        const maxLength = col.CHARACTER_MAXIMUM_LENGTH;
        const precision = col.NUMERIC_PRECISION;
        const scale = col.NUMERIC_SCALE;

        switch (dataType) {
          case 'nvarchar':
          case 'varchar':
          case 'char':
          case 'nchar':
            return `${col.DATA_TYPE.toUpperCase()}(${maxLength > 0 ? maxLength : 'MAX'})`;

          case 'varbinary':
          case 'binary':
            return `${col.DATA_TYPE.toUpperCase()}(${maxLength > 0 ? maxLength : 'MAX'})`;

          case 'decimal':
          case 'numeric':
            return `${col.DATA_TYPE.toUpperCase()}(${precision || 18}, ${scale || 0})`;

          case 'datetime2':
          case 'time':
            return `${col.DATA_TYPE.toUpperCase()}(${scale || 7})`;

          case 'datetime':
          case 'date':
          case 'int':
          case 'bigint':
          case 'smallint':
          case 'tinyint':
          case 'bit':
          case 'float':
          case 'real':
          case 'uniqueidentifier':
          case 'text':
          case 'ntext':
          case 'money':
          case 'smallmoney':
          case 'image':
            return col.DATA_TYPE.toUpperCase();

          default:
            return 'NVARCHAR(MAX)';
        }
      };

      // Construcción dinámica de columnas
      let columns = sourceColumns.map(col => {
        const dataType = mapDataType(col);
        const nullable = col.IS_NULLABLE === 'YES' ? 'NULL' : 'NOT NULL';
        const identity = col.IS_IDENTITY === 1 ? 'IDENTITY(1,1)' : '';
        return {
          name: col.COLUMN_NAME,
          definition: `[${col.COLUMN_NAME}] ${dataType} ${identity} ${nullable}`.trim()
        };
      });

      // ✅ Forzar agregar 'modifica' y 'estatus' si no existen
      const existingColumnNames = columns.map(c => c.name.toLowerCase());

      if (!existingColumnNames.includes('modifica')) {
        columns.push({ name: 'modifica', definition: `[modifica] DATETIME NULL` });
      }

      if (!existingColumnNames.includes('estatus')) {
        columns.push({ name: 'estatus', definition: `[estatus] INT NULL` });
      }

      const columnsDef = columns.map(c => c.definition).join(',\n  ');

      const createTableQuery = `
        CREATE TABLE ${schemaName}.${tableName} (
          ${columnsDef}
        )
      `;

      logger.debug(`Creando tabla ${schemaName}.${tableName}`);
      await this.targetConnection.exec(createTableQuery);
      logger.info(`Tabla ${schemaName}.${tableName} creada exitosamente`);
      // ✅ Marcar tabla como verificada
      this.tablesColumnsVerified.add(tableKey);
      return;
    }


    // 3️⃣ Si la tabla YA existe, verificar y agregar columnas faltantes con tipos correctos
    logger.debug(`Tabla ${schemaName}.${tableName} ya existe. Verificando columnas faltantes...`);

    // Obtener estructura completa de la tabla origen
    const sourceColumnInfoQuery = `
      SELECT
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.NUMERIC_PRECISION,
        c.NUMERIC_SCALE,
        c.IS_NULLABLE,
        CASE
          WHEN ic.column_id IS NOT NULL THEN 1
          ELSE 0
        END AS IS_IDENTITY
      FROM INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN sys.columns sc
        ON sc.object_id = OBJECT_ID('${schemaName}.${tableName}')
        AND sc.name = c.COLUMN_NAME
      LEFT JOIN sys.identity_columns ic
        ON ic.object_id = sc.object_id AND ic.column_id = sc.column_id
      WHERE c.TABLE_SCHEMA = '${schemaName}' AND c.TABLE_NAME = '${tableName}'
    `;
    const sourceColumns = await this.sourceConnection.query(sourceColumnInfoQuery);

    // Obtener columnas existentes en destino
    const targetColQuery = `
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schemaName}' AND TABLE_NAME = '${tableName}'
    `;
    const targetColResult = await this.targetConnection.query(targetColQuery);
    const existingColumns = new Set(targetColResult.map(r => r.COLUMN_NAME.toLowerCase()));

    // Mapear tipo de datos SQL con tamaños correctos
    const mapDataType = (col) => {
      const dataType = col.DATA_TYPE.toLowerCase();
      const maxLength = col.CHARACTER_MAXIMUM_LENGTH;
      const precision = col.NUMERIC_PRECISION;
      const scale = col.NUMERIC_SCALE;

      switch (dataType) {
        case 'nvarchar':
        case 'varchar':
        case 'char':
        case 'nchar':
          return `${col.DATA_TYPE.toUpperCase()}(${maxLength > 0 ? maxLength : 'MAX'})`;

        case 'varbinary':
        case 'binary':
          return `${col.DATA_TYPE.toUpperCase()}(${maxLength > 0 ? maxLength : 'MAX'})`;

        case 'decimal':
        case 'numeric':
          return `${col.DATA_TYPE.toUpperCase()}(${precision || 18}, ${scale || 0})`;

        case 'datetime2':
        case 'time':
          return `${col.DATA_TYPE.toUpperCase()}(${scale || 7})`;

        case 'datetime':
        case 'date':
        case 'int':
        case 'bigint':
        case 'smallint':
        case 'tinyint':
        case 'bit':
        case 'float':
        case 'real':
        case 'uniqueidentifier':
        case 'text':
        case 'ntext':
        case 'money':
        case 'smallmoney':
        case 'image':
          return col.DATA_TYPE.toUpperCase();

        default:
          return 'NVARCHAR(MAX)';
      }
    };

    // Encontrar columnas faltantes en destino
    const missingColumns = sourceColumns.filter(col =>
      !existingColumns.has(col.COLUMN_NAME.toLowerCase())
    );

    // Agregar columnas faltantes con su tipo correcto
    for (const col of missingColumns) {
      const dataType = mapDataType(col);
      const nullable = col.IS_NULLABLE === 'YES' ? 'NULL' : 'NOT NULL';
      // No agregar IDENTITY en ALTER TABLE (causaría error)

      const alterQuery = `
        ALTER TABLE ${schemaName}.${tableName}
        ADD [${col.COLUMN_NAME}] ${dataType} ${nullable}
      `;
      await this.targetConnection.exec(alterQuery);
      logger.info(`Columna '${col.COLUMN_NAME}' (${dataType}) agregada a ${schemaName}.${tableName}`);
    }

    if (missingColumns.length === 0) {
      logger.debug(`No hay columnas faltantes en ${schemaName}.${tableName}`);
    }

    // ✅ Marcar tabla como verificada para no repetir en próximos ciclos
    this.tablesColumnsVerified.add(tableKey);
  }

  interpretOperation(operationCode) {
    switch (operationCode) {
      case 1: return 'DELETE';
      case 2: return 'INSERT';
      case 3: return 'UPDATE_BEFORE';
      case 4: return 'UPDATE_AFTER';
      default: return 'UNKNOWN';
    }
  }
}

class ConnectionRunner {
  constructor(initialConn, config, logger, name = 'db') {
    this.conn = initialConn;       // instancia de tedious.Connection
    this.config = config;          // { server, database, userName, password }
    this.logger = logger;
    this.name = name;

    this._queue = Promise.resolve();
    this._isConnecting = false;

    // Configuración de reintentos
    this.MAX_RETRIES = 5;
    this.BASE_DELAY_MS = 1000;  // 1 segundo inicial
    this.MAX_DELAY_MS = 30000;  // máximo 30 segundos

    // engancha eventos si viene conexión viva
    if (this.conn) this._attachEvents(this.conn);
  }

  _attachEvents(conn) {
    conn.on('end', () => {
      this.logger?.warn?.(`[${this.name}] conexión END - se reestablecerá automáticamente`);
      this.conn = null; // Marcar como desconectado para forzar reconexión
    });
    conn.on('error', (err) => {
      this.logger?.error?.(`[${this.name}] error de conexión: ${err && err.message}`);
      this.conn = null; // Marcar como desconectado para forzar reconexión
    });
  }

  /**
   * Verifica si un error es de conexión y se puede reintentar
   */
  _isConnectionError(err) {
    if (!err) return false;
    const message = err.message?.toLowerCase() || '';
    const code = err.code?.toLowerCase() || '';

    return (
      message.includes('connection') ||
      message.includes('socket') ||
      message.includes('timeout') ||
      message.includes('econnreset') ||
      message.includes('econnrefused') ||
      message.includes('epipe') ||
      message.includes('network') ||
      message.includes('closed') ||
      message.includes('not connected') ||
      message.includes('loggedin') ||
      code === 'esocket' ||
      code === 'econnreset' ||
      code === 'econnrefused' ||
      code === 'etimedout'
    );
  }

  /**
   * Calcula el delay con backoff exponencial
   */
  _getRetryDelay(attempt) {
    const delay = Math.min(
      this.BASE_DELAY_MS * Math.pow(2, attempt),
      this.MAX_DELAY_MS
    );
    // Agregar jitter aleatorio (±20%)
    const jitter = delay * 0.2 * (Math.random() - 0.5);
    return Math.floor(delay + jitter);
  }

  async _ensureConnected() {
    // Si ya está en LoggedIn, OK
    if (this.conn && this.conn.state && this.conn.state.name === 'LoggedIn') return;

    if (this._isConnecting) {
      // espera a que otro intento termine
      await new Promise(resolve => setTimeout(resolve, 200));
      return this._ensureConnected();
    }

    this._isConnecting = true;

    let lastError = null;

    for (let attempt = 0; attempt < this.MAX_RETRIES; attempt++) {
      try {
        // si había una conexión rota, deséchala
        if (this.conn) {
          try {
            this.conn.close();
          } catch (e) {
            // Ignorar errores al cerrar
          }
        }
        this.conn = null;

        const cfg = {
          server: this.config.server,
          authentication: {
            type: 'default',
            options: { userName: this.config.userName, password: this.config.password },
          },
          options: {
            database: this.config.database,
            encrypt: false,
            trustServerCertificate: true,
            appName: `cdc-sync-${this.name}`,
            connectTimeout: 30000,      // 30 segundos timeout de conexión
            requestTimeout: 300000,     // 5 minutos timeout de request
          },
        };

        if (attempt > 0) {
          this.logger?.warn?.(`[${this.name}] reintento ${attempt}/${this.MAX_RETRIES - 1} conectando a ${cfg.server}/${cfg.options.database}...`);
        } else {
          this.logger?.info?.(`[${this.name}] conectando a ${cfg.server}/${cfg.options.database}...`);
        }

        this.conn = new Connection(cfg);
        this._attachEvents(this.conn);

        await new Promise((resolve, reject) => {
          this.conn.on('connect', (err) => err ? reject(err) : resolve());
        });

        if (this.conn.state.name !== 'LoggedIn') {
          throw new Error(`Estado ${this.conn.state.name}, no LoggedIn`);
        }

        this.logger?.info?.(`[${this.name}] conectado exitosamente`);
        this._isConnecting = false;
        return; // Conexión exitosa

      } catch (err) {
        lastError = err;
        this.logger?.error?.(`[${this.name}] error de conexión (intento ${attempt + 1}/${this.MAX_RETRIES}): ${err.message}`);

        if (attempt < this.MAX_RETRIES - 1) {
          const delay = this._getRetryDelay(attempt);
          this.logger?.info?.(`[${this.name}] esperando ${delay}ms antes de reintentar...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }

    this._isConnecting = false;
    throw new Error(`[${this.name}] No se pudo conectar después de ${this.MAX_RETRIES} intentos. Último error: ${lastError?.message}`);
  }

  // Serializa tareas en la cola
  _run(fn) {
    this._queue = this._queue.then(fn, fn);
    return this._queue.catch(err => {
      // reset cola para no atascarla
      this._queue = Promise.resolve();
      throw err;
    });
  }

  /**
   * Ejecuta una operación con reintentos automáticos si hay error de conexión
   */
  async _executeWithRetry(operation, operationType = 'query') {
    let lastError = null;

    for (let attempt = 0; attempt < this.MAX_RETRIES; attempt++) {
      try {
        await this._ensureConnected();
        return await operation();
      } catch (err) {
        lastError = err;

        if (this._isConnectionError(err)) {
          this.logger?.warn?.(`[${this.name}] error de conexión en ${operationType} (intento ${attempt + 1}/${this.MAX_RETRIES}): ${err.message}`);

          // Marcar conexión como inválida
          this.conn = null;

          if (attempt < this.MAX_RETRIES - 1) {
            const delay = this._getRetryDelay(attempt);
            this.logger?.info?.(`[${this.name}] esperando ${delay}ms antes de reintentar ${operationType}...`);
            await new Promise(resolve => setTimeout(resolve, delay));
            continue;
          }
        }

        // Si no es error de conexión o ya no hay más reintentos, propagar el error
        throw err;
      }
    }

    throw lastError;
  }

  // SELECT o DDL que devuelve filas
  async query(sql, params = []) {
    return this._run(async () => {
      return this._executeWithRetry(async () => {
        return new Promise((resolve, reject) => {
          const rows = [];
          let req;
          if (typeof sql !== 'string')
            req = sql;
          else {
            req = new Request(sql, (err) => err ? reject(err) : resolve(rows));
            params.forEach(p => req.addParameter(p.name, p.type, p.value));
            req.on('row', cols => {
              const o = {};
              Object.values(cols).forEach(c => o[c.metadata.colName] = c.value);
              rows.push(o);
            });
          }
          this.conn.execSql(req);
        });
      }, 'query');
    });
  }

  // DML (INSERT/UPDATE/DELETE) o cualquier non-query
  async exec(sql, params = []) {
    return this._run(async () => {
      return this._executeWithRetry(async () => {
        return new Promise((resolve, reject) => {
          const req = new Request(sql, (err, rowCount) => err ? reject(err) : resolve(rowCount));
          params.forEach(p => req.addParameter(p.name, p.type, p.value));
          this.conn.execSql(req);
        });
      }, 'exec');
    });
  }

  // Ejecutar un Request pre-armado (útil para parámetros tipados dinámicos)
  async execRequest(requestFactory) {
    return this._run(async () => {
      return this._executeWithRetry(async () => {
        const req = requestFactory(Request, TYPES);
        return new Promise((resolve, reject) => {
          this.conn.execSql(req);
          req.on('requestCompleted', resolve);
          req.on('error', reject);
        });
      }, 'execRequest');
    });
  }
}
module.exports = SyncService;
