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
  }

  async isTargetTableEmpty(tableName, schemaName) {
    const query = `SELECT TOP 1 1 as existsFlag FROM ${schemaName}.${tableName}`;
    const result = await new (require('../config/cdcService'))(this.targetConnection).executeQuery(query);
    console.log('[DEBUG] isTargetTableEmpty query:', result);
    return result.length === 0;
  }


  async snapshotInitial(tableName, schemaName) {
    logger.info(`Realizando snapshot inicial para ${schemaName}.${tableName}...`);

    // Obtener todos los registros de la tabla origen
    const sourceService = new (require('../config/cdcService'))(this.sourceConnection);
    const rows = await sourceService.executeQuery(`SELECT * FROM ${schemaName}.${tableName}`);

    for (const row of rows) {
      // Aplicar reglas de negocio antes de insertar
      const processedData = await this.businessRules.applyRules(row, tableName, 'INSERT');
      if (!processedData) {
        logger.debug(`Registro filtrado por reglas de negocio en snapshot para ${tableName}`);
        continue;
      }

      // Insertar en destino
      await this.handleInsert(processedData, tableName, schemaName);
    }

    logger.info(`Snapshot inicial completado para ${schemaName}.${tableName}`);
  }

  async syncTable(tableName, schemaName = 'dbo') {
    try {
      await this.ensureTableAndColumnsExist(tableName, schemaName, {});

      const isEmpty = await this.isTargetTableEmpty(tableName, schemaName);

      if (isEmpty) {
        logger.info(`Tabla destino ${schemaName}.${tableName} vacía → ejecutando snapshot inicial`);
        await this.snapshotInitial(tableName, schemaName);
        return; // No procesar CDC en este arranque
      }

      // CDC normal si la tabla ya tiene datos
      const cdcService = new (require('../config/cdcService'))(this.sourceConnection);
      const lastProcessedLSN = this.syncState.get(`${schemaName}.${tableName}`) || null;

      const changes = await cdcService.getTableChanges(tableName, schemaName, lastProcessedLSN);

      if (changes.length === 0) {
        logger.debug(`No hay cambios para la tabla ${schemaName}.${tableName}`);
        return;
      }

      logger.info(`Procesando ${changes.length} cambios para ${schemaName}.${tableName}`);

      for (const change of changes) {
        await this.processChange(change, tableName, schemaName);
        this.syncState.set(`${schemaName}.${tableName}`, change.start_lsn);
      }

      logger.info(`Sincronización completada para ${schemaName}.${tableName}`);
    } catch (error) {
      logger.error(`Error sincronizando tabla ${schemaName}.${tableName}:`, error);
      throw error;
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
      const values = columns.map(col => data[col]);

      const query = `
        INSERT INTO ${schemaName}.${tableName} (${columns.join(', ')})
        VALUES (${columns.map((_, i) => `@param${i}`).join(', ')})
      `;

      // 🔹 Función para reemplazar parámetros por valores reales
      const prettyQuery = (sql, vals) => {
        let q = sql;
        vals.forEach((v, i) => {
          let val = v;
          if (Buffer.isBuffer(v)) val = `'0x${v.toString('hex')}'`;
          else if (v instanceof Date) val = `'${v.toISOString()}'`;
          else if (typeof v === 'string') val = `'${v.replace(/'/g, "''")}'`;
          else if (v === null || v === undefined) val = 'NULL';
          q = q.replace(`@param${i}`, val);
        });
        return q;
      };

      console.log('Executing INSERT:\n', prettyQuery(query, values));


      //await this.executeTargetQuery(query, values);
      await this.executeTargetQuery(`SET IDENTITY_INSERT ${schemaName}.${tableName} ON; ${prettyQuery(query, values)}; SET IDENTITY_INSERT ${schemaName}.${tableName} OFF;`);


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
          if (Buffer.isBuffer(v)) return '0x' + v.toString('hex');

          // Si viene string -> asegurar prefijo '0x'
          if (typeof v === 'string') {
            if (!v.length) return null;         // cadena vacía => NULL
            return v.startsWith('0x') ? v : ('0x' + v);
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
      console.log('Executing UPDATE:\n', prettyQuery(query, values));

      // 8) Ejecuta (tu ejecutor enviará NVARCHAR, por eso usamos CONVERT en SQL)
      await this.executeTargetQuery(prettyQuery(query, values));
      logger.debug(`UPDATE ejecutado para ${tableName}`);
    } catch (error) {
      logger.error(`Error en UPDATE para ${tableName}:`, error);
      throw error;
    }
  }

  async getTableColumns(tableName, schemaName) {
    const cdcService = new (require('../config/cdcService'))(this.sourceConnection);

    const query = `
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schemaName}'
        AND TABLE_NAME = '${tableName}'
    `;

    const result = await cdcService.executeQuery(query);
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
    const cdcService = new (require('../config/cdcService'))(this.sourceConnection);

    const query = `SELECT c.name AS COLUMN_NAME
      FROM sys.columns c
      JOIN sys.objects o ON o.object_id = c.object_id
      WHERE o.type = 'U' AND o.name = '${tableName}' and c.is_identity = 1;`;

    const result = await cdcService.executeQuery(query);
    return result.length ? result[0].COLUMN_NAME : null;
  }

  async executeTargetQuery(query, parameters = []) {
    const { Request, TYPES } = require('tedious');

    const detectTediousType = (val) => {
      if (val === null || val === undefined) return TYPES.NVarChar;
      if (typeof val === 'string') return TYPES.NVarChar;
      if (typeof val === 'number') {
        return Number.isInteger(val) ? TYPES.Int : TYPES.Float;
      }
      if (val instanceof Date) return TYPES.DateTime;
      if (Buffer.isBuffer(val)) return TYPES.VarBinary;
      if (typeof val === 'boolean') return TYPES.Bit;
      if (typeof val === 'object' && 'value' in val) {
        return detectTediousType(val.value);
      }
      return TYPES.NVarChar;
    };

    const normalizeValue = (val) => {
      if (val && typeof val === 'object' && 'value' in val) {
        return val.value;
      }
      return val;
    };

    return new Promise(async (resolve, reject) => {
      if (this.targetConnection.state !== this.targetConnection.STATE.LoggedIn) {
        console.log('Connection not logged in, attempting to reconnect...');
        await this.reconnectTarget();
      }

      const request = new Request(query, (err, rowCount) => {
        if (err) {
          reject(err);
        } else {
          resolve(rowCount);
        }
      });

      parameters.forEach((param, index) => {
        const cleanVal = normalizeValue(param);
        const type = detectTediousType(cleanVal);
        request.addParameter(`param${index}`, type, cleanVal);
      });

      this.targetConnection.query(request);
    });

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
          console.error('Error reconnecting to target database:', err);
          reject(err);
        } else {
          console.log('Successfully reconnected to target database');
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
    const cdcService = new (require('../config/cdcService'))(this.targetConnection);

    // 1️⃣ Verificar si la tabla existe
    const tableCheckQuery = `
      SELECT COUNT(*) AS count
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = '${schemaName}' AND TABLE_NAME = '${tableName}'
    `;
    const tableCheck = await cdcService.executeQuery(tableCheckQuery);

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

      const cdcServiceSource = new (require('../config/cdcService'))(this.sourceConnection);

      // Consultar columnas con tipos y si son identidad
      const columnInfoQuery = `
        SELECT
          c.COLUMN_NAME,
          c.DATA_TYPE,
          c.CHARACTER_MAXIMUM_LENGTH,
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

      const sourceColumns = await cdcServiceSource.executeQuery(columnInfoQuery);

      if (!sourceColumns.length) {
        throw new Error(`No se encontraron columnas en la tabla origen ${schemaName}.${tableName}`);
      }

      // Mapear tipo de datos SQL
      const mapDataType = (dataType, maxLength) => {
        switch (dataType.toLowerCase()) {
          case 'nvarchar':
          case 'varchar':
          case 'char':
          case 'nchar':
            return `${dataType.toUpperCase()}(${maxLength > 0 ? maxLength : 'MAX'})`;
          case 'decimal':
          case 'numeric':
            return `${dataType.toUpperCase()}(18, 4)`;
          case 'datetime2':
          case 'datetime':
          case 'date':
          case 'time':
          case 'int':
          case 'bigint':
          case 'smallint':
          case 'bit':
          case 'float':
          case 'real':
          case 'uniqueidentifier':
          case 'text':
          case 'ntext':
          case 'money':
          case 'smallmoney':
          case 'binary':
          case 'varbinary':
            return dataType.toUpperCase();
          default:
            return 'NVARCHAR(MAX)';
        }
      };

      // Construcción dinámica de columnas
      let columns = sourceColumns.map(col => {
        const dataType = mapDataType(col.DATA_TYPE, col.CHARACTER_MAXIMUM_LENGTH);
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

      console.log('Creating table with query:\n', createTableQuery);
      await cdcService.executeQuery(createTableQuery);
      logger.info(`Tabla ${schemaName}.${tableName} creada exitosamente`);
      return;
    }


    const colQuery = `
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schemaName}' AND TABLE_NAME = '${tableName}'
    `;
    const colResult = await cdcService.executeQuery(colQuery);
    const existingColumns = colResult.map(r => r.COLUMN_NAME);
    const cdcMetadataColumns = new Set(['operation', 'start_lsn', 'seqval', 'update_mask']);

    const missingColumns = Object.keys(data)
      .filter(col => !col.startsWith('__$'))
      .filter(col => !existingColumns.includes(col))
      .filter(col => !cdcMetadataColumns.has(col));

    for (const col of missingColumns) {
      const alterQuery = `
        ALTER TABLE ${schemaName}.${tableName}
        ADD [${col}] NVARCHAR(MAX) NULL
      `;
      await cdcService.executeQuery(alterQuery);
      logger.info(`Columna '${col}' agregada a ${schemaName}.${tableName}`);
    }
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

    // engancha eventos si viene conexión viva
    if (this.conn) this._attachEvents(this.conn);
  }

  _attachEvents(conn) {
    conn.on('end', () => this.logger?.warn?.(`[${this.name}] conexión END`));
    conn.on('error', (err) => this.logger?.error?.(
      `[${this.name}] error de conexión: ${err && err.message}`
    ));
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
    try {
      // si había una conexión rota, deséchala
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
        },
      };

      this.logger?.info?.(`[${this.name}] conectando a ${cfg.server}/${cfg.options.database}...`);

      this.conn = new Connection(cfg);
      this._attachEvents(this.conn);

      await new Promise((resolve, reject) => {
        this.conn.on('connect', (err) => err ? reject(err) : resolve());
      });

      if (this.conn.state.name !== 'LoggedIn') {
        throw new Error(`Estado ${this.conn.state.name}, no LoggedIn`);
      }

      this.logger?.info?.(`[${this.name}] conectado`);
    } finally {
      this._isConnecting = false;
    }
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

  // SELECT o DDL que devuelve filas
  async query(sql, params = []) {
    return this._run(async () => {
      await this._ensureConnected();
      return new Promise((resolve, reject) => {
        const rows = [];
        let req;
        if (typeof sql !== 'string')
          req = sql;
        else{
          req = new Request(sql, (err) => err ? reject(err) : resolve(rows));
          params.forEach(p => req.addParameter(p.name, p.type, p.value));
          req.on('row', cols => {
            const o = {}; cols.forEach(c => o[c.metadata.colName] = c.value); rows.push(o);
          });
        }
        this.conn.execSql(req);
      });
    });
  }

  // DML (INSERT/UPDATE/DELETE) o cualquier non-query
  async exec(sql, params = []) {
    return this._run(async () => {
      await this._ensureConnected();
      return new Promise((resolve, reject) => {
        const req = new Request(sql, (err, rowCount) => err ? reject(err) : resolve(rowCount));
        params.forEach(p => req.addParameter(p.name, p.type, p.value));
        this.conn.execSql(req);
      });
    });
  }

  // Ejecutar un Request pre-armado (útil para parámetros tipados dinámicos)
  async execRequest(requestFactory) {
    return this._run(async () => {
      await this._ensureConnected();
      const req = requestFactory(Request, TYPES);
      return new Promise((resolve, reject) => {
        this.conn.execSql(req);
        req.on('requestCompleted', resolve);
        req.on('error', reject);
      });
    });
  }
}
module.exports = SyncService;
