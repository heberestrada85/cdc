// src/services/cdcService.js
const { Request, TYPES } = require('tedious');
const logger = require('../utils/logger');

class CDCService {
  constructor(connection) {
    this.connection = connection;
    this.lastLSN = null;
    this.cdcTables = new Map();
  }

  async enableCDC(tableName, schemaName = 'dbo') {
    const query = `
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = '${tableName}' AND is_tracked_by_cdc = 1)
      BEGIN
        EXEC sys.sp_cdc_enable_table
          @source_schema = N'${schemaName}',
          @source_name = N'${tableName}',
          @role_name = NULL,
          @supports_net_changes = 1
      END
    `;

    return this.executeQuery(query);
  }

  async getLastLSN() {
    const query = 'SELECT sys.fn_cdc_get_max_lsn() AS max_lsn';
    const result = await this.executeQuery(query);
    return result[0]?.max_lsn;
  }

  async getTableChanges(tableName, schemaName = 'dbo', fromLSN = null, toLSN = null) {
    if (!fromLSN) {
      fromLSN = await this.getMinValidLSN(tableName, schemaName);
    }
    if (!toLSN) {
      toLSN = await this.getLastLSN();
    }

    // Si alguno de los LSN sigue nulo, no se puede llamar
    if (!fromLSN || !toLSN) {
      throw new Error(`No se pudo obtener LSN válido para ${schemaName}.${tableName}`);
    }

    const query = `
      SELECT
        __$operation AS operation,
        __$start_lsn AS start_lsn,
        __$seqval AS seqval,
        __$update_mask AS update_mask,
        *
      FROM cdc.fn_cdc_get_all_changes_${schemaName}_${tableName}(@from_lsn, @to_lsn, @row_filter)
      ORDER BY __$start_lsn, __$seqval
    `;

    return this.executeQuery(query, [
      { name: 'from_lsn', type: TYPES.Binary, value: fromLSN },
      { name: 'to_lsn', type: TYPES.Binary, value: toLSN },
      { name: 'row_filter', type: TYPES.NVarChar, value: 'all' }
    ]);
  }

  async getMinValidLSN(tableName, schemaName = 'dbo') {
    const query = `
      SELECT sys.fn_cdc_get_min_lsn('${schemaName}_${tableName}') AS min_lsn
    `;
    const result = await this.executeQuery(query);
    return result[0]?.min_lsn;
  }

  async executeQuery(query, parameters = []) {
    return new Promise((resolve, reject) => {
      const request = new Request(query, (err, rowCount) => {
        if (err) {
          reject(err);
        }
      });

      const results = [];
      request.on('row', (columns) => {
        const row = {};
        //console.log('[DEBUG] columns:', JSON.stringify(columns, null, 2));

        Object.entries(columns).forEach(([key, col]) => {
          const value = col.value;
          const metadata = col.metadata;
          //console.log(`Column: ${key}, Value: ${value}, Type: ${metadata.type.name}`);
        });

        Object.values(columns).forEach(column => {
          row[column.metadata.colName] = column.value;
        });
        results.push(row);
      });

      request.on('requestCompleted', () => {
        resolve(results);
      });

      parameters.forEach(param => {
        request.addParameter(param.name, param.type, param.value);
      });

      this.connection.query(request);
    });
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

module.exports = CDCService;
