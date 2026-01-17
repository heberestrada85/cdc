// src/services/cdcService.js
const { Request, TYPES } = require('tedious');
const logger = require('../utils/logger');

class CDCService {
  /**
   * @param {Object} connectionRunner - Instancia de ConnectionRunner (no la conexión raw)
   */
  constructor(connectionRunner) {
    this.connectionRunner = connectionRunner;
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

    return this.connectionRunner.query(query);
  }

  async getLastLSN() {
    const query = 'SELECT sys.fn_cdc_get_max_lsn() AS max_lsn';
    const result = await this.connectionRunner.query(query);
    return result[0]?.max_lsn;
  }

  async getTableChanges(tableName, schemaName = 'dbo', fromLSN = null, toLSN = null) {
    // Si tenemos un fromLSN (último procesado), necesitamos incrementarlo
    // para no re-procesar el mismo cambio
    const originalFromLSN = fromLSN;

    if (fromLSN) {
      const incrementResult = await this.connectionRunner.query(
        'SELECT sys.fn_cdc_increment_lsn(@lsn) AS next_lsn',
        [{ name: 'lsn', type: TYPES.Binary, value: fromLSN }]
      );
      fromLSN = incrementResult[0]?.next_lsn;
    }

    if (!fromLSN) {
      fromLSN = await this.getMinValidLSN(tableName, schemaName);
    }
    if (!toLSN) {
      toLSN = await this.getLastLSN();
    }

    // Debug log para Empleados
    if (tableName === 'Empleados') {
      logger.info(`[CDC getTableChanges] ${schemaName}.${tableName} - fromLSN: ${fromLSN ? Buffer.from(fromLSN).toString('hex') : 'null'}, toLSN: ${toLSN ? Buffer.from(toLSN).toString('hex') : 'null'}`);
    }

    // Si alguno de los LSN sigue nulo, no se puede llamar
    if (!fromLSN || !toLSN) {
      throw new Error(`No se pudo obtener LSN válido para ${schemaName}.${tableName}`);
    }

    // Si fromLSN > toLSN, no hay cambios nuevos
    const compareResult = await this.connectionRunner.query(
      'SELECT CASE WHEN @from_lsn > @to_lsn THEN 1 ELSE 0 END AS is_greater',
      [
        { name: 'from_lsn', type: TYPES.Binary, value: fromLSN },
        { name: 'to_lsn', type: TYPES.Binary, value: toLSN }
      ]
    );
    if (compareResult[0]?.is_greater === 1) {
      if (tableName === 'Empleados') {
        logger.info(`[CDC getTableChanges] ${schemaName}.${tableName} - fromLSN > toLSN, no hay cambios nuevos`);
      }
      return []; // No hay cambios nuevos
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

    return this.connectionRunner.query(query, [
      { name: 'from_lsn', type: TYPES.Binary, value: fromLSN },
      { name: 'to_lsn', type: TYPES.Binary, value: toLSN },
      { name: 'row_filter', type: TYPES.NVarChar, value: 'all' }
    ]);
  }

  async getMinValidLSN(tableName, schemaName = 'dbo') {
    const query = `
      SELECT sys.fn_cdc_get_min_lsn('${schemaName}_${tableName}') AS min_lsn
    `;
    const result = await this.connectionRunner.query(query);
    return result[0]?.min_lsn;
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
