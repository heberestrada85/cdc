/**
 * Servicio de Auditoría para Replicación CDC
 *
 * Registra cada operación de replicación en la tabla dbo.AuditoriaReplicacion
 * para facilitar la depuración y el monitoreo.
 *
 * Características:
 * - Registro asíncrono (no bloquea operaciones principales)
 * - Buffer de escritura para optimizar rendimiento
 * - Manejo de errores silencioso (no falla la replicación si falla el log)
 * - Soporte para operaciones batch
 */

const { TYPES } = require('tedious');
const logger = require('../utils/logger');

class AuditoriaService {
  constructor(connectionRunner, options = {}) {
    this.connection = connectionRunner;
    this.processId = options.processId || `PID-${process.pid}-${Date.now()}`;
    this.appVersion = options.appVersion || '1.0.0';
    this.sourceDb = options.sourceDb || process.env.SOURCE_DB_NAME || 'TadaNomina';
    this.targetDb = options.targetDb || process.env.TARGET_DB_NAME || 'TadaNomina-2.0';

    // Buffer para escritura batch
    this.buffer = [];
    this.bufferSize = options.bufferSize || 50;      // Registros antes de flush
    this.flushInterval = options.flushInterval || 5000; // ms entre flushes
    this.isEnabled = options.enabled !== false;      // Habilitado por defecto

    // Iniciar flush periódico
    if (this.isEnabled) {
      this._startPeriodicFlush();
    }

    // Estadísticas en memoria
    this.stats = {
      totalRegistros: 0,
      exitosos: 0,
      errores: 0,
      omitidos: 0,
      tiempoTotalMs: 0
    };
  }

  /**
   * Inicia el flush periódico del buffer
   */
  _startPeriodicFlush() {
    this._flushTimer = setInterval(async () => {
      await this.flush();
    }, this.flushInterval);

    // Permitir que el proceso termine aunque el timer esté activo
    if (this._flushTimer.unref) {
      this._flushTimer.unref();
    }
  }

  /**
   * Detiene el servicio de auditoría
   */
  async stop() {
    if (this._flushTimer) {
      clearInterval(this._flushTimer);
    }
    await this.flush();
  }

  /**
   * Escapa strings para SQL de forma segura
   */
  _escapeSql(str, maxLength = null) {
    if (str === null || str === undefined) return 'NULL';
    let escaped = String(str).replace(/'/g, "''");
    if (maxLength && escaped.length > maxLength) {
      escaped = escaped.substring(0, maxLength - 3) + '...';
    }
    return `N'${escaped}'`;
  }

  /**
   * Registra una operación de replicación
   *
   * @param {Object} params - Parámetros de la operación
   * @param {string} params.tabla - Nombre de la tabla
   * @param {string} params.schema - Schema de la tabla (default: dbo)
   * @param {string} params.tipoOperacion - INSERT, UPDATE, DELETE, MERGE, etc.
   * @param {number} params.codigoCDC - Código de operación CDC (1-4)
   * @param {string} params.registroId - ID del registro afectado
   * @param {string} params.estado - SUCCESS, ERROR, SKIPPED, RETRY
   * @param {Object} params.datosAntes - Datos anteriores (para UPDATE/DELETE)
   * @param {Object} params.datosDespues - Datos nuevos (para INSERT/UPDATE)
   * @param {number} params.tiempoMs - Tiempo de ejecución en ms
   * @param {number} params.registrosProcesados - Número de registros (para batch)
   * @param {string} params.mensajeError - Mensaje de error si aplica
   * @param {number} params.codigoError - Código de error SQL si aplica
   * @param {Buffer} params.lsnInicio - LSN de inicio
   * @param {Buffer} params.lsnFin - LSN de fin
   * @param {Object} params.metadata - Metadata adicional
   */
  async registrar(params) {
    if (!this.isEnabled) return;

    try {
      const registro = {
        fechaHora: new Date(),
        procesoId: this.processId,
        baseDatosOrigen: this.sourceDb,
        baseDatosDestino: this.targetDb,
        esquemaTabla: params.schema || 'dbo',
        nombreTabla: params.tabla,
        tipoOperacion: params.tipoOperacion,
        codigoOperacionCDC: params.codigoCDC || null,
        clavesPrimarias: params.clavesPrimarias || null,
        registroId: params.registroId ? String(params.registroId) : null,
        estado: params.estado || 'SUCCESS',
        codigoError: params.codigoError || null,
        mensajeError: params.mensajeError || null,
        datosAntes: params.datosAntes ? JSON.stringify(params.datosAntes) : null,
        datosDespues: params.datosDespues ? JSON.stringify(params.datosDespues) : null,
        tiempoEjecucionMs: params.tiempoMs || null,
        registrosProcesados: params.registrosProcesados || 1,
        bytesProcesados: params.bytesProcesados || null,
        lsnInicio: params.lsnInicio || null,
        lsnFin: params.lsnFin || null,
        versionApp: this.appVersion,
        metadata: params.metadata ? JSON.stringify(params.metadata) : null
      };

      // Actualizar estadísticas
      this.stats.totalRegistros++;
      if (registro.estado === 'SUCCESS') this.stats.exitosos++;
      else if (registro.estado === 'ERROR') this.stats.errores++;
      else if (registro.estado === 'SKIPPED') this.stats.omitidos++;
      if (registro.tiempoEjecucionMs) {
        this.stats.tiempoTotalMs += registro.tiempoEjecucionMs;
      }

      // Agregar al buffer
      this.buffer.push(registro);

      // Flush si el buffer está lleno
      if (this.buffer.length >= this.bufferSize) {
        await this.flush();
      }
    } catch (error) {
      // No fallar si el logging falla
      logger.debug(`Error registrando auditoría: ${error.message}`);
    }
  }

  /**
   * Registra un INSERT
   */
  async registrarInsert(tabla, registroId, datos, tiempoMs, options = {}) {
    await this.registrar({
      tabla,
      tipoOperacion: 'INSERT',
      codigoCDC: 2,
      registroId,
      estado: 'SUCCESS',
      datosDespues: datos,
      tiempoMs,
      ...options
    });
  }

  /**
   * Registra un UPDATE
   */
  async registrarUpdate(tabla, registroId, datosAntes, datosDespues, tiempoMs, options = {}) {
    await this.registrar({
      tabla,
      tipoOperacion: 'UPDATE',
      codigoCDC: 4,
      registroId,
      estado: 'SUCCESS',
      datosAntes,
      datosDespues,
      tiempoMs,
      ...options
    });
  }

  /**
   * Registra un DELETE
   */
  async registrarDelete(tabla, registroId, datosAntes, tiempoMs, options = {}) {
    await this.registrar({
      tabla,
      tipoOperacion: 'DELETE',
      codigoCDC: 1,
      registroId,
      estado: 'SUCCESS',
      datosAntes,
      tiempoMs,
      ...options
    });
  }

  /**
   * Registra una operación MERGE masiva
   */
  async registrarMerge(tabla, registrosProcesados, tiempoMs, resumen, options = {}) {
    await this.registrar({
      tabla,
      tipoOperacion: 'MERGE_ALL',
      registrosProcesados,
      estado: 'SUCCESS',
      tiempoMs,
      metadata: resumen,
      ...options
    });
  }

  /**
   * Registra un error
   */
  async registrarError(tabla, tipoOperacion, registroId, error, options = {}) {
    await this.registrar({
      tabla,
      tipoOperacion,
      registroId,
      estado: 'ERROR',
      codigoError: error.number || null,
      mensajeError: error.message || String(error),
      ...options
    });
  }

  /**
   * Registra una operación omitida (por reglas de negocio, etc.)
   */
  async registrarOmitido(tabla, tipoOperacion, registroId, razon, options = {}) {
    await this.registrar({
      tabla,
      tipoOperacion,
      registroId,
      estado: 'SKIPPED',
      mensajeError: razon,
      ...options
    });
  }

  /**
   * Escribe el buffer a la base de datos
   */
  async flush() {
    if (this.buffer.length === 0) return;

    const registros = [...this.buffer];
    this.buffer = [];

    try {
      // Construir INSERT masivo
      const values = registros.map(r => {
        const lsnInicioHex = r.lsnInicio ? `0x${Buffer.from(r.lsnInicio).toString('hex')}` : 'NULL';
        const lsnFinHex = r.lsnFin ? `0x${Buffer.from(r.lsnFin).toString('hex')}` : 'NULL';

        return `(
          ${this._escapeSql(r.procesoId, 100)},
          ${this._escapeSql(r.baseDatosOrigen, 128)},
          ${this._escapeSql(r.baseDatosDestino, 128)},
          ${this._escapeSql(r.esquemaTabla, 128)},
          ${this._escapeSql(r.nombreTabla, 128)},
          ${this._escapeSql(r.tipoOperacion, 20)},
          ${r.codigoOperacionCDC || 'NULL'},
          ${this._escapeSql(r.clavesPrimarias, 500)},
          ${this._escapeSql(r.registroId, 100)},
          ${this._escapeSql(r.estado, 20)},
          ${r.codigoError || 'NULL'},
          ${this._escapeSql(r.mensajeError, 4000)},
          ${this._escapeSql(r.datosAntes, 8000)},
          ${this._escapeSql(r.datosDespues, 8000)},
          ${r.tiempoEjecucionMs || 'NULL'},
          ${r.registrosProcesados || 1},
          ${r.bytesProcesados || 'NULL'},
          ${lsnInicioHex},
          ${lsnFinHex},
          ${this._escapeSql(r.versionApp, 50)},
          ${this._escapeSql(r.metadata, 4000)}
        )`;
      });

      const query = `
        INSERT INTO dbo.AuditoriaReplicacion (
          ProcesoId,
          BaseDatosOrigen,
          BaseDatosDestino,
          EsquemaTabla,
          NombreTabla,
          TipoOperacion,
          CodigoOperacionCDC,
          ClavesPrimarias,
          RegistroId,
          Estado,
          CodigoError,
          MensajeError,
          DatosAntes,
          DatosDespues,
          TiempoEjecucionMs,
          RegistrosProcesados,
          BytesProcesados,
          LSN_Inicio,
          LSN_Fin,
          VersionApp,
          Metadata
        )
        VALUES ${values.join(',\n')}
      `;

      await this.connection.exec(query);
      logger.debug(`Auditoría: ${registros.length} registros escritos`);
    } catch (error) {
      // Si falla el flush, intentar escribir uno por uno
      logger.warn(`Error en flush de auditoría: ${error.message}. Intentando uno por uno...`);

      for (const r of registros) {
        try {
          await this._escribirRegistroIndividual(r);
        } catch (individualError) {
          logger.debug(`Error escribiendo registro de auditoría: ${individualError.message}`);
        }
      }
    }
  }

  /**
   * Escribe un registro individual (fallback si falla el batch)
   */
  async _escribirRegistroIndividual(r) {
    const lsnInicioHex = r.lsnInicio ? `0x${Buffer.from(r.lsnInicio).toString('hex')}` : 'NULL';
    const lsnFinHex = r.lsnFin ? `0x${Buffer.from(r.lsnFin).toString('hex')}` : 'NULL';

    const query = `
      INSERT INTO dbo.AuditoriaReplicacion (
        ProcesoId, BaseDatosOrigen, BaseDatosDestino, EsquemaTabla, NombreTabla,
        TipoOperacion, CodigoOperacionCDC, ClavesPrimarias, RegistroId, Estado,
        CodigoError, MensajeError, DatosAntes, DatosDespues, TiempoEjecucionMs,
        RegistrosProcesados, BytesProcesados, LSN_Inicio, LSN_Fin, VersionApp, Metadata
      )
      VALUES (
        ${this._escapeSql(r.procesoId, 100)},
        ${this._escapeSql(r.baseDatosOrigen, 128)},
        ${this._escapeSql(r.baseDatosDestino, 128)},
        ${this._escapeSql(r.esquemaTabla, 128)},
        ${this._escapeSql(r.nombreTabla, 128)},
        ${this._escapeSql(r.tipoOperacion, 20)},
        ${r.codigoOperacionCDC || 'NULL'},
        ${this._escapeSql(r.clavesPrimarias, 500)},
        ${this._escapeSql(r.registroId, 100)},
        ${this._escapeSql(r.estado, 20)},
        ${r.codigoError || 'NULL'},
        ${this._escapeSql(r.mensajeError, 4000)},
        ${this._escapeSql(r.datosAntes, 8000)},
        ${this._escapeSql(r.datosDespues, 8000)},
        ${r.tiempoEjecucionMs || 'NULL'},
        ${r.registrosProcesados || 1},
        ${r.bytesProcesados || 'NULL'},
        ${lsnInicioHex},
        ${lsnFinHex},
        ${this._escapeSql(r.versionApp, 50)},
        ${this._escapeSql(r.metadata, 4000)}
      )
    `;

    await this.connection.exec(query);
  }

  /**
   * Obtiene estadísticas del servicio
   */
  getStats() {
    return {
      ...this.stats,
      bufferActual: this.buffer.length,
      tiempoPromedio: this.stats.totalRegistros > 0
        ? Math.round(this.stats.tiempoTotalMs / this.stats.totalRegistros)
        : 0
    };
  }

  /**
   * Consulta registros de auditoría
   *
   * @param {Object} filtros - Filtros de consulta
   * @param {string} filtros.tabla - Filtrar por tabla
   * @param {string} filtros.estado - Filtrar por estado
   * @param {Date} filtros.desde - Fecha desde
   * @param {Date} filtros.hasta - Fecha hasta
   * @param {number} filtros.limit - Límite de registros
   */
  async consultar(filtros = {}) {
    const conditions = ['1=1'];

    if (filtros.tabla) {
      conditions.push(`NombreTabla = ${this._escapeSql(filtros.tabla)}`);
    }
    if (filtros.estado) {
      conditions.push(`Estado = ${this._escapeSql(filtros.estado)}`);
    }
    if (filtros.tipoOperacion) {
      conditions.push(`TipoOperacion = ${this._escapeSql(filtros.tipoOperacion)}`);
    }
    if (filtros.desde) {
      conditions.push(`FechaHora >= '${filtros.desde.toISOString()}'`);
    }
    if (filtros.hasta) {
      conditions.push(`FechaHora <= '${filtros.hasta.toISOString()}'`);
    }
    if (filtros.registroId) {
      conditions.push(`RegistroId = ${this._escapeSql(filtros.registroId)}`);
    }

    const limit = filtros.limit || 100;

    const query = `
      SELECT TOP ${limit}
        AuditoriaId,
        FechaHora,
        NombreTabla,
        TipoOperacion,
        RegistroId,
        Estado,
        MensajeError,
        TiempoEjecucionMs,
        RegistrosProcesados
      FROM dbo.AuditoriaReplicacion
      WHERE ${conditions.join(' AND ')}
      ORDER BY FechaHora DESC
    `;

    return await this.connection.query(query);
  }

  /**
   * Obtiene resumen de errores recientes
   */
  async getErroresRecientes(limit = 10) {
    return await this.consultar({
      estado: 'ERROR',
      limit
    });
  }

  /**
   * Obtiene estadísticas agregadas de la base de datos
   */
  async getEstadisticasDB(desde = null) {
    const whereClause = desde
      ? `WHERE FechaHora >= '${desde.toISOString()}'`
      : '';

    const query = `
      SELECT
        NombreTabla,
        TipoOperacion,
        Estado,
        COUNT(*) as Total,
        AVG(TiempoEjecucionMs) as TiempoPromedioMs,
        SUM(RegistrosProcesados) as TotalRegistros,
        MIN(FechaHora) as PrimeraOperacion,
        MAX(FechaHora) as UltimaOperacion
      FROM dbo.AuditoriaReplicacion
      ${whereClause}
      GROUP BY NombreTabla, TipoOperacion, Estado
      ORDER BY Total DESC
    `;

    return await this.connection.query(query);
  }
}

module.exports = AuditoriaService;
