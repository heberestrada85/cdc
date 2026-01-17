// src/index.js
require('dotenv').config();
const cron = require('node-cron');
const dbConfig = require('./services/database');
const SyncService = require('./services/syncService');
const logger = require('./utils/logger');
const tablesToSync = require('./config/tablesToSync');

class CDCSyncApplication {
  constructor() {
    this.syncService = null;
    this.isRunning = false;
    this.tablesToSync = tablesToSync;
  }

  async initialize() {
    try {
      logger.info('Inicializando aplicación CDC Sync...');

      // Crear conexiones
      logger.info('Conectando a base de datos origen...');
      const sourceConnection = await dbConfig.createConnection(
        'source',
        process.env.SOURCE_DB_SERVER,
        process.env.SOURCE_DB_NAME,
        process.env.SOURCE_DB_USER,
        "HCq$9ynmF@V!%04P0u6#"
      );

      logger.info('Conectando a base de datos destino...');
      const targetConnection = await dbConfig.createConnection(
        'target',
        process.env.TARGET_DB_SERVER,
        process.env.TARGET_DB_NAME,
        process.env.TARGET_DB_USER,
        "HCq$9ynmF@V!%04P0u6#"
      );

      this.syncService = new SyncService({
        conn: sourceConnection,
        config: {
          server: process.env.SOURCE_DB_SERVER,
          database: process.env.SOURCE_DB_NAME,
          userName: process.env.SOURCE_DB_USER,
          password: "HCq$9ynmF@V!%04P0u6#",
        },
      },
      {
        conn: targetConnection,
        config: {
          server: process.env.TARGET_DB_SERVER,
          database: process.env.TARGET_DB_NAME,
          userName: process.env.TARGET_DB_USER,
          password: "HCq$9ynmF@V!%04P0u6#",
        },
      });

      logger.info('Conexiones establecidas exitosamente');

      // Habilitar CDC en todas las tablas configuradas
      await this.setupCDC();

      return true;
    } catch (error) {
      logger.error('Error inicializando aplicación:', error);
      return false;
    }
  }

  /**
   * Habilita CDC en la base de datos y en todas las tablas configuradas
   */
  async setupCDC() {
    // 1. Primero habilitar CDC a nivel de base de datos (una sola vez)
    logger.info('');
    logger.info('╔══════════════════════════════════════════════════════════════╗');
    logger.info('║           CONFIGURANDO CDC EN BASE DE DATOS                  ║');
    logger.info('╚══════════════════════════════════════════════════════════════╝');
    logger.info('');

    await this.syncService.ensureDatabaseCDCEnabled();

    // 2. Luego habilitar CDC en cada tabla
    logger.info('');
    logger.info(`Habilitando CDC en ${this.tablesToSync.length} tablas...`);
    logger.info('');

    let enabledCount = 0;
    let skippedCount = 0;
    let errorCount = 0;

    for (let i = 0; i < this.tablesToSync.length; i++) {
      const table = this.tablesToSync[i];
      const tableIndex = `[${i + 1}/${this.tablesToSync.length}]`;

      try {
        // Verificar si la tabla existe en origen
        const exists = await this.syncService.checkSourceTableExists(table.name, table.schema);
        if (!exists) {
          logger.warn(`${tableIndex} ⚠️  ${table.schema}.${table.name} NO EXISTE en origen. Omitiendo...`);
          skippedCount++;
          continue;
        }

        await this.syncService.ensureCDCEnabled(table.name, table.schema);
        enabledCount++;
      } catch (error) {
        logger.error(`${tableIndex} ✗ Error habilitando CDC en ${table.schema}.${table.name}:`, error.message);
        errorCount++;
      }
    }

    logger.info('');
    logger.info(`Resumen CDC: ${enabledCount} habilitadas, ${skippedCount} no existen, ${errorCount} errores`);

    // Esperar un momento para que SQL Server cree las funciones CDC
    logger.info('Esperando a que SQL Server cree las funciones CDC...');
    await new Promise(resolve => setTimeout(resolve, 2000));

    logger.info('Configuración de CDC completada');
  }

  async startSync() {
    if (this.isRunning) {
      logger.warn('El proceso de sincronización ya está en ejecución');
      return;
    }

    this.isRunning = true;

    // ═══════════════════════════════════════════════════════════════
    // FASE 1: SNAPSHOT INICIAL
    // ═══════════════════════════════════════════════════════════════
    logger.info('');
    logger.info('╔══════════════════════════════════════════════════════════════╗');
    logger.info('║          INICIANDO SNAPSHOT INICIAL DE TABLAS                ║');
    logger.info('╚══════════════════════════════════════════════════════════════╝');
    logger.info('');

    const startTime = Date.now();
    let snapshotCount = 0;
    let errorCount = 0;

    for (let i = 0; i < this.tablesToSync.length; i++) {
      const table = this.tablesToSync[i];
      const tableIndex = `[${i + 1}/${this.tablesToSync.length}]`;

      try {
        logger.info(`${tableIndex} Procesando ${table.schema}.${table.name}...`);
        await this.syncService.syncTable(table.name, table.schema);

        // Verificar si se hizo snapshot o ya existía
        const tableKey = `${table.schema}.${table.name}`;
        if (this.syncService.syncState.get(`${tableKey}_initial_sync_done`)) {
          // Contar solo si no estaba previamente marcada
          const wasNew = !this.syncService.syncState.get(`${tableKey}_was_counted`);
          if (wasNew) {
            this.syncService.syncState.set(`${tableKey}_was_counted`, true);
            // Determinar si es nueva o existente basándose en el log
            snapshotCount++;
          }
        }
      } catch (error) {
        errorCount++;
        logger.error(`${tableIndex} Error en snapshot de ${table.schema}.${table.name}:`, error.message);
      }
    }

    const totalTime = ((Date.now() - startTime) / 1000).toFixed(2);

    // ═══════════════════════════════════════════════════════════════
    // RESUMEN DEL SNAPSHOT INICIAL
    // ═══════════════════════════════════════════════════════════════
    logger.info('');
    logger.info('╔══════════════════════════════════════════════════════════════╗');
    logger.info('║            SNAPSHOT INICIAL COMPLETADO                       ║');
    logger.info('╠══════════════════════════════════════════════════════════════╣');
    logger.info(`║  Tablas procesadas: ${this.tablesToSync.length.toString().padEnd(40)}║`);
    logger.info(`║  Tablas sincronizadas: ${snapshotCount.toString().padEnd(37)}║`);
    logger.info(`║  Errores: ${errorCount.toString().padEnd(50)}║`);
    logger.info(`║  Tiempo total: ${totalTime}s${' '.repeat(Math.max(0, 44 - totalTime.length))}║`);
    logger.info('╚══════════════════════════════════════════════════════════════╝');
    logger.info('');

    // ═══════════════════════════════════════════════════════════════
    // FASE 2: MONITOREO CDC EN TIEMPO REAL
    // ═══════════════════════════════════════════════════════════════
    logger.info('╔══════════════════════════════════════════════════════════════╗');
    logger.info('║       INICIANDO MONITOREO CDC EN TIEMPO REAL                 ║');
    logger.info('╚══════════════════════════════════════════════════════════════╝');
    logger.info('');

    // Programar sincronización cada X segundos
    const interval = parseInt(process.env.POLLING_INTERVAL, 10) || 5;

    // Usar setInterval para intervalos cortos (más preciso que cron para < 5 segundos)
    setInterval(async () => {
      await this.performSync();
    }, interval * 1000);

    logger.info(`CDC activo - Escuchando cambios cada ${interval} segundo(s)...`);
  }

  async performSync() {
    try {
      for (const table of this.tablesToSync) {
        await this.syncService.syncTable(table.name, table.schema);
      }
    } catch (error) {
      logger.error('Error durante la sincronización:', error);
    }
  }

  async stop() {
    this.isRunning = false;
    await dbConfig.closeAllConnections();
    logger.info('Aplicación detenida');
  }
}

// Manejo de señales del sistema
const app = new CDCSyncApplication();

process.on('SIGINT', async () => {
  logger.info('Recibida señal SIGINT, cerrando aplicación...');
  await app.stop();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  logger.info('Recibida señal SIGTERM, cerrando aplicación...');
  await app.stop();
  process.exit(0);
});

// Función de espera
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Iniciar aplicación con reintentos
(async () => {
  const MAX_RETRIES = Infinity; // Reintentar indefinidamente
  const INITIAL_DELAY = 5000;   // 5 segundos inicial
  const MAX_DELAY = 60000;      // Máximo 60 segundos entre reintentos

  let retryCount = 0;
  let delay = INITIAL_DELAY;

  while (true) {
    const initialized = await app.initialize();

    if (initialized) {
      retryCount = 0; // Reset contador si conecta exitosamente
      delay = INITIAL_DELAY;
      await app.startSync();
      break; // Salir del loop si startSync termina normalmente
    } else {
      retryCount++;
      logger.info(`Reintento ${retryCount}: Esperando ${delay / 1000} segundos antes de reintentar...`);
      await sleep(delay);

      // Backoff exponencial con máximo
      delay = Math.min(delay * 1.5, MAX_DELAY);
    }
  }
})();
