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
      logger.info('source');
      console.log('[DEBUG] SOURCE CONFIG:', {
        server: process.env.SOURCE_DB_SERVER,
        database: process.env.SOURCE_DB_NAME,
        user: process.env.SOURCE_DB_USER,
        password: "HCq$9ynmF@V!%04P0u6#"
      });
      const sourceConnection = await dbConfig.createConnection(
        'source',
        process.env.SOURCE_DB_SERVER,
        process.env.SOURCE_DB_NAME,
        process.env.SOURCE_DB_USER,
        "HCq$9ynmF@V!%04P0u6#"
      );

      logger.info('target');
      console.log('[DEBUG] TARGET CONFIG:', {
        server: process.env.TARGET_DB_SERVER,
        database: process.env.TARGET_DB_NAME,
        user: process.env.TARGET_DB_USER,
        password: "HCq$9ynmF@V!%04P0u6#"
      });
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
      return true;
    } catch (error) {
      logger.error('Error inicializando aplicación:', error);
      return false;
    }
  }

  async startSync() {
    if (this.isRunning) {
      logger.warn('El proceso de sincronización ya está en ejecución');
      return;
    }

    this.isRunning = true;
    logger.info('Iniciando sincronización...');

    // Programar sincronización cada X segundos
    const interval = process.env.POLLING_INTERVAL || 5; // Valor por defecto de 5000 ms (5 segundos)
    cron.schedule(`*/${interval} * * * * *`, async () => {
      await this.performSync();
    });

    logger.info(`Sincronización programada cada ${interval} segundos`);
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

// Iniciar aplicación
(async () => {
  const initialized = await app.initialize();
  if (initialized) {
    await app.startSync();
  } else {
    process.exit(1);
  }
})();
