// scripts/setup-cdc.js
require('dotenv').config();
const dbConfig = require('../src/config/database');
const CDCService = require('../src/services/cdcService');
const logger = require('../src/utils/logger');
const tablesToSync = require('../src/config/tablesToSync');

async function setupCDC() {
  try {
    logger.info('Configurando CDC en SQL Server...');

    const connection = await dbConfig.createConnection(
      'setup',
      process.env.SOURCE_DB_SERVER,
      process.env.SOURCE_DB_NAME,
      process.env.SOURCE_DB_USER,
      process.env.SOURCE_DB_PASSWORD
    );

    const cdcService = new CDCService(connection);

    // Habilitar CDC a nivel de base de datos
    await cdcService.executeQuery(`
      IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = '${process.env.SOURCE_DB_NAME}' AND is_cdc_enabled = 1)
      BEGIN
        EXEC sys.sp_cdc_enable_db
      END
    `);

    // Habilitar CDC para tablas específicas
    const tables = tablesToSync;
    for (const table of this.tablesToSync) {
      await this.syncService.syncTable(table.name, table.schema);
    }
    for (const table of tables) {

      await cdcService.enableCDC(`${table.name}.${table.schema}`);
      logger.info(`CDC habilitado para la tabla: ${table.name, table.schema}`);
    }

    logger.info('Configuración de CDC completada');
    await dbConfig.closeConnection('setup');
  } catch (error) {
    logger.error('Error configurando CDC:', error);
    process.exit(1);
  }
}

setupCDC();
