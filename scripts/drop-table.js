// Script para eliminar tabla destino y forzar recreación
require('dotenv').config();
const dbConfig = require('../src/services/database');
const logger = require('../src/utils/logger');

async function dropTable() {
  try {
    logger.info('Conectando a la base de datos destino...');

    const targetConnection = await dbConfig.createConnection(
      'target',
      process.env.TARGET_DB_SERVER,
      process.env.TARGET_DB_NAME,
      process.env.TARGET_DB_USER,
      process.env.TARGET_DB_PASSWORD || "HCq$9ynmF@V!%04P0u6#"
    );

    logger.info('Eliminando tabla dbo.Empleados...');

    const { Request } = require('tedious');
    const dropQuery = `
      IF OBJECT_ID('dbo.Empleados', 'U') IS NOT NULL
      BEGIN
        DROP TABLE dbo.Empleados
        SELECT 'Tabla eliminada exitosamente' AS resultado
      END
      ELSE
      BEGIN
        SELECT 'Tabla no existe' AS resultado
      END
    `;

    return new Promise((resolve, reject) => {
      const request = new Request(dropQuery, (err, rowCount) => {
        if (err) {
          logger.error('Error eliminando tabla:', err);
          reject(err);
        } else {
          logger.info('Operación completada');
          resolve();
        }
      });

      request.on('row', (columns) => {
        const row = {};
        Object.values(columns).forEach(column => {
          row[column.metadata.colName] = column.value;
        });
        logger.info(row.resultado);
      });

      targetConnection.execSql(request);
    });

  } catch (error) {
    logger.error('Error:', error);
    process.exit(1);
  }
}

dropTable().then(() => {
  logger.info('✅ Tabla eliminada. Ahora reinicia la aplicación con: npm start');
  process.exit(0);
}).catch(err => {
  logger.error('❌ Error:', err);
  process.exit(1);
});
