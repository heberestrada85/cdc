// src/utils/logger.js
const winston = require('winston');

// Formato personalizado: timestamp primero, luego el mensaje
const customFormat = winston.format.printf(({ level, message, timestamp }) => {
  // Formatear timestamp a hora local legible
  const ts = new Date(timestamp).toLocaleTimeString('es-MX', {
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
  return `${ts} ${level}: ${message}`;
});

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  defaultMeta: { service: 'cdc-sync' },
  transports: [
    new winston.transports.File({ filename: 'logs/error.log', level: 'error' }),
    new winston.transports.File({ filename: 'logs/combined.log' }),
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.colorize(),
        customFormat
      )
    })
  ]
});

module.exports = logger;
