// src/services/businessRules.js
const logger = require('../utils/logger');

class BusinessRules {
  constructor() {
    this.rules = new Map();
    this.setupDefaultRules();
  }

  setupDefaultRules() {
    // Regla para filtrar registros inactivos
    this.addRule('clientes', 'estatus', (data, operation) => {
      if (data.status === 'inactive' && operation === 'INSERT') {
        return null; // Filtrar registro
      }
      return data;
    });

    // Regla para transformar datos
    this.addRule('clientes', 'estatus', (data, operation) => {
      if (data.status && operation !== 'DELETE') {
        data.status = 2
      }
      return data;
    });

    this.addRule('Empleados', 'PassKiosko', (data, operation) => {
      if (data.status && operation !== 'UPDATE') {
        data.status = 2
      }
      return data;
    });

    // Regla para auditoría
    this.addRule('*', 'estatus', (data, operation) => {
      if (operation !== 'DELETE') {
        data.modifica = new Date().toISOString();
        data.status = 2;
      }
      return data;
    });
  }

  addRule(tableName, ruleName, ruleFunction) {
    if (!this.rules.has(tableName)) {
      this.rules.set(tableName, new Map());
    }
    this.rules.get(tableName).set(ruleName, ruleFunction);
  }

  async applyRules(data, tableName, operation) {
    let processedData = { ...data };

    // Aplicar reglas específicas de la tabla
    const tableRules = this.rules.get(tableName);
    if (tableRules) {
      for (const [ruleName, ruleFunction] of tableRules) {
        try {
          processedData = await ruleFunction(processedData, operation);
          if (!processedData) {
            logger.debug(`Regla ${ruleName} filtró el registro de ${tableName}`);
            return null;
          }
        } catch (error) {
          logger.error(`Error aplicando regla ${ruleName} en ${tableName}:`, error);
        }
      }
    }

    // Aplicar reglas globales
    const globalRules = this.rules.get('*');
    if (globalRules) {
      for (const [ruleName, ruleFunction] of globalRules) {
        try {
          processedData = await ruleFunction(processedData, operation);
          if (!processedData) {
            logger.debug(`Regla global ${ruleName} filtró el registro de ${tableName}`);
            return null;
          }
        } catch (error) {
          logger.error(`Error aplicando regla global ${ruleName}:`, error);
        }
      }
    }

    return processedData;
  }

  removeRule(tableName, ruleName) {
    const tableRules = this.rules.get(tableName);
    if (tableRules) {
      tableRules.delete(ruleName);
    }
  }

  listRules(tableName) {
    const tableRules = this.rules.get(tableName);
    return tableRules ? Array.from(tableRules.keys()) : [];
  }
}

module.exports = BusinessRules;
