// src/services/businessRules.js
const logger = require('../utils/logger');
const fs = require('fs');
const path = require('path');

class BusinessRules {
  constructor() {
    this.rules = new Map();
    this.loadRules();
  }

  loadRules() {
    try {
      const rulesPath = path.join(__dirname, 'rules');
      const ruleFiles = fs.readdirSync(rulesPath)
        .filter(file => file.endsWith('Rules.js') && file !== 'baseRule.js');

      ruleFiles.forEach(file => {
        try {
          const RuleClass = require(`./rules/${file}`);
          const tableName = file.replace('Rules.js', '').toLowerCase();
          const ruleInstance = new RuleClass();

          this.rules.set(tableName, ruleInstance.getRules());
          logger.info(`Reglas cargadas para la tabla: ${tableName}`);
        } catch (error) {
          logger.error(`Error cargando reglas de ${file}:`, error);
        }
      });
    } catch (error) {
      logger.error('Error al cargar las reglas:', error);
    }
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
    const tableRules = this.rules.get(tableName.toLowerCase());
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
