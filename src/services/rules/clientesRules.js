// src/services/rules/clientesRules.js
const BaseRule = require('./baseRule');
const logger = require('../../utils/logger');

class ClientesRules extends BaseRule {
  constructor() {
    super();
    this.setupRules();
  }

  setupRules() {
    // Regla para forzar id_estatus=1 en clientes
    this.rules.set('forzar_estatus_activo', (data, operation) => {
      if (operation !== 'DELETE') {
        data.id_estatus = 1;
        logger.debug('Establecido id_estatus=1 para cliente');
      }
      return data;
    });

    // Regla para filtrar registros inactivos
    this.rules.set('filtrar_inactivos', (data, operation) => {
      if (data.status === 'inactive' && operation === 'INSERT') {
        logger.debug('Cliente inactivo filtrado');
        return null;
      }
      return data;
    });

    // Regla para transformar datos de estatus
    this.rules.set('transformar_estatus', (data, operation) => {
      if (data.status && operation !== 'DELETE') {
        data.status = 2;
      }
      return data;
    });
  }
}

module.exports = ClientesRules;
