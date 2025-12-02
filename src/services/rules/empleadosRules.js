// src/services/rules/empleadosRules.js
const BaseRule = require('./baseRule');

class EmpleadosRules extends BaseRule {
  constructor() {
    super();
    this.setupRules();
  }

  setupRules() {
    this.rules.set('PassKiosko', (data, operation) => {
      if (data.status && operation !== 'UPDATE') {
        data.status = 2;
      }
      return data;
    });
  }
}

module.exports = EmpleadosRules;
