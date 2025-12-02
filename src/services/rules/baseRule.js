// src/services/rules/baseRule.js
class BaseRule {
  constructor() {
    this.rules = new Map();
  }

  getRules() {
    return this.rules;
  }
}

module.exports = BaseRule;
