// src/config/database.js
const { Connection } = require('tedious');

class DatabaseConfig {
  constructor() {
    this.connections = new Map();
  }

  getConnectionConfig(server, database, user, password) {
    return {
      server: server,
      authentication: {
        type: 'default',
        options: {
          userName: user,
          password: password
        }
      },
      options: {
        database: database,
        serverName: server,                  // evita el warning TLS si usas IP
        encrypt: true,                       // encripta la conexión
        trustServerCertificate: true,        // útil para entornos de testing
        rowCollectionOnRequestCompletion: true,
        useColumnNames: true,
        connectTimeout: 30000,
        requestTimeout: 30000,
        cancelTimeout: 5000
      }
    };
  }

  async createConnection(name, server, database, user, password) {
    const config = this.getConnectionConfig(server, database, user, password);
    const connection = new Connection(config);

    return new Promise((resolve, reject) => {
      connection.on('connect', (err) => {
        if (err) {
          reject(err);
        } else {
          this.connections.set(name, connection);
          resolve(connection);
        }
      });

      connection.connect();
    });
  }

  getConnection(name) {
    return this.connections.get(name);
  }

  async closeConnection(name) {
    const connection = this.connections.get(name);
    if (connection) {
      connection.close();
      this.connections.delete(name);
    }
  }

  async closeAllConnections() {
    for (const [name, connection] of this.connections) {
      connection.close();
    }
    this.connections.clear();
  }
}

module.exports = new DatabaseConfig();
