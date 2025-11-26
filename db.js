const { Client } = require("pg");

const db = new Client({
  user: "postgres",
  host: "localhost",
  database: "spotify_app",
  password: "password",
  port: 5432,
});

db.connect();

module.exports = db;

