const csv = require("csv-parser");
const fs = require("fs");

const pgtools = require("pgtools");
const path = require("path");
const promise = require("bluebird");

const pgp = require("pg-promise")({
  promiseLib: promise
});

const readSqlFile = file => {
  const fullPath = path.join(__dirname, "dados", file);
  return new pgp.QueryFile(fullPath, { minify: true });
};

const modelagem = {
  edgv3pro: readSqlFile("edgv_30_pro.sql"),
  edgv213pro: readSqlFile("edgv_213_pro.sql")
};

const createDatabase = async (
  dbUser,
  dbPassword,
  dbPort,
  dbServer,
  dbName,
  modelagem
) => {
  const config = {
    user: dbUser,
    password: dbPassword,
    port: dbPort,
    host: dbServer
  };

  await pgtools.createdb(config, dbName);

  const connectionString = `postgres://${dbUser}:${dbPassword}@${dbServer}:${dbPort}/${dbName}`;

  const db = pgp(connectionString);
  await db.none(modelagem);
};

const bancos = [];

fs.createReadStream("./dados/bancos_trabalho.csv")
  .pipe(csv())
  .on("data", row => {
    bancos.push(row);
  })
  .on("end", async () => {
    try {
      for (const banco of bancos) {
        await createDatabase(
          "postgres",
          "postgres",
          "5432",
          "localhost",
          banco["nome_db"],
          modelagem[banco["nome_modelagem"]]
        );
        console.log(
          `Banco ${banco["nome_db"]} criado com a modelagem ${banco["nome_modelagem"]}`
        );
      }
      console.log("Todos os bancos criados com sucesso");
    } catch (e) {
      console.log("Erro na criação de bancos");
      console.log(e.message);
    }
  });
