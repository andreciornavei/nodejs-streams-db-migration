import fs from "fs";
import _ from "lodash";
import knex from "knex";
import { Transform } from "stream";

// <!-- create an instance to source database -->
const sourceDatabase = knex({
  client: "mysql",
  connection: {
    host: "127.0.0.1",
    port: 3306,
    user: "root",
    password: "admin",
    database: "source_database",
  },
});

// <!-- execute a query stream to start receiving data -->
const readtableStream = sourceDatabase
  .select(["id", "name", "email"])
  .from("bulk_table")
  .stream();

// <!-- create a function to transform data from source pattern to target pattern
const dataTransformer = new Transform({
  writableObjectMode: true,
  transform(chunk, _encoding, callback) {
    callback(
      null,
      [
        "A",
        _.get(chunk, "id"),
        _.get(chunk, "name"),
        _.get(chunk, "email"),
      ].join("|")
    );
  },
});

// <!-- create a function to print data transformation
const dataPrinter = new Transform({
  writableObjectMode: true,
  transform(chunk, _encoding, callback) {
    console.log(String(chunk));
    callback(null, chunk);
  },
});

// <!-- create a writable stream to write output data into a file
const writableStream = fs.createWriteStream("output.txt");

// <!-- pipe ETL streams on readable stream
readtableStream.pipe(dataTransformer).pipe(dataPrinter).pipe(writableStream);

// <!-- finish application when ETL finishes
readtableStream.on("end", () => {
  console.log("finished");
  process.exit(0);
});
