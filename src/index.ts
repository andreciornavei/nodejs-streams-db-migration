import fs from "fs";
import _ from "lodash";
import knex from "knex";
import { Transform } from "stream";
import { EOL } from "os";
import { cleanpipe } from "./utils/cleanpipe";

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

// <!-- create a function to transform data from source pattern to target pattern -->
// <!-- * must to use cleanpipe on each value to prevent output pattern brokens -->
const dataTransformer = new Transform({
  writableObjectMode: true,
  transform(chunk, _encoding, callback) {
    callback(
      null,
      [
        "A",
        cleanpipe(_.get(chunk, "id")),
        cleanpipe(_.get(chunk, "name")),
        cleanpipe(_.get(chunk, "email")),
      ].join("|")
    );
  },
});

// <!-- create a function to append EOL for each tranformed line
const eolTransformer = new Transform({
  writableObjectMode: true,
  transform(chunk, _encoding, callback) {
    console.log(String(chunk));
    callback(null, chunk + EOL);
  },
});

// <!-- create a writable stream to write output data into a file
const writableStream = fs.createWriteStream("output.txt");

// <!-- pipe ETL streams on readable stream
readtableStream.pipe(dataTransformer).pipe(eolTransformer).pipe(writableStream);

// <!-- finish application when ETL finishes
writableStream.on("finish", () => {
  console.log("finished");
  process.exit(0);
});
