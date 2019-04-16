import fs from 'fs';


class Database {
  constructor(directory) {

    this._dir = directory;

    let files = [];

    try {
      files = fs.readdirSync(directory)
        .filter(fname => fname.endsWith('.tsv'));
    }
    catch (e) {
      fs.mkdirSync(directory);
    }

    this._tables = {};

    const parser = new DSVParser('\t');

    for (const filename of files) {

      const path = directory + '/' + filename;
      const parts = filename.split('.');
      const base = parts[0];

      const contents = fs.readFileSync(path);
      const { columns, records } = parser.parse(fs.readFileSync(path, 'utf-8'));

      if (records !== null) {
        const table = new Table(columns, records);
        this._tables[base] = table;
      }
      else {
        console.error("Invalid table file: " + path);
      }
    }
  }
  
  getTable(name) {
    return this._tables[name];
  }

  persist() {

    const writer = new DSVWriter('\t');

    for (const key in this._tables) {
      const table = this._tables[key];
      const path = this._dir + '/' + key;
      writer.write(path, table.getColumns(), table.getAll());
    }
  }
}

class Table {
  constructor(columns, records) {
    this._columns = columns;
    this._records = records;
  }

  getColumns() {
    return this._columns;
  }

  getAll() {
    return this._records;
  }

  getByColumn(col, value) {
    return this._records.filter(x => x[col] === value);
  }
}


class DSVParser {
  constructor(sep) {
    this._sep = sep || '\t';
  }

  parse(text) {
    const lines = text.split('\n');
    const columns = this.parseHeader(lines[0]);

    let data = null;

    if (columns !== null) {
      data = lines.slice(1)
        // TODO: make this filter more precise
        .filter(line => line.length > 0 && !line.startsWith('#') && !line.startsWith(this._sep))
        // TODO: make this more functional
        .map(line => {
          const values = line.split(this._sep);
          const entry = {};
          for (let i = 0; i < columns.length; i++) {
            const column = columns[i];
            entry[column.name] = this._parseValue(values[i].replace('\r', ''), column.type);
          }

          return entry;
        });
    }

    return { columns, records: data };
  }

  parseHeader(line) {

    let columns = null;

    if (line.length > 0) {
      columns = line.split(this._sep).map(col => this.parseColumnSpec(col.replace('\r', '')));
    }

    return columns;
  }

  parseColumnSpec(value) {
    const parts = value.split(': ');
    const name = parts[0];
    const type = parts[1];

    return { name, type };
  }

  _parseValue(value, type) {
    switch (type) {
      case 'Int64':
      case 'Float64':
        return Number(value);
        break;
      case 'Text':
        return String(value);
        break;
      default:
        throw new Error("Invalid type: " + type);
        break;
    }
  }
}


class DSVWriter {
  constructor(sep) {
    this._sep = sep || '\t';
  }

  write(filename, columns, records) {
    const file = fs.createWriteStream(filename);

    file.write(columns.map(col => col.name + ': ' + col.type).join(this._sep));
    file.write('\n');

    for (const entry of records) {
      const line = columns.map(col => entry[col.name]);
      file.write(line.join(this._sep));
      file.write('\n');
    }
  }
}


export { Database };
