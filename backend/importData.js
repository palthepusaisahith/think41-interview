const Database = require('better-sqlite3');
const csv = require('csvtojson');
const fs = require('fs');
const path = require('path');

const db = new Database('ecommerce.db');

// Drop existing tables if any and recreate fresh
db.exec(`
  DROP TABLE IF EXISTS users;
  DROP TABLE IF EXISTS orders;
  DROP TABLE IF EXISTS order_items;
  DROP TABLE IF EXISTS inventory_items;
  DROP TABLE IF EXISTS distribution_centers;
  DROP TABLE IF EXISTS products;

  CREATE TABLE users (
    id TEXT, first_name TEXT, last_name TEXT, email TEXT, age INTEGER,
    gender TEXT, state TEXT, street_address TEXT, postal_code TEXT,
    city TEXT, country TEXT, latitude REAL, longitude REAL, traffic_source TEXT, created_at TEXT
  );

  CREATE TABLE orders (
    order_id TEXT, user_id TEXT, status TEXT, gender TEXT,
    created_at TEXT, returned_at TEXT, shipped_at TEXT,
    delivered_at TEXT, num_of_item INTEGER
  );

  CREATE TABLE order_items (
    id TEXT, order_id TEXT, user_id TEXT, product_id TEXT,
    inventory_item_id TEXT, status TEXT, created_at TEXT,
    shipped_at TEXT, delivered_at TEXT, returned_at TEXT, sale_price REAL
  );

  CREATE TABLE inventory_items (
    id TEXT, product_id TEXT, created_at TEXT, sold_at TEXT, cost REAL,
    product_category TEXT, product_name TEXT, product_brand TEXT,
    product_retail_price REAL, product_department TEXT, product_sku TEXT,
    product_distribution_center_id TEXT
  );

  CREATE TABLE distribution_centers (
    id TEXT, name TEXT, latitude REAL, longitude REAL
  );

  CREATE TABLE products (
    id TEXT, cost REAL, retail_price REAL, department TEXT, sku TEXT
  );
`);

async function importCSV(filename, table, columns) {
  const filePath = path.join(__dirname, 'data', filename);
  if (!fs.existsSync(filePath)) {
    console.error(`❌ File not found: ${filePath}`);
    return;
  }

  const jsonArray = await csv().fromFile(filePath);
  const insertQuery = `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${columns.map(c => `@${c}`).join(', ')})`;
  const insert = db.prepare(insertQuery);
  const insertMany = db.transaction((rows) => {
    for (const row of rows) {
      const cleanedRow = {};
      for (const col of columns) {
        cleanedRow[col] = row[col] ?? null;
      }
      insert.run(cleanedRow);
    }
  });

  try {
    insertMany(jsonArray);
    console.log(`✅ Imported ${jsonArray.length} rows into ${table}`);
  } catch (err) {
    console.error(`❌ Failed to import ${table}:`, err.message);
  }
}

// Sequentially import all datasets
(async () => {
  console.log("⏳ Starting import...");

  await importCSV('users.csv', 'users', [
    'id', 'first_name', 'last_name', 'email', 'age',
    'gender', 'state', 'street_address', 'postal_code', 'city',
    'country', 'latitude', 'longitude', 'traffic_source', 'created_at'
  ]);

  await importCSV('orders.csv', 'orders', [
    'order_id', 'user_id', 'status', 'gender', 'created_at',
    'returned_at', 'shipped_at', 'delivered_at', 'num_of_item'
  ]);

  await importCSV('order_items.csv', 'order_items', [
    'id', 'order_id', 'user_id', 'product_id', 'inventory_item_id',
    'status', 'created_at', 'shipped_at', 'delivered_at', 'returned_at', 'sale_price'
  ]);

  await importCSV('inventory_items.csv', 'inventory_items', [
    'id', 'product_id', 'created_at', 'sold_at', 'cost',
    'product_category', 'product_name', 'product_brand',
    'product_retail_price', 'product_department', 'product_sku',
    'product_distribution_center_id'
  ]);

  await importCSV('distribution_centers.csv', 'distribution_centers', [
    'id', 'name', 'latitude', 'longitude'
  ]);

  await importCSV('products.csv', 'products', [
    'id', 'cost', 'retail_price', 'department', 'sku'
  ]);

  console.log("✅ All data imported successfully!");
})();
