const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const dotenv = require('dotenv');
const axios = require('axios');
const path = require('path');

dotenv.config();

const app = express();
const PORT = 33400;

app.use(cors());
app.use(express.json());

const dbPath = path.resolve(__dirname, 'ecommerce.db');
const db = new sqlite3.Database(dbPath, (err) => {
  if (err) console.error('❌ DB connection error:', err.message);
  else console.log('✅ Connected to SQLite database');
});

// Health check
app.get('/', (req, res) => {
  res.send('✅ API is working! Available endpoints: /users, /products, /orders, /order-items, /inventory, /distribution-centers');
});

// General GET endpoint helper
const createEndpoint = (route, table) => {
  app.get(route, (req, res) => {
    db.all(`SELECT * FROM ${table}`, (err, rows) => {
      if (err) return res.status(500).json({ error: 'Internal server error' });
      res.json(rows);
    });
  });
};

createEndpoint('/users', 'users');
createEndpoint('/products', 'products');
createEndpoint('/orders', 'orders');
createEndpoint('/order-items', 'order_items');
createEndpoint('/inventory', 'inventory_items');
createEndpoint('/distribution-centers', 'distribution_centers');

// Core chat endpoint
app.post('/api/chat', (req, res) => {
  const { user_id, message } = req.body;
  if (!user_id || !message) return res.status(400).json({ error: 'Missing user_id or message' });

  db.run(`INSERT INTO conversations (user_id) VALUES (?)`, [user_id], function (err) {
    if (err) return res.status(500).json({ error: 'DB error in conversation' });

    const conversationId = this.lastID;

    // === Groq API Integration ===
    const groqKey = process.env.GROQ_API_KEY;
    const headers = {
      'Authorization': `Bearer ${groqKey}`,
      'Content-Type': 'application/json',
    };

    const body = {
      model: 'llama3-8b-8192',
      messages: [
        { role: 'user', content: message }
      ]
    };

    axios.post('https://api.groq.com/openai/v1/chat/completions', body, { headers })
      .then(response => {
        const aiResponse = response.data.choices[0].message.content;

        db.run(
          `INSERT INTO chat_messages (conversation_id, user_message, ai_response) VALUES (?, ?, ?)`,
          [conversationId, message, aiResponse],
          (err) => {
            if (err) return res.status(500).json({ error: 'DB error in chat message insert' });
            res.json({ conversation_id: conversationId, user_message: message, ai_response: aiResponse });
          }
        );
      })
      .catch(error => {
        console.error('Groq API error:', error.response?.data || error.message);
        res.status(500).json({ error: 'Failed to fetch response from Groq API' });
      });
  });
});

app.listen(PORT, () => {
  console.log(`✅ Server is running on http://localhost:${PORT}`);
});
