const express = require('express');
const { Kafka } = require('kafkajs');
const { Client } = require('pg');
const redis = require('redis');

const app = express();
app.use(express.json());

// Set up Kafka
const kafka = new Kafka({
  clientId: 'loan-app-service',
  brokers: ['kafka:9092']
});
const producer = kafka.producer();

// PostgreSQL client
const pgClient = new Client({
  user: 'user',
  host: 'postgres',
  database: 'loandb',
  password: 'password',
  port: 5432,
});
pgClient.connect();

// Redis client
const redisClient = redis.createClient({
  host: 'redis',
  port: 6379
});
redisClient.on('error', (err) => console.log('Redis Client Error', err));
redisClient.connect();

// JWT middleware (simplified)
const authenticateJWT = (req, res, next) => {
  // ... JWT validation logic
  next();
};

// Submit loan application
app.post('/loan-applications', authenticateJWT, async (req, res) => {
  const { customerId, loanAmount, purpose } = req.body;

  // Check cache for customer details
  let customer = await redisClient.get(`customer:${customerId}`);
  if (!customer) {
    // Fetch from PostgreSQL if not in cache
    const result = await pgClient.query('SELECT * FROM customers WHERE id = $1', [customerId]);
    customer = result.rows[0];
    // Cache the customer for 1 hour
    await redisClient.setEx(`customer:${customerId}`, 3600, JSON.stringify(customer));
  } else {
    customer = JSON.parse(customer);
  }

  // Insert loan application into PostgreSQL
  const insertAppQuery = 'INSERT INTO loan_applications (customer_id, loan_amount, purpose) VALUES ($1, $2, $3) RETURNING id';
  const insertResult = await pgClient.query(insertAppQuery, [customerId, loanAmount, purpose]);
  const appId = insertResult.rows[0].id;

  // Publish event to Kafka
  await producer.connect();
  await producer.send({
    topic: 'loan-applications',
    messages: [
      { value: JSON.stringify({ appId, customerId, loanAmount, purpose, timestamp: new Date() }) }
    ],
  });
  await producer.disconnect();

  res.status(201).json({ applicationId: appId });
});

app.listen(3000, () => console.log('Loan Application Service running on port 3000'));
