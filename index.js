require('dotenv').config()
const express = require('express')
const amqp = require('amqplib')
const { Pool } = require('pg')

const app = express()
const port = process.env.PORT || 4000
const pool = new Pool({ connectionString: process.env.DATABASE_URL })

const QUEUE = 'status-queque'

// Traducción de estados
function traducirEstado(estado) {
  switch (estado) {
    case 'OPEN':
      return 'Abierta'
    case 'CLOSED':
      return 'Cerrada'
    case 'UNDER_REVIEW':
      return 'En revisión'
    default:
      return estado
  }
}

// 1. Consumidor de RabbitMQ
async function startConsumer() {
  const conn = await amqp.connect(process.env.RABBITMQ_URL)
  const channel = await conn.createChannel()
  await channel.assertQueue(QUEUE, { durable: true })

  // Procesa mensajes iniciales
  await procesarMensajesIniciales(channel)

  // Consume nuevos mensajes sin borrarlos
  channel.consume(QUEUE, async (msg) => {
    if (msg !== null) {
      const event = JSON.parse(msg.content.toString())
      await guardarMensajeEnBD(event)
      await limpiarCola(channel)
      console.log('Evento guardado:', event)
      // No se borra el mensaje con ack
    }
  }, { noAck: true })
}

// 2. Endpoint para obtener todos los registros
app.get('/events', async (req, res) => {
  const result = await pool.query('SELECT * FROM complaint_events ORDER BY timestamp DESC')
  res.json(result.rows)
})

// 3. Inicia el microservicio y el consumidor
app.listen(port, () => {
  console.log(`Event Sourcing Service escuchando en puerto ${port}`)
  startConsumer().catch(console.error)
})