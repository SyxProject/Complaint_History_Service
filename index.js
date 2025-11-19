require('dotenv').config()
const express = require('express')
const amqp = require('amqplib')
const { Pool } = require('pg')

const app = express()
const port = process.env.PORT || 4000
const pool = new Pool({ connectionString: process.env.DATABASE_URL })

const QUEUE = 'status-queque'

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
      await pool.query(
        'INSERT INTO history.complaint_history (complaint_id, description ,prev_state, new_state, change_date) VALUES ($1, $2, $3, $4, $5)',
        [event.complaintId, event.description, event.prevState, event.newState, event.changeDate]
      )
      channel.ack(msg) // Borra el mensaje de la cola
      console.log('[',new Date().toLocaleString(),']: Evento consumido y almacenado en la base de datos.')
      console.log('Mensaje del evento:', event)
    }
  })
}

// 3. Inicia el microservicio y el consumidor
app.listen(port, () => {
  console.log(`Event Sourcing Service escuchando en puerto ${port}`)
  startConsumer().catch(console.error)
})