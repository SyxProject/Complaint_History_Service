require('dotenv').config()
const express = require('express')
const amqp = require('amqplib')
const { Pool } = require('pg')

const app = express()
const port = process.env.PORT || 4000
const pool = new Pool({ connectionString: process.env.DATABASE_URL })

const QUEUE = 'status-queque'

// Guarda un mensaje en la base de datos
async function guardarMensajeEnBD(event) {
  await pool.query(
    'INSERT INTO history.complaint_history (complaint_id, description, prev_state, new_state, change_date) VALUES ($1, $2, $3, $4, $5)',
    [event.complaintId, event.description, event.prevState, event.newState, event.changeDate]
  )
}

// Se pone al dia con los registros no guardados
async function procesarMensajesIniciales(channel) {
  const { rows } = await pool.query('SELECT * FROM history.complaint_history ORDER BY change_date DESC LIMIT 1')
  const ultimoRegistro = rows[0]

  // Leer todos los mensajes de la cola
  const mensajes = []
  let msg
  do {
    msg = await channel.get(QUEUE, { noAck: true })
    if (msg) mensajes.push(msg)
  } while (msg)

  if (!ultimoRegistro) {
    for (const m of mensajes) {
      const event = JSON.parse(m.content.toString())
      await guardarMensajeEnBD(event)
    }
    return
  }

  // Buscar coincidencia exacta con el último registro de base de datos
  let indiceCoincidencia = mensajes.findIndex(m => {
    const event = JSON.parse(m.content.toString())
    return (
      event.complaintId === ultimoRegistro.complaint_id &&
      event.description === ultimoRegistro.description &&
      event.prevState === ultimoRegistro.prev_state &&
      event.newState === ultimoRegistro.new_state &&
      event.changeDate === ultimoRegistro.change_date.toISOString()
    )
  })

  if (indiceCoincidencia !== -1) {
    for (let i = indiceCoincidencia + 1; i < mensajes.length; i++) {
      const event = JSON.parse(mensajes[i].content.toString())
      await guardarMensajeEnBD(event)
    }
    return
  }

  // Si no encuentra coincidencia busca por fecha igual o posterior más cercana
  let indiceFecha = mensajes.findIndex(m => {
    const event = JSON.parse(m.content.toString())
    return event.changeDate >= ultimoRegistro.change_date.toISOString()
  })

  if (indiceFecha !== -1) {
    for (let i = indiceFecha; i < mensajes.length; i++) {
      const event = JSON.parse(mensajes[i].content.toString())
      await guardarMensajeEnBD(event)
    }
  } else {
    for (const m of mensajes) {
      const event = JSON.parse(m.content.toString())
      await guardarMensajeEnBD(event)
    }
  }
}

// Borra los 25 mensajes más viejos si hay más de 50 en la cola
async function limpiarCola(channel) {
  const queueInfo = await channel.checkQueue(QUEUE)
  if (queueInfo.messageCount > 50) {
    for (let i = 0; i < 25; i++) {
      await channel.get(QUEUE, { noAck: true })
    }
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
      console.log('[', new Date().toLocaleString(), ']: Evento consumido y almacenado en la base de datos.')
      console.log('Mensaje del evento:', event)
    }
  }, { noAck: true })
}

// 3. Inicia el microservicio y el consumidor
app.listen(port, () => {
  console.log(`Event Sourcing Service escuchando en puerto ${port}`)
  startConsumer().catch(console.error)
})