# ms-pedidos360-notify

Microservicio de **notificaciones** de Pedidos360. No es público: es un **consumidor de RabbitMQ** que procesa comandos de forma asíncrona.

## Topología (6 colas, 3 flujos)

| Cola principal | Propósito | DLQ |
|---|---|---|
| `q.cmd.email` | Email / push al cliente con el estado del pedido | `q.cmd.email.dlq` |
| `q.cmd.kitchen` | Ticket de cocina al aceptar un pedido | `q.cmd.kitchen.dlq` |
| `q.cmd.invoice` | Boleta al entregar un pedido | `q.cmd.invoice.dlq` |

| Exchange | Tipo | Bindings |
|---|---|---|
| `cmd.direct` | direct | `email.send`, `kitchen.ticket`, `invoice.gen` |
| `cmd.topic` | topic | `email.*`, `kitchen.#`, `invoice.*` |
| `cmd.dead.dlx` | direct | DLQ de cada cola |

## Buenas prácticas implementadas

- **Envelope común:** `type`, `eventId`, `timestamp`, `traceId`, `correlationId` y `payload`.
- **ACK/NACK explícitos** (`acknowledge-mode: manual`):
  - Si el comando se procesa bien → `basicAck`.
  - Si es inválido o falla → `basicNack` sin reencolar, y RabbitMQ lo mueve a su DLQ.
- **Idempotencia:** un `eventId` ya procesado se confirma sin repetir el efecto.
- **Métricas:** contador `pedidos360.notify.commands{queue, result=ok|duplicate|dlq}` en `/actuator/metrics`. La tasa de DLQ también se ve en la Management UI.

El envío real (SMTP, Web Push, impresora, PDF) se simula con logs.

## Variables de entorno

| Variable | Por defecto |
|---|---|
| `RABBITMQ_HOST` | `localhost` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USER` | `pedidos360` |
| `RABBITMQ_PASSWORD` | `pedidos360` |

## Ejecutar

```bash
./mvnw test
./mvnw spring-boot:run
```

## Autores

Germán Maraboli & Camila Vera

Proyecto Pedidos360 · DSY1107 Desarrollo Cloud Native I · Duoc UC
