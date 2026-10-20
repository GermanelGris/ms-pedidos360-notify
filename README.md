# ms-pedidos360-notify

Microservicio **consumidor de RabbitMQ** de Pedidos360. Procesa de forma asíncrona los comandos que publica `ms-pedidos360-orders`: aviso al cliente, ticket de cocina y boleta. No expone API pública.

## Topología

Todos los nombres viven en `application.yml` (`pedidos360.rabbitmq.*`) y se leen con `RabbitMessagingProperties`. En el código no hay nombres de colas ni de exchanges escritos a mano.

| Caso de uso | Routing key | Cola principal | Cola de reintento (TTL 5 s) | DLQ | Configuración |
|---|---|---|---|---|---|
| Aviso al cliente (email/push) | `email.send` | `q.cmd.email` | `q.cmd.email.retry` | `q.cmd.email.dlq` | `config/EmailRabbitConfig` |
| Ticket de cocina | `kitchen.ticket` | `q.cmd.kitchen` | `q.cmd.kitchen.retry` | `q.cmd.kitchen.dlq` | `config/KitchenRabbitConfig` |
| Boleta | `invoice.gen` | `q.cmd.invoice` | `q.cmd.invoice.retry` | `q.cmd.invoice.dlq` | `config/InvoiceRabbitConfig` |

| Exchange | Tipo | Función |
|---|---|---|
| `cmd.direct` | direct | Entrada de los comandos hacia la cola principal |
| `cmd.retry` | direct | Recibe los mensajes que se reintentan; la cola `.retry` los devuelve a `cmd.direct` cuando vence su TTL |
| `cmd.dead.dlx` | direct | Dead-letter exchange: los NACK de la cola principal llegan a la DLQ |

```
orders ──email.send──> cmd.direct ──> q.cmd.email ──> EmailCommandListener
                                          │  ▲
                        NACK (requeue=false)│  │ TTL vencido
                                          ▼  │
                      cmd.dead.dlx ──> q.cmd.email.dlq     cmd.retry ──> q.cmd.email.retry
```

## Estructura (por dominio)

| Paquete | Contenido |
|---|---|
| `config` | Propiedades y beans `Queue` / `Exchange` / `Binding` (un archivo por caso de uso). Solo configuración |
| `messaging` | Infraestructura común: `CommandAcknowledger` (ACK/NACK/reintentos), envelope, idempotencia y excepciones |
| `email` | `EmailCommandListener` (`@RabbitListener`) y `EmailNotificationService` |
| `kitchen` | `KitchenTicketListener` y `KitchenTicketService` |
| `invoice` | `InvoiceListener` e `InvoiceService` |

Los servicios de negocio reciben un `CommandEnvelope` y no conocen RabbitMQ.

## Confirmación y manejo de errores

`acknowledge-mode: manual`. La política es la misma para todos los consumidores y está en `CommandAcknowledger`:

| Situación | Tipo | Acción |
|---|---|---|
| Procesado correctamente | — | `basicAck` |
| `eventId` ya procesado | duplicado | `basicAck` sin repetir el efecto (idempotencia) |
| JSON inválido, sin `eventId` o falta un campo obligatorio | **no recuperable** | `basicNack(requeue=false)`: pasa a la DLQ de inmediato |
| Falla temporal (SMTP, impresora o servicio de boletas caídos) | **recuperable** | Se republica en la cola `.retry` con el header `x-retry-count` y se hace `basicAck` del original. Cuando vence el TTL vuelve a la cola principal |
| Falla recuperable en el último intento (`max-attempts: 3`) | agotado | `basicNack(requeue=false)`: DLQ |
| No se pudo publicar el reintento | — | `basicNack(requeue=false)`: DLQ |

- Nunca se usa `requeue=true`, porque un mensaje que siempre falla quedaría en un bucle.
- Cada envío a la DLQ se registra en el log con nivel `ERROR`, incluyendo la cola y el motivo. También queda en la métrica `pedidos360.notify.commands{queue, result=ok|duplicate|retry|dlq}` (`/actuator/metrics`).
- Para la demo, si el payload trae `"simulateFailure": "transient"` se simula una falla recuperable, y con `"fatal"` una no recuperable. Se puede enviar desde el administrador (`POST /api/mq/messages`).

```
WARN  [q.cmd.email] Intento 1/3 fallido, se reintenta en 5000 ms vía q.cmd.email.retry: Servidor SMTP no disponible
WARN  [q.cmd.email] Intento 2/3 fallido, se reintenta en 5000 ms vía q.cmd.email.retry: ...
ERROR [q.cmd.email] Intento 3/3 fallido, sin reintentos disponibles, NACK -> q.cmd.email.dlq: ...
ERROR [q.cmd.kitchen] Mensaje no recuperable, NACK -> q.cmd.kitchen.dlq: Falta el campo obligatorio 'items'
```

## Listener de las DLQ

`messaging/DeadLetterListener` consume `q.cmd.email.dlq`, `q.cmd.kitchen.dlq` y `q.cmd.invoice.dlq`. Por cada mensaje muerto registra con nivel `ERROR` la cola de origen, el motivo y la cantidad de veces (header `x-death`: `rejected`, `expired` o `maxlen`), el exchange, la routing key, el `messageId` y el payload (hasta 2000 caracteres), y después confirma con ACK:

```
ERROR DeadLetterListener : DLQ [<dlq>] mensaje muerto: origen=<cola> motivo=rejected veces=1 exchange=<exchange> routingKeys=[<key>] messageId=<id> payload={...}
```

Un mensaje sin `x-death` es uno que el productor reenvió a la DLQ porque no tenía cola de destino (NO_ROUTE).

## Variables de entorno

| Variable | Por defecto |
|---|---|
| `RABBITMQ_HOST` | `localhost` |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USER` | `pedidos360` |
| `RABBITMQ_PASSWORD` | `pedidos360` |

## Ejecutar

```bash
./mvnw test                 # 15 pruebas: ACK/NACK, reintentos, DLQ, idempotencia y topología
./mvnw spring-boot:run      # requiere RabbitMQ (infra/local: docker compose up -d rabbit1 rabbit2)
```

## CI/CD

`.github/workflows/ci-cd.yml` corre las pruebas en cada push. En `main`, además, despliega en la EC2: hace `git pull` y `docker compose up -d --build notify`.

## Autores

Germán Marabolí, Sebastián Velez & Camila Vera

Proyecto Pedidos360 · DSY1107 Desarrollo Cloud Native I · Duoc UC
