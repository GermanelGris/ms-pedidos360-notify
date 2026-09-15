# ms-pedidos360-notify

> 🚧 **En construcción.** Fuera del alcance de la EP1: por ahora solo contiene los DTO (contratos) del servicio.

Microservicio de **notificaciones** de Pedidos360. Enviará email o push al cliente con el estado de su pedido, de forma asíncrona. No expondrá API pública.

## Contenido actual

| Tipo | Clase | Descripción |
|---|---|---|
| DTO | `dto/CommandEnvelope` | Envelope común de los comandos: `type`, `eventId`, `timestamp`, `traceId`, `correlationId` y `payload` |

Puerto reservado: `8083`.

## Autores

Germán Maraboli & Camila Vera

Proyecto Pedidos360 · DSY1107 Desarrollo Cloud Native I · Duoc UC
