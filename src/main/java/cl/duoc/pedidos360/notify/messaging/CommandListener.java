package cl.duoc.pedidos360.notify.messaging;

import java.io.IOException;
import java.util.function.Consumer;

import com.rabbitmq.client.Channel;

import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import tools.jackson.databind.ObjectMapper;

import cl.duoc.pedidos360.notify.config.RabbitTopology;
import cl.duoc.pedidos360.notify.service.IdempotencyRegistry;
import cl.duoc.pedidos360.notify.service.NotificationService;

/**
 * Consumidor de comandos con ACK/NACK explícitos:
 * <ul>
 *   <li>Procesado correctamente: basicAck.</li>
 *   <li>Duplicado (mismo eventId): basicAck sin volver a ejecutar el efecto (idempotencia).</li>
 *   <li>Inválido o con error: basicNack sin reencolar, por lo que RabbitMQ lo mueve a la DLQ de la cola.</li>
 * </ul>
 */
@Component
public class CommandListener {

    static final String METRIC = "pedidos360.notify.commands";

    private static final Logger log = LoggerFactory.getLogger(CommandListener.class);

    private final ObjectMapper mapper;
    private final NotificationService notifications;
    private final IdempotencyRegistry processed;
    private final MeterRegistry meters;

    public CommandListener(ObjectMapper mapper, NotificationService notifications, IdempotencyRegistry processed,
                           MeterRegistry meters) {
        this.mapper = mapper;
        this.notifications = notifications;
        this.processed = processed;
        this.meters = meters;
    }

    @RabbitListener(queues = RabbitTopology.EMAIL_QUEUE)
    public void onEmail(Message message, Channel channel) throws IOException {
        handle(RabbitTopology.EMAIL_QUEUE, message, channel, notifications::sendEmail);
    }

    @RabbitListener(queues = RabbitTopology.KITCHEN_QUEUE)
    public void onKitchenTicket(Message message, Channel channel) throws IOException {
        handle(RabbitTopology.KITCHEN_QUEUE, message, channel, notifications::printKitchenTicket);
    }

    @RabbitListener(queues = RabbitTopology.INVOICE_QUEUE)
    public void onInvoice(Message message, Channel channel) throws IOException {
        handle(RabbitTopology.INVOICE_QUEUE, message, channel, notifications::generateInvoice);
    }

    void handle(String queue, Message message, Channel channel, Consumer<CommandEnvelope> action) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            CommandEnvelope command = mapper.readValue(message.getBody(), CommandEnvelope.class);
            if (command.eventId() == null || command.eventId().isBlank()) {
                throw new IllegalArgumentException("El comando no tiene eventId");
            }
            if (processed.contains(command.eventId())) {
                log.info("[{}] Comando {} duplicado: se confirma sin reprocesar", queue, command.eventId());
                channel.basicAck(deliveryTag, false);
                count(queue, "duplicate");
                return;
            }
            action.accept(command);
            processed.add(command.eventId());
            channel.basicAck(deliveryTag, false);
            count(queue, "ok");
        } catch (RuntimeException ex) {
            log.error("[{}] Mensaje rechazado, se envía a {}.dlq: {}", queue, queue, ex.getMessage());
            channel.basicNack(deliveryTag, false, false);
            count(queue, "dlq");
        }
    }

    private void count(String queue, String result) {
        meters.counter(METRIC, "queue", queue, "result", result).increment();
    }
}
