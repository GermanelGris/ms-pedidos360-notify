package cl.duoc.pedidos360.notify.messaging;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.Channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Listener de las DLQ: registra cada mensaje muerto con su payload, la cola de origen y el motivo (header
 * {@code x-death} que agrega RabbitMQ al hacer dead-lettering) y luego confirma con ACK.
 *
 * <p>Motivos posibles en {@code x-death}: {@code rejected} (NACK sin requeue), {@code expired} (venció el
 * {@code message-ttl}), {@code maxlen} (la cola llegó a {@code max-length}). Un mensaje sin {@code x-death} lo
 * reenvió directamente el productor porque no tenía cola de destino (NO_ROUTE).
 */
@Component
public class DeadLetterListener {

    static final int MAX_PAYLOAD = 2000;

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    @RabbitListener(id = "dead-letters", queues = {"${pedidos360.rabbitmq.email.dlq}",
            "${pedidos360.rabbitmq.kitchen.dlq}", "${pedidos360.rabbitmq.invoice.dlq}"})
    public void onDeadLetter(Message message, Channel channel) throws IOException {
        try {
            log.error(describe(message));
        } finally {
            // Ya quedó registrado: se confirma para no reprocesarlo (la DLQ no tiene otra DLQ detrás)
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        }
    }

    static String describe(Message message) {
        MessageProperties properties = message.getMessageProperties();
        List<Map<String, ?>> deaths = properties.getXDeathHeader();
        Map<String, ?> death = deaths == null || deaths.isEmpty() ? Map.of() : deaths.get(0);
        return "DLQ [%s] mensaje muerto: origen=%s motivo=%s veces=%s exchange=%s routingKeys=%s messageId=%s payload=%s"
                .formatted(properties.getConsumerQueue(),
                        death.isEmpty() ? "-" : death.get("queue"),
                        death.isEmpty() ? "sin x-death (reenviado por el productor: NO_ROUTE)" : death.get("reason"),
                        death.isEmpty() ? "-" : death.get("count"),
                        death.isEmpty() ? properties.getReceivedExchange() : death.get("exchange"),
                        death.isEmpty() ? properties.getReceivedRoutingKey() : death.get("routing-keys"),
                        properties.getMessageId(),
                        payload(message.getBody()));
    }

    private static String payload(byte[] body) {
        String text = new String(body, StandardCharsets.UTF_8);
        return text.length() > MAX_PAYLOAD ? text.substring(0, MAX_PAYLOAD) + "…(truncado)" : text;
    }
}
