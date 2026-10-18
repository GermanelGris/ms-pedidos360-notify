package cl.duoc.pedidos360.notify.messaging;

import java.io.IOException;

import com.rabbitmq.client.Channel;

import io.micrometer.core.instrument.MeterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import cl.duoc.pedidos360.notify.config.RabbitMessagingProperties;
import cl.duoc.pedidos360.notify.config.RabbitMessagingProperties.Route;

/**
 * Política única de confirmación para todos los consumidores (acknowledge-mode: manual).
 *
 * <ul>
 *   <li><b>Procesado:</b> basicAck.</li>
 *   <li><b>Duplicado</b> (eventId ya visto): basicAck sin repetir el efecto (idempotencia).</li>
 *   <li><b>Error no recuperable</b> ({@link NonRecoverableMessageException}: JSON inválido, sin eventId,
 *       faltan campos): basicNack(requeue=false), RabbitMQ lo mueve a la DLQ al instante.</li>
 *   <li><b>Error recuperable</b> ({@link RecoverableMessageException} o cualquier otra excepción) con intentos
 *       disponibles: se republica en la cola .retry con el header {@value #RETRY_HEADER} y se hace basicAck del
 *       original; al vencer el TTL vuelve a la cola principal.</li>
 *   <li><b>Error recuperable sin intentos disponibles:</b> basicNack(requeue=false), DLQ.</li>
 * </ul>
 *
 * Nunca se usa requeue=true: un mensaje que siempre falla quedaría en un bucle infinito.
 * Todo envío a la DLQ queda registrado en el log con nivel ERROR y en la métrica {@value #METRIC}.
 */
@Component
public class CommandAcknowledger {

    public static final String RETRY_HEADER = "x-retry-count";
    static final String METRIC = "pedidos360.notify.commands";

    private static final Logger log = LoggerFactory.getLogger(CommandAcknowledger.class);

    private final ObjectMapper mapper;
    private final IdempotencyRegistry processed;
    private final RabbitTemplate rabbit;
    private final MeterRegistry meters;
    private final RabbitMessagingProperties properties;

    public CommandAcknowledger(ObjectMapper mapper, IdempotencyRegistry processed, RabbitTemplate rabbit,
                               MeterRegistry meters, RabbitMessagingProperties properties) {
        this.mapper = mapper;
        this.processed = processed;
        this.rabbit = rabbit;
        this.meters = meters;
        this.properties = properties;
    }

    public void process(Route route, Message message, Channel channel, CommandHandler handler) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            CommandEnvelope command = parse(message);
            if (processed.contains(command.eventId())) {
                log.info("[{}] Comando {} duplicado: ACK sin reprocesar", route.queue(), command.eventId());
                channel.basicAck(deliveryTag, false);
                count(route, "duplicate");
                return;
            }
            handler.handle(command);
            processed.add(command.eventId());
            channel.basicAck(deliveryTag, false);
            count(route, "ok");
        } catch (NonRecoverableMessageException ex) {
            log.error("[{}] Mensaje no recuperable, NACK -> {}: {}", route.queue(), route.dlq(), ex.getMessage());
            channel.basicNack(deliveryTag, false, false);
            count(route, "dlq");
        } catch (RuntimeException ex) {
            retryOrDeadLetter(route, message, channel, deliveryTag, ex);
        }
    }

    private void retryOrDeadLetter(Route route, Message message, Channel channel, long deliveryTag,
                                   RuntimeException error) throws IOException {
        int attempt = retryCount(message) + 1;
        int maxAttempts = properties.retry().maxAttempts();
        if (attempt >= maxAttempts) {
            log.error("[{}] Intento {}/{} fallido, sin reintentos disponibles, NACK -> {}: {}",
                    route.queue(), attempt, maxAttempts, route.dlq(), error.getMessage());
            channel.basicNack(deliveryTag, false, false);
            count(route, "dlq");
            return;
        }
        try {
            Message retry = MessageBuilder.fromMessage(message).setHeader(RETRY_HEADER, attempt).build();
            rabbit.send(properties.exchanges().retry(), route.routingKey(), retry);
            channel.basicAck(deliveryTag, false);
            log.warn("[{}] Intento {}/{} fallido, se reintenta en {} ms vía {}: {}", route.queue(), attempt,
                    maxAttempts, properties.retry().delayMs(), route.retryQueue(), error.getMessage());
            count(route, "retry");
        } catch (AmqpException publishError) {
            log.error("[{}] No se pudo programar el reintento, NACK -> {}: {}", route.queue(), route.dlq(),
                    publishError.getMessage());
            channel.basicNack(deliveryTag, false, false);
            count(route, "dlq");
        }
    }

    private CommandEnvelope parse(Message message) {
        CommandEnvelope command;
        try {
            command = mapper.readValue(message.getBody(), CommandEnvelope.class);
        } catch (JacksonException ex) {
            throw new NonRecoverableMessageException("El cuerpo no es un comando JSON válido", ex);
        }
        if (command.eventId() == null || command.eventId().isBlank()) {
            throw new NonRecoverableMessageException("El comando no tiene eventId");
        }
        return command;
    }

    static int retryCount(Message message) {
        Object value = message.getMessageProperties().getHeader(RETRY_HEADER);
        return value instanceof Number number ? number.intValue() : 0;
    }

    private void count(Route route, String result) {
        meters.counter(METRIC, "queue", route.queue(), "result", result).increment();
    }
}
