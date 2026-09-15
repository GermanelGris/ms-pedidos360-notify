package cl.duoc.pedidos360.notify.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.nio.charset.StandardCharsets;

import com.rabbitmq.client.Channel;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import tools.jackson.databind.json.JsonMapper;

import cl.duoc.pedidos360.notify.config.RabbitTopology;
import cl.duoc.pedidos360.notify.service.IdempotencyRegistry;
import cl.duoc.pedidos360.notify.service.NotificationService;

class CommandListenerTest {

    private static final String EMAIL_COMMAND = "{\"type\":\"EmailNotification\",\"eventId\":\"evt-1\","
            + "\"timestamp\":\"2026-09-14T12:00:00Z\",\"traceId\":\"trace-1\",\"correlationId\":\"order-1\","
            + "\"payload\":{\"orderId\":1,\"status\":\"ACEPTADO\",\"to\":\"cliente1@pedidos360.onmicrosoft.com\"}}";

    private final Channel channel = mock(Channel.class);
    private final NotificationService notifications = mock(NotificationService.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final CommandListener listener = new CommandListener(JsonMapper.builder().build(), notifications,
            new IdempotencyRegistry(100), meters);

    private static Message message(String body, long deliveryTag) {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        return new Message(body.getBytes(StandardCharsets.UTF_8), properties);
    }

    @Test
    void comandoValidoSeProcesaYConfirmaConAck() throws Exception {
        listener.onEmail(message(EMAIL_COMMAND, 1L), channel);

        verify(notifications).sendEmail(any(CommandEnvelope.class));
        verify(channel).basicAck(1L, false);
        verify(channel, never()).basicNack(1L, false, false);
    }

    @Test
    void comandoDuplicadoNoSeReprocesa() throws Exception {
        listener.onEmail(message(EMAIL_COMMAND, 1L), channel);
        listener.onEmail(message(EMAIL_COMMAND, 2L), channel);

        verify(notifications, times(1)).sendEmail(any(CommandEnvelope.class));
        verify(channel).basicAck(2L, false);
        assertThat(meters.get(CommandListener.METRIC).tag("result", "duplicate").counter().count()).isEqualTo(1.0);
    }

    @Test
    void mensajeInvalidoVaALaDlqConNack() throws Exception {
        listener.onKitchenTicket(message("esto no es json", 3L), channel);

        verify(channel).basicNack(3L, false, false);
        verifyNoInteractions(notifications);
        assertThat(meters.get(CommandListener.METRIC).tag("queue", RabbitTopology.KITCHEN_QUEUE)
                .tag("result", "dlq").counter().count()).isEqualTo(1.0);
    }

    @Test
    void errorAlProcesarVaALaDlqYPermiteReintentoManual() throws Exception {
        doThrow(new IllegalArgumentException("Falta el campo obligatorio 'status'"))
                .when(notifications).sendEmail(any(CommandEnvelope.class));

        listener.onEmail(message(EMAIL_COMMAND, 4L), channel);
        verify(channel).basicNack(4L, false, false);

        // Al no marcarse como procesado, si se reenvía desde la DLQ vuelve a intentarse
        listener.onEmail(message(EMAIL_COMMAND, 5L), channel);
        verify(notifications, times(2)).sendEmail(any(CommandEnvelope.class));
    }
}
