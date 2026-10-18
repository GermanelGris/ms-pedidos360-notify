package cl.duoc.pedidos360.notify.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import tools.jackson.databind.json.JsonMapper;

import cl.duoc.pedidos360.notify.config.RabbitMessagingProperties;
import cl.duoc.pedidos360.notify.config.RabbitMessagingProperties.Exchanges;
import cl.duoc.pedidos360.notify.config.RabbitMessagingProperties.Retry;
import cl.duoc.pedidos360.notify.config.RabbitMessagingProperties.Route;

class CommandAcknowledgerTest {

    private static final String COMMAND = "{\"type\":\"EmailNotification\",\"eventId\":\"evt-1\","
            + "\"timestamp\":\"2026-10-17T12:00:00Z\",\"traceId\":\"trace-1\",\"correlationId\":\"order-1\","
            + "\"payload\":{\"orderId\":1,\"status\":\"ACEPTADO\"}}";

    private static final Route EMAIL = new Route("q.email", "q.email.retry", "q.email.dlq", "email.send");
    private static final RabbitMessagingProperties PROPERTIES = new RabbitMessagingProperties(
            new Exchanges("cmd", "retry", "dlx"), new Retry(3, 1000), EMAIL, EMAIL, EMAIL);

    private final Channel channel = mock(Channel.class);
    private final RabbitTemplate rabbit = mock(RabbitTemplate.class);
    private final CommandHandler handler = mock(CommandHandler.class);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final CommandAcknowledger acknowledger = new CommandAcknowledger(JsonMapper.builder().build(),
            new IdempotencyRegistry(100), rabbit, meters, PROPERTIES);

    private static Message message(String body, long deliveryTag, Integer retryCount) {
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(deliveryTag);
        if (retryCount != null) {
            properties.setHeader(CommandAcknowledger.RETRY_HEADER, retryCount);
        }
        return new Message(body.getBytes(StandardCharsets.UTF_8), properties);
    }

    private double counter(String result) {
        return meters.get(CommandAcknowledger.METRIC).tag("result", result).counter().count();
    }

    @Test
    void comandoValidoSeProcesaYConfirmaConAck() throws Exception {
        acknowledger.process(EMAIL, message(COMMAND, 1L, null), channel, handler);

        verify(handler).handle(any(CommandEnvelope.class));
        verify(channel).basicAck(1L, false);
        verify(channel, never()).basicNack(anyLong(), eq(false), eq(false));
    }

    @Test
    void comandoDuplicadoSeConfirmaSinReprocesar() throws Exception {
        acknowledger.process(EMAIL, message(COMMAND, 1L, null), channel, handler);
        acknowledger.process(EMAIL, message(COMMAND, 2L, null), channel, handler);

        verify(handler, times(1)).handle(any(CommandEnvelope.class));
        verify(channel).basicAck(2L, false);
        assertThat(counter("duplicate")).isEqualTo(1.0);
    }

    @Test
    void jsonInvalidoVaDirectoALaDlqSinReintentar() throws Exception {
        acknowledger.process(EMAIL, message("esto no es json", 3L, null), channel, handler);

        verify(channel).basicNack(3L, false, false);
        verifyNoInteractions(handler, rabbit);
        assertThat(counter("dlq")).isEqualTo(1.0);
    }

    @Test
    void comandoSinEventIdVaALaDlq() throws Exception {
        acknowledger.process(EMAIL, message("{\"type\":\"EmailNotification\",\"payload\":{}}", 4L, null),
                channel, handler);

        verify(channel).basicNack(4L, false, false);
        verifyNoInteractions(handler);
    }

    @Test
    void errorNoRecuperableDelServicioVaALaDlq() throws Exception {
        doThrow(new NonRecoverableMessageException("Falta el campo obligatorio 'status'"))
                .when(handler).handle(any(CommandEnvelope.class));

        acknowledger.process(EMAIL, message(COMMAND, 5L, null), channel, handler);

        verify(channel).basicNack(5L, false, false);
        verifyNoInteractions(rabbit);
    }

    @Test
    void errorRecuperableSeReintentaPorLaColaRetryConAck() throws Exception {
        doThrow(new RecoverableMessageException("SMTP no disponible")).when(handler).handle(any(CommandEnvelope.class));

        acknowledger.process(EMAIL, message(COMMAND, 6L, null), channel, handler);

        ArgumentCaptor<Message> retry = ArgumentCaptor.forClass(Message.class);
        verify(rabbit).send(eq("retry"), eq("email.send"), retry.capture());
        assertThat(CommandAcknowledger.retryCount(retry.getValue())).isEqualTo(1);
        verify(channel).basicAck(6L, false);
        verify(channel, never()).basicNack(anyLong(), eq(false), eq(false));
        assertThat(counter("retry")).isEqualTo(1.0);
    }

    @Test
    void alAgotarLosReintentosVaALaDlq() throws Exception {
        doThrow(new RecoverableMessageException("SMTP no disponible")).when(handler).handle(any(CommandEnvelope.class));

        // Ya se intentó 2 veces: este es el tercer y último intento (max-attempts = 3)
        acknowledger.process(EMAIL, message(COMMAND, 7L, 2), channel, handler);

        verify(channel).basicNack(7L, false, false);
        verify(rabbit, never()).send(anyString(), anyString(), any(Message.class));
        assertThat(counter("dlq")).isEqualTo(1.0);
    }

    @Test
    void siNoSePuedeProgramarElReintentoVaALaDlq() throws Exception {
        doThrow(new RecoverableMessageException("SMTP no disponible")).when(handler).handle(any(CommandEnvelope.class));
        doThrow(new AmqpConnectException(new RuntimeException("broker caído")))
                .when(rabbit).send(anyString(), anyString(), any(Message.class));

        acknowledger.process(EMAIL, message(COMMAND, 8L, null), channel, handler);

        verify(channel).basicNack(8L, false, false);
        verify(channel, never()).basicAck(8L, false);
    }

    @Test
    void unMensajeFallidoNoQuedaMarcadoComoProcesado() throws Exception {
        doThrow(new RecoverableMessageException("SMTP no disponible"))
                .doNothing()
                .when(handler).handle(any(CommandEnvelope.class));

        acknowledger.process(EMAIL, message(COMMAND, 9L, null), channel, handler);
        acknowledger.process(EMAIL, message(COMMAND, 10L, 1), channel, handler);

        verify(handler, times(2)).handle(any(CommandEnvelope.class));
        verify(channel).basicAck(10L, false);
        assertThat(counter("ok")).isEqualTo(1.0);
    }
}
