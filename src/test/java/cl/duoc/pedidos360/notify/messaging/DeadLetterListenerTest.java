package cl.duoc.pedidos360.notify.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.Channel;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;

class DeadLetterListenerTest {

    private static Message deadLetter(Map<String, Object> headers) {
        Message message = MessageBuilder.withBody("{\"eventId\":\"evt-1\"}".getBytes(StandardCharsets.UTF_8))
                .setMessageId("evt-1")
                .build();
        headers.forEach(message.getMessageProperties()::setHeader);
        message.getMessageProperties().setConsumerQueue("q.cmd.email.dlq");
        message.getMessageProperties().setDeliveryTag(7L);
        return message;
    }

    @Test
    void registraOrigenMotivoYPayloadDesdeXDeath() {
        Message message = deadLetter(Map.of("x-death", List.of(Map.of("queue", "q.cmd.email", "reason", "rejected",
                "count", 1L, "exchange", "cmd.direct", "routing-keys", List.of("email.send")))));

        String line = DeadLetterListener.describe(message);

        assertThat(line).contains("DLQ [q.cmd.email.dlq]", "origen=q.cmd.email", "motivo=rejected", "veces=1",
                "exchange=cmd.direct", "routingKeys=[email.send]", "messageId=evt-1", "payload={\"eventId\":\"evt-1\"}");
    }

    @Test
    void mensajeSinXDeathSeExplicaComoNoRoute() {
        assertThat(DeadLetterListener.describe(deadLetter(Map.of()))).contains("NO_ROUTE");
    }

    @Test
    void siempreConfirmaConAck() throws Exception {
        Channel channel = mock(Channel.class);

        new DeadLetterListener().onDeadLetter(deadLetter(Map.of()), channel);

        verify(channel).basicAck(7L, false);
    }
}
