package cl.duoc.pedidos360.notify;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

/** Levanta el contexto sin broker (los listeners no arrancan) y verifica la topología declarada. */
@SpringBootTest(properties = "spring.rabbitmq.listener.simple.auto-startup=false")
class MsPedidos360NotifyApplicationTests {

    @Autowired
    private ApplicationContext context;

    @Test
    void declaraColaPrincipalRetryYDlqPorCadaCasoDeUso() {
        Map<String, Queue> queues = context.getBeansOfType(Queue.class);
        assertThat(queues.values()).extracting(Queue::getName).containsExactlyInAnyOrder(
                "q.cmd.email", "q.cmd.email.retry", "q.cmd.email.dlq",
                "q.cmd.kitchen", "q.cmd.kitchen.retry", "q.cmd.kitchen.dlq",
                "q.cmd.invoice", "q.cmd.invoice.retry", "q.cmd.invoice.dlq");
        assertThat(context.getBeansOfType(Binding.class)).hasSize(9);
    }

    @Test
    void laColaPrincipalEnviaLosRechazosAlDeadLetterExchange() {
        Queue email = context.getBean("emailQueue", Queue.class);
        assertThat(email.getArguments())
                .containsEntry("x-dead-letter-exchange", "cmd.dead.dlx")
                .containsEntry("x-dead-letter-routing-key", "email.send");

        Queue retry = context.getBean("emailRetryQueue", Queue.class);
        assertThat(retry.getArguments())
                .containsEntry("x-message-ttl", 5000)
                .containsEntry("x-dead-letter-exchange", "cmd.direct");
    }
}
