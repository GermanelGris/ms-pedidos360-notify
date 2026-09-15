package cl.duoc.pedidos360.notify.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Topología del caso: 6 colas, 3 flujos de trabajo (email, cocina, boleta), cada uno con su DLQ.
 *
 * <pre>
 * cmd.direct   q.cmd.email   email.send       cmd.topic  q.cmd.email   email.*
 * cmd.direct   q.cmd.kitchen kitchen.ticket   cmd.topic  q.cmd.kitchen kitchen.#
 * cmd.direct   q.cmd.invoice invoice.gen      cmd.topic  q.cmd.invoice invoice.*
 * cmd.dead.dlx q.cmd.email.dlq / q.cmd.kitchen.dlq / q.cmd.invoice.dlq
 * </pre>
 */
@Configuration
public class RabbitTopology {

    public static final String DIRECT_EXCHANGE = "cmd.direct";
    public static final String TOPIC_EXCHANGE = "cmd.topic";
    public static final String DEAD_LETTER_EXCHANGE = "cmd.dead.dlx";

    public static final String EMAIL_QUEUE = "q.cmd.email";
    public static final String KITCHEN_QUEUE = "q.cmd.kitchen";
    public static final String INVOICE_QUEUE = "q.cmd.invoice";

    @Bean
    Declarables commandTopology() {
        DirectExchange direct = ExchangeBuilder.directExchange(DIRECT_EXCHANGE).durable(true).build();
        TopicExchange topic = ExchangeBuilder.topicExchange(TOPIC_EXCHANGE).durable(true).build();
        DirectExchange deadLetter = ExchangeBuilder.directExchange(DEAD_LETTER_EXCHANGE).durable(true).build();

        List<Declarable> declarables = new ArrayList<>(List.of(direct, topic, deadLetter));
        addWorkflow(declarables, direct, topic, deadLetter, EMAIL_QUEUE, "email.send", "email.*");
        addWorkflow(declarables, direct, topic, deadLetter, KITCHEN_QUEUE, "kitchen.ticket", "kitchen.#");
        addWorkflow(declarables, direct, topic, deadLetter, INVOICE_QUEUE, "invoice.gen", "invoice.*");
        return new Declarables(declarables);
    }

    private static void addWorkflow(List<Declarable> declarables, DirectExchange direct, TopicExchange topic,
                                    DirectExchange deadLetter, String queueName, String routingKey, String pattern) {
        Queue queue = QueueBuilder.durable(queueName)
                .deadLetterExchange(DEAD_LETTER_EXCHANGE)
                .deadLetterRoutingKey(routingKey)
                .build();
        Queue dlq = QueueBuilder.durable(queueName + ".dlq").build();

        declarables.add(queue);
        declarables.add(dlq);
        declarables.add(BindingBuilder.bind(queue).to(direct).with(routingKey));
        declarables.add(BindingBuilder.bind(queue).to(topic).with(pattern));
        declarables.add(BindingBuilder.bind(dlq).to(deadLetter).with(routingKey));
    }
}
