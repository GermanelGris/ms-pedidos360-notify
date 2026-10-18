package cl.duoc.pedidos360.notify.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Caso de uso: notificar al cliente (email o push) cada cambio de estado del pedido.
 *
 * <pre>
 * cmd.direct   --email.send-->  q.cmd.email        (si falla: NACK -> cmd.dead.dlx)
 * cmd.retry    --email.send-->  q.cmd.email.retry  (TTL; al expirar vuelve a cmd.direct)
 * cmd.dead.dlx --email.send-->  q.cmd.email.dlq
 * </pre>
 */
@Configuration
public class EmailRabbitConfig {

    private final RabbitMessagingProperties.Route route;
    private final RabbitMessagingProperties properties;

    public EmailRabbitConfig(RabbitMessagingProperties properties) {
        this.properties = properties;
        this.route = properties.email();
    }

    @Bean
    Queue emailQueue() {
        return QueueBuilder.durable(route.queue())
                .deadLetterExchange(properties.exchanges().deadLetter())
                .deadLetterRoutingKey(route.routingKey())
                .build();
    }

    @Bean
    Queue emailRetryQueue() {
        return QueueBuilder.durable(route.retryQueue())
                .ttl(properties.retry().delayMs())
                .deadLetterExchange(properties.exchanges().commands())
                .deadLetterRoutingKey(route.routingKey())
                .build();
    }

    @Bean
    Queue emailDeadLetterQueue() {
        return QueueBuilder.durable(route.dlq()).build();
    }

    @Bean
    Binding emailBinding(@Qualifier("commandsExchange") DirectExchange commandsExchange) {
        return BindingBuilder.bind(emailQueue()).to(commandsExchange).with(route.routingKey());
    }

    @Bean
    Binding emailRetryBinding(@Qualifier("retryExchange") DirectExchange retryExchange) {
        return BindingBuilder.bind(emailRetryQueue()).to(retryExchange).with(route.routingKey());
    }

    @Bean
    Binding emailDeadLetterBinding(@Qualifier("deadLetterExchange") DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(emailDeadLetterQueue()).to(deadLetterExchange).with(route.routingKey());
    }
}
