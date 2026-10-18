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
 * Caso de uso: imprimir el ticket de cocina cuando el pedido se acepta.
 *
 * <pre>
 * cmd.direct   --kitchen.ticket-->  q.cmd.kitchen        (si falla: NACK -> cmd.dead.dlx)
 * cmd.retry    --kitchen.ticket-->  q.cmd.kitchen.retry  (TTL; al expirar vuelve a cmd.direct)
 * cmd.dead.dlx --kitchen.ticket-->  q.cmd.kitchen.dlq
 * </pre>
 */
@Configuration
public class KitchenRabbitConfig {

    private final RabbitMessagingProperties.Route route;
    private final RabbitMessagingProperties properties;

    public KitchenRabbitConfig(RabbitMessagingProperties properties) {
        this.properties = properties;
        this.route = properties.kitchen();
    }

    @Bean
    Queue kitchenQueue() {
        return QueueBuilder.durable(route.queue())
                .deadLetterExchange(properties.exchanges().deadLetter())
                .deadLetterRoutingKey(route.routingKey())
                .build();
    }

    @Bean
    Queue kitchenRetryQueue() {
        return QueueBuilder.durable(route.retryQueue())
                .ttl(properties.retry().delayMs())
                .deadLetterExchange(properties.exchanges().commands())
                .deadLetterRoutingKey(route.routingKey())
                .build();
    }

    @Bean
    Queue kitchenDeadLetterQueue() {
        return QueueBuilder.durable(route.dlq()).build();
    }

    @Bean
    Binding kitchenBinding(@Qualifier("commandsExchange") DirectExchange commandsExchange) {
        return BindingBuilder.bind(kitchenQueue()).to(commandsExchange).with(route.routingKey());
    }

    @Bean
    Binding kitchenRetryBinding(@Qualifier("retryExchange") DirectExchange retryExchange) {
        return BindingBuilder.bind(kitchenRetryQueue()).to(retryExchange).with(route.routingKey());
    }

    @Bean
    Binding kitchenDeadLetterBinding(@Qualifier("deadLetterExchange") DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(kitchenDeadLetterQueue()).to(deadLetterExchange).with(route.routingKey());
    }
}
