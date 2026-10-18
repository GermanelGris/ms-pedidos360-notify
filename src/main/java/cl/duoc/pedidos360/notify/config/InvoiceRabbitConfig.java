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
 * Caso de uso: generar la boleta cuando el pedido se entrega.
 *
 * <pre>
 * cmd.direct   --invoice.gen-->  q.cmd.invoice        (si falla: NACK -> cmd.dead.dlx)
 * cmd.retry    --invoice.gen-->  q.cmd.invoice.retry  (TTL; al expirar vuelve a cmd.direct)
 * cmd.dead.dlx --invoice.gen-->  q.cmd.invoice.dlq
 * </pre>
 */
@Configuration
public class InvoiceRabbitConfig {

    private final RabbitMessagingProperties.Route route;
    private final RabbitMessagingProperties properties;

    public InvoiceRabbitConfig(RabbitMessagingProperties properties) {
        this.properties = properties;
        this.route = properties.invoice();
    }

    @Bean
    Queue invoiceQueue() {
        return QueueBuilder.durable(route.queue())
                .deadLetterExchange(properties.exchanges().deadLetter())
                .deadLetterRoutingKey(route.routingKey())
                .build();
    }

    @Bean
    Queue invoiceRetryQueue() {
        return QueueBuilder.durable(route.retryQueue())
                .ttl(properties.retry().delayMs())
                .deadLetterExchange(properties.exchanges().commands())
                .deadLetterRoutingKey(route.routingKey())
                .build();
    }

    @Bean
    Queue invoiceDeadLetterQueue() {
        return QueueBuilder.durable(route.dlq()).build();
    }

    @Bean
    Binding invoiceBinding(@Qualifier("commandsExchange") DirectExchange commandsExchange) {
        return BindingBuilder.bind(invoiceQueue()).to(commandsExchange).with(route.routingKey());
    }

    @Bean
    Binding invoiceRetryBinding(@Qualifier("retryExchange") DirectExchange retryExchange) {
        return BindingBuilder.bind(invoiceRetryQueue()).to(retryExchange).with(route.routingKey());
    }

    @Bean
    Binding invoiceDeadLetterBinding(@Qualifier("deadLetterExchange") DirectExchange deadLetterExchange) {
        return BindingBuilder.bind(invoiceDeadLetterQueue()).to(deadLetterExchange).with(route.routingKey());
    }
}
