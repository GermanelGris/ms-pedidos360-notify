package cl.duoc.pedidos360.notify.config;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exchanges compartidos por los tres flujos (email, cocina, boleta).
 *
 * <pre>
 * cmd.direct    -> cola principal de cada flujo          (lo publica ms-pedidos360-orders)
 * cmd.retry     -> cola .retry con TTL, que al expirar vuelve a cmd.direct
 * cmd.dead.dlx  -> DLQ de cada flujo                     (destino de los NACK)
 * </pre>
 */
@Configuration
public class RabbitExchangesConfig {

    private final RabbitMessagingProperties.Exchanges exchanges;

    public RabbitExchangesConfig(RabbitMessagingProperties properties) {
        this.exchanges = properties.exchanges();
    }

    @Bean
    DirectExchange commandsExchange() {
        return ExchangeBuilder.directExchange(exchanges.commands()).durable(true).build();
    }

    @Bean
    DirectExchange retryExchange() {
        return ExchangeBuilder.directExchange(exchanges.retry()).durable(true).build();
    }

    @Bean
    DirectExchange deadLetterExchange() {
        return ExchangeBuilder.directExchange(exchanges.deadLetter()).durable(true).build();
    }
}
