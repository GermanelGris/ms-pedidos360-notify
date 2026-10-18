package cl.duoc.pedidos360.notify.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Nombres de exchanges, colas y routing keys, leídos de {@code pedidos360.rabbitmq} en application.yml.
 * Ninguna otra clase escribe nombres de RabbitMQ en el código.
 */
@Validated
@ConfigurationProperties("pedidos360.rabbitmq")
public record RabbitMessagingProperties(
        @Valid @NotNull Exchanges exchanges,
        @Valid @NotNull Retry retry,
        @Valid @NotNull Route email,
        @Valid @NotNull Route kitchen,
        @Valid @NotNull Route invoice) {

    public record Exchanges(@NotBlank String commands, @NotBlank String retry, @NotBlank String deadLetter) {
    }

    public record Retry(@Min(1) int maxAttempts, @Min(100) int delayMs) {
    }

    /** Un flujo de trabajo: cola principal, cola de espera para reintentos, DLQ y routing key. */
    public record Route(@NotBlank String queue, @NotBlank String retryQueue, @NotBlank String dlq,
                        @NotBlank String routingKey) {
    }
}
