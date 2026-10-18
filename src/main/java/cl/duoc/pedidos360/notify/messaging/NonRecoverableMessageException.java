package cl.duoc.pedidos360.notify.messaging;

/**
 * El mensaje es inválido (JSON mal formado, sin eventId, faltan campos) y reintentarlo no cambiaría el resultado:
 * se rechaza con NACK sin reencolar y RabbitMQ lo mueve a la DLQ de inmediato.
 */
public class NonRecoverableMessageException extends RuntimeException {

    public NonRecoverableMessageException(String message) {
        super(message);
    }

    public NonRecoverableMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
