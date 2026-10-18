package cl.duoc.pedidos360.notify.messaging;

/**
 * Falla temporal (servidor SMTP, impresora o servicio de boletas no disponible): el mensaje es válido
 * y se reintenta después de una espera, hasta agotar {@code pedidos360.rabbitmq.retry.max-attempts}.
 */
public class RecoverableMessageException extends RuntimeException {

    public RecoverableMessageException(String message) {
        super(message);
    }
}
