package cl.duoc.pedidos360.notify.messaging;

/** Acción de negocio que procesa un comando ya validado. No conoce RabbitMQ. */
@FunctionalInterface
public interface CommandHandler {

    void handle(CommandEnvelope command);
}
