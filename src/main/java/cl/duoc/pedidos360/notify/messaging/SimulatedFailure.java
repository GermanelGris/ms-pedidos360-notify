package cl.duoc.pedidos360.notify.messaging;

/**
 * Permite demostrar los caminos de error sin romper nada: si el payload trae
 * {@code "simulateFailure": "transient"} se lanza un error recuperable (reintentos y luego DLQ);
 * con {@code "fatal"} un error no recuperable (DLQ inmediata).
 */
public final class SimulatedFailure {

    static final String FIELD = "simulateFailure";

    private SimulatedFailure() {
    }

    public static void check(CommandEnvelope command, String dependency) {
        Object mode = command.optional(FIELD);
        if ("transient".equals(mode)) {
            throw new RecoverableMessageException(dependency + " no disponible (falla simulada)");
        }
        if ("fatal".equals(mode)) {
            throw new NonRecoverableMessageException("Datos rechazados por " + dependency + " (falla simulada)");
        }
    }
}
