package cl.duoc.pedidos360.notify.messaging;

import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Envelope común de todos los comandos: type, eventId, timestamp, traceId, correlationId + payload. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CommandEnvelope(String type, String eventId, Instant timestamp, String traceId, String correlationId,
                              Map<String, Object> payload) {

    /** Campo obligatorio del payload; si falta, el mensaje nunca podrá procesarse (error no recuperable). */
    public Object require(String field) {
        Object value = optional(field);
        if (value == null) {
            throw new NonRecoverableMessageException("Falta el campo obligatorio '" + field + "' en " + type);
        }
        return value;
    }

    public Object optional(String field) {
        return payload == null ? null : payload.get(field);
    }
}
