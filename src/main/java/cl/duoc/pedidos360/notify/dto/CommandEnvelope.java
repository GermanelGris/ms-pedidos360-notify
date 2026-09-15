package cl.duoc.pedidos360.notify.dto;

import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Envelope común de los comandos de notificación: type, eventId, timestamp, traceId, correlationId + payload.
 * Contrato definido; el consumidor aún no está implementado (fuera del alcance de la EP1).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CommandEnvelope(String type, String eventId, Instant timestamp, String traceId, String correlationId,
                              Map<String, Object> payload) {
}
