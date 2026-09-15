package cl.duoc.pedidos360.notify.service;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

/**
 * Registro de eventId ya procesados para evitar efectos secundarios duplicados.
 * Guarda los últimos N en memoria; en producción se usaría Redis o una tabla compartida.
 */
@Component
public class IdempotencyRegistry {

    private final Map<String, Boolean> processed;

    public IdempotencyRegistry() {
        this(10_000);
    }

    public IdempotencyRegistry(int capacity) {
        this.processed = new LinkedHashMap<>(16, 0.75f, false) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > capacity;
            }
        };
    }

    public synchronized boolean contains(String eventId) {
        return processed.containsKey(eventId);
    }

    public synchronized void add(String eventId) {
        processed.put(eventId, Boolean.TRUE);
    }
}
