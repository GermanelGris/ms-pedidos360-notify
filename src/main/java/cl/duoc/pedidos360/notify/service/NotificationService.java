package cl.duoc.pedidos360.notify.service;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.notify.messaging.CommandEnvelope;

/**
 * Ejecuta los comandos. El envío real (SMTP, Web Push, impresora térmica, PDF) se simula con logs:
 * el objetivo es demostrar el procesamiento asíncrono, la idempotencia y las DLQ.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    public void sendEmail(CommandEnvelope command) {
        Map<String, Object> payload = payload(command);
        Object orderId = require(payload, "orderId");
        Object status = require(payload, "status");
        Object to = payload.get("to");
        if (to == null || to.toString().isBlank()) {
            log.info("PUSH  -> {}: tu pedido #{} ahora está {} [trace={}]",
                    payload.get("customerName"), orderId, status, command.traceId());
        } else {
            log.info("EMAIL -> {}: tu pedido #{} ahora está {} [trace={}]", to, orderId, status, command.traceId());
        }
    }

    public void printKitchenTicket(CommandEnvelope command) {
        Map<String, Object> payload = payload(command);
        Object orderId = require(payload, "orderId");
        Object items = payload.get("items");
        int lines = items instanceof Collection<?> collection ? collection.size() : 0;
        log.info("COCINA -> ticket del pedido #{} ({} producto(s)) para {} [trace={}]",
                orderId, lines, payload.get("customerName"), command.traceId());
    }

    public void generateInvoice(CommandEnvelope command) {
        Map<String, Object> payload = payload(command);
        Object orderId = require(payload, "orderId");
        Object total = require(payload, "total");
        log.info("BOLETA -> generada para el pedido #{} por ${} a nombre de {} [trace={}]",
                orderId, total, payload.get("customerName"), command.traceId());
    }

    private static Map<String, Object> payload(CommandEnvelope command) {
        if (command.payload() == null) {
            throw new IllegalArgumentException("El comando " + command.type() + " no tiene payload");
        }
        return command.payload();
    }

    private static Object require(Map<String, Object> payload, String field) {
        Object value = payload.get(field);
        if (value == null) {
            throw new IllegalArgumentException("Falta el campo obligatorio '" + field + "'");
        }
        return value;
    }
}
