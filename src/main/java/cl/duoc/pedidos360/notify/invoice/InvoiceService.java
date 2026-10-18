package cl.duoc.pedidos360.notify.invoice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.notify.messaging.CommandEnvelope;
import cl.duoc.pedidos360.notify.messaging.SimulatedFailure;

/** Genera la boleta de un pedido entregado. La generación del documento (PDF / SII) se simula con logs. */
@Service
public class InvoiceService {

    private static final Logger log = LoggerFactory.getLogger(InvoiceService.class);

    public void generate(CommandEnvelope command) {
        Object orderId = command.require("orderId");
        Object total = command.require("total");
        SimulatedFailure.check(command, "Servicio de boletas");
        log.info("BOLETA -> generada para el pedido #{} por ${} a nombre de {} [trace={}]",
                orderId, total, command.optional("customerName"), command.traceId());
    }
}
