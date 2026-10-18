package cl.duoc.pedidos360.notify.kitchen;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.notify.messaging.CommandEnvelope;
import cl.duoc.pedidos360.notify.messaging.SimulatedFailure;

/** Imprime el ticket de cocina de un pedido aceptado. La impresora térmica se simula con logs. */
@Service
public class KitchenTicketService {

    private static final Logger log = LoggerFactory.getLogger(KitchenTicketService.class);

    public void print(CommandEnvelope command) {
        Object orderId = command.require("orderId");
        Object items = command.require("items");
        SimulatedFailure.check(command, "Impresora de cocina");
        int lines = items instanceof Collection<?> collection ? collection.size() : 0;
        log.info("COCINA -> ticket del pedido #{} ({} producto(s)) para {} [trace={}]",
                orderId, lines, command.optional("customerName"), command.traceId());
    }
}
