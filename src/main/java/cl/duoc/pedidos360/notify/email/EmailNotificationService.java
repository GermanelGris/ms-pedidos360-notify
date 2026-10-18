package cl.duoc.pedidos360.notify.email;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cl.duoc.pedidos360.notify.messaging.CommandEnvelope;
import cl.duoc.pedidos360.notify.messaging.SimulatedFailure;

/** Avisa al cliente el nuevo estado de su pedido. El envío real (SMTP / Web Push) se simula con logs. */
@Service
public class EmailNotificationService {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationService.class);

    public void send(CommandEnvelope command) {
        Object orderId = command.require("orderId");
        Object status = command.require("status");
        SimulatedFailure.check(command, "Servidor SMTP");
        Object to = command.optional("to");
        if (to == null || to.toString().isBlank()) {
            log.info("PUSH  -> {}: tu pedido #{} ahora está {} [trace={}]",
                    command.optional("customerName"), orderId, status, command.traceId());
        } else {
            log.info("EMAIL -> {}: tu pedido #{} ahora está {} [trace={}]", to, orderId, status, command.traceId());
        }
    }
}
