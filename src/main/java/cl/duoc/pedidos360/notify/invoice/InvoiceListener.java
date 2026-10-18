package cl.duoc.pedidos360.notify.invoice;

import java.io.IOException;

import com.rabbitmq.client.Channel;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.notify.config.RabbitMessagingProperties;
import cl.duoc.pedidos360.notify.messaging.CommandAcknowledger;

/** Consumidor del dominio facturación: cola {@code pedidos360.rabbitmq.invoice.queue}. */
@Component
public class InvoiceListener {

    private final CommandAcknowledger acknowledger;
    private final InvoiceService invoices;
    private final RabbitMessagingProperties.Route route;

    public InvoiceListener(CommandAcknowledger acknowledger, InvoiceService invoices,
                           RabbitMessagingProperties properties) {
        this.acknowledger = acknowledger;
        this.invoices = invoices;
        this.route = properties.invoice();
    }

    @RabbitListener(id = "invoices", queues = "${pedidos360.rabbitmq.invoice.queue}")
    public void onMessage(Message message, Channel channel) throws IOException {
        acknowledger.process(route, message, channel, invoices::generate);
    }
}
