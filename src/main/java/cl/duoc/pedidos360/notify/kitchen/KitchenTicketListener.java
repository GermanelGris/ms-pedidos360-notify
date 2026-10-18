package cl.duoc.pedidos360.notify.kitchen;

import java.io.IOException;

import com.rabbitmq.client.Channel;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.notify.config.RabbitMessagingProperties;
import cl.duoc.pedidos360.notify.messaging.CommandAcknowledger;

/** Consumidor del dominio cocina: cola {@code pedidos360.rabbitmq.kitchen.queue}. */
@Component
public class KitchenTicketListener {

    private final CommandAcknowledger acknowledger;
    private final KitchenTicketService tickets;
    private final RabbitMessagingProperties.Route route;

    public KitchenTicketListener(CommandAcknowledger acknowledger, KitchenTicketService tickets,
                                 RabbitMessagingProperties properties) {
        this.acknowledger = acknowledger;
        this.tickets = tickets;
        this.route = properties.kitchen();
    }

    @RabbitListener(id = "kitchen-tickets", queues = "${pedidos360.rabbitmq.kitchen.queue}")
    public void onMessage(Message message, Channel channel) throws IOException {
        acknowledger.process(route, message, channel, tickets::print);
    }
}
