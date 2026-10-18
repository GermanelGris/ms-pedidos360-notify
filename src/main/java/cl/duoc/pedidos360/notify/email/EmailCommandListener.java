package cl.duoc.pedidos360.notify.email;

import java.io.IOException;

import com.rabbitmq.client.Channel;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import cl.duoc.pedidos360.notify.config.RabbitMessagingProperties;
import cl.duoc.pedidos360.notify.messaging.CommandAcknowledger;

/** Consumidor del dominio notificaciones al cliente: cola {@code pedidos360.rabbitmq.email.queue}. */
@Component
public class EmailCommandListener {

    private final CommandAcknowledger acknowledger;
    private final EmailNotificationService emails;
    private final RabbitMessagingProperties.Route route;

    public EmailCommandListener(CommandAcknowledger acknowledger, EmailNotificationService emails,
                                RabbitMessagingProperties properties) {
        this.acknowledger = acknowledger;
        this.emails = emails;
        this.route = properties.email();
    }

    @RabbitListener(id = "email-commands", queues = "${pedidos360.rabbitmq.email.queue}")
    public void onMessage(Message message, Channel channel) throws IOException {
        acknowledger.process(route, message, channel, emails::send);
    }
}
