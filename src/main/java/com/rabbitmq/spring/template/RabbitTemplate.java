package com.rabbitmq.spring.template;

import com.rabbitmq.client.*;
import com.rabbitmq.spring.ExchangeType;
import com.rabbitmq.spring.channel.RabbitChannelFactory;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.io.Serializable;

/**
 * N.B. Be careful: This class it NOT thread safe.
 * For thread save rabbit template, use: ASyncRabbitTemplate
 */
public class RabbitTemplate implements InitializingBean, ShutdownListener, ReturnListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitTemplate.class);

    public static final ExchangeType DEFAULT_EXCHANGE_TYPE = ExchangeType.DIRECT;
    public static final String DEFAULT_ROUTING_KEY = "#";

    private RabbitChannelFactory channelFactory;
    private String exchange;
    private ExchangeType exchangeType = DEFAULT_EXCHANGE_TYPE;
    private String routingKey = DEFAULT_ROUTING_KEY;
    private boolean mandatory;
    private boolean immediate;


    private Channel channel;

    public void send(Serializable object) {
        send(object, routingKey, mandatory, immediate);
    }

    public void send(Serializable object, String routingKey) {
        send(object, routingKey, mandatory, immediate);
    }

    public void send(Serializable object, boolean mandatory, boolean immediate) {
        send(object, routingKey, mandatory, immediate);
    }

    public void send(Serializable object, String routingKey, boolean mandatory, boolean immediate) {
        LOGGER.trace("Sending object [%s] with routingKey [{}] - mandatory [{}] - immediate [{}]",
                new Object[]{object, routingKey, mandatory, immediate});

        try {
            channel.basicPublish(exchange, routingKey, mandatory, immediate, null, SerializationUtils.serialize(object));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        exchangeType.validateRoutingKey(routingKey);
        connectChannel();
    }

    private void connectChannel() {
        if (channel == null || !channel.isOpen()) {
            try {
                channel = channelFactory.createChannel();
                channel.getConnection().addShutdownListener(this);
                channel.setReturnListener(this);
                channel.exchangeDeclare(exchange, exchangeType.toString());

                LOGGER.info("Connected to exchange [{}({})] - routingKey [{}]", new Object[]{exchange, exchangeType, routingKey});
            } catch (IOException e) {
                LOGGER.warn("Unable to connect channel", e);
            }
        }
    }

    @Required
    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    @Required
    public void setChannelFactory(RabbitChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        LOGGER.info("Channel connection lost for reason [{}]", cause.getReason());
        LOGGER.info("Reference [{}]", cause.getReference());

        if (cause.isInitiatedByApplication()) {
            LOGGER.info("Shutdown initiated by application");
        } else if (cause.isHardError()) {
            LOGGER.error("Shutdown is a hard error, trying to reconnect the channel...");
            connectChannel();
        }
    }

    @Override
    public void handleBasicReturn(int replyCode, String replyText, String exchange, String routingKey,
                                  AMQP.BasicProperties properties, byte[] body) throws IOException {

        LOGGER.warn("Got message back from server [{}] - [{}]", replyCode, replyText);
        handleReturn(replyCode, replyText, exchange, routingKey, properties, body);
    }


    /**
     * Callback hook for returned messages, overwrite where needed.
     * Will only be called when sending with 'immediate' or 'mandatory' set to true.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) {

    }

    public void setMandatory(boolean mandatory) {
        this.mandatory = mandatory;
    }

    public void setImmediate(boolean immediate) {
        this.immediate = immediate;
    }

    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
    }
}
