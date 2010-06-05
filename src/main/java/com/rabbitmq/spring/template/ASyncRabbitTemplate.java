package com.rabbitmq.spring.template;

import com.rabbitmq.spring.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ASyncRabbitTemplate extends RabbitTemplate implements DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(ASyncRabbitTemplate.class);

    private static final int MAXIMUM_POOL_SIZE = 50;
    private static final int CORE_POOL_SIZE = 10;

    private final ThreadPoolExecutor messageSender = new ThreadPoolExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE,
            60L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
    }

    @Override
    public void destroy() {
        LOGGER.debug("Shutting down message sender thread pool...");
        Utils.shutdownTreadPool(messageSender, 2L, TimeUnit.MINUTES);
    }

    @Override
    public void send(Serializable object, String routingKey, boolean mandatory, boolean direct) {
        //TODO: what to do if  messageSender stopped but we have send method call?
        messageSender.submit(new SenderTask(new RabbitMessage(object, routingKey, mandatory, direct)));
    }

    private void sendMessage(RabbitMessage message) {
        super.send(message.getObject(), message.routingKey, message.isMandatory(), message.isDirect());
    }

    private final class RabbitMessage {

        private final Serializable object;
        private final String routingKey;
        private final boolean mandatory;
        private final boolean direct;

        private RabbitMessage(Serializable object, String routingKey, boolean mandatory, boolean direct) {
            this.object = object;
            this.routingKey = routingKey;
            this.mandatory = mandatory;
            this.direct = direct;
        }

        public Serializable getObject() {
            return object;
        }

        public String getRoutingKey() {
            return routingKey;
        }

        public boolean isMandatory() {
            return mandatory;
        }

        public boolean isDirect() {
            return direct;
        }
    }

    private class SenderTask implements Runnable {

        private final RabbitMessage rabbitMessage;

        SenderTask(RabbitMessage rabbitMessage) {
            this.rabbitMessage = rabbitMessage;
        }

        @Override
        public void run() {
            sendMessage(rabbitMessage);
        }
    }
}
