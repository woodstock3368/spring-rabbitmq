package com.rabbitmq.spring.listener;

import com.rabbitmq.client.*;
import com.rabbitmq.spring.ExchangeType;
import com.rabbitmq.spring.channel.RabbitChannelFactory;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.MethodInvoker;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

public class RabbitMessageListenerAdapter implements Consumer, InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMessageListenerAdapter.class);

    public static final String DEFAULT_LISTENER_METHOD = "handleMessage";
    public static final int DEFAULT_POOL_SIZE = 1;
    public static final String DEFAULT_ROUTING_KEY = "#";
    public static final ExchangeType DEFAULT_EXCHANGE_TYPE = ExchangeType.DIRECT;

    private Object delegate;

    private RabbitChannelFactory channelFactory;
    private String exchange;
    private ExchangeType exchangeType = DEFAULT_EXCHANGE_TYPE;
    private String queueName;
    private String routingKey = DEFAULT_ROUTING_KEY;
    private String listenerMethod = DEFAULT_LISTENER_METHOD;
    private int poolsize = DEFAULT_POOL_SIZE;

    private Channel channel;

    @Override
    public void afterPropertiesSet() throws Exception {

        exchangeType.validateRoutingKey(routingKey);

        startConsumer();
    }

    private void startConsumer() {
        if (channel == null || !channel.isOpen()) {
            try {
                channel = channelFactory.createChannel();
                String internalQueueName;
                if (queueName == null) {
                    // declare anonymous queue and get name for binding and consuming
                    internalQueueName = channel.queueDeclare().getQueue();
                } else {
                    internalQueueName = channel.queueDeclare(queueName, false, false, false, true, null).getQueue();
                }
                channel.exchangeDeclare(exchange, exchangeType.toString());
                channel.queueBind(internalQueueName, exchange, routingKey);

                for (int index = 1; index <= poolsize; index++) {
                    channel.basicConsume(internalQueueName, this);
                    LOGGER.info("Started consumer {} on exchange [{}({})] - queue [{}] - routingKey [{}]",
                            new Object[]{index, exchange, exchangeType, queueName, routingKey});
                }
            } catch (IOException e) {
                LOGGER.warn("Unable start consumer", e);
            }
        }
    }

    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    @Required
    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    @Required
    public void setChannelFactory(RabbitChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    @Override
    public void handleConsumeOk(String consumerTag) {
        LOGGER.trace("handleConsumeOk [{}]", consumerTag);
    }

    @Override
    public void handleCancelOk(String consumerTag) {
        LOGGER.trace("handleCancelOk [{}]", consumerTag);
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException cause) {
        LOGGER.info("Channel connection lost for reason [{}]", cause.getReason());
        LOGGER.info("Reference [{}]", cause.getReference());

        if (cause.isInitiatedByApplication()) {
            LOGGER.info("Shutdown initiated by application");
        } else if (cause.isHardError()) {
            LOGGER.error("Shutdown is a hard error, trying to restart consumer(s)...");
            startConsumer();
        }
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {

        LOGGER.debug("Handling message with tag [{}] on [{}]", consumerTag, envelope.getRoutingKey());
        try {

            Object message = SerializationUtils.deserialize(body);
            // Invoke the handler method with appropriate arguments.
            Object result = invokeListenerMethod(listenerMethod, new Object[]{message});

            if (result != null && result instanceof Serializable) {
                handleResult((Serializable) result, envelope, properties);
            } else {
                LOGGER.trace("No result object given - no result to handle");
            }
        } finally {
            channel.basicAck(envelope.getDeliveryTag(), false);
        }

    }

    private void handleResult(Serializable result, Envelope envelope, AMQP.BasicProperties properties) throws IOException {
        if (properties.getReplyTo() != null) {
            channel.basicPublish(envelope.getExchange(), properties.getReplyTo(), null, SerializationUtils.serialize(result));
        }
    }

    protected Object invokeListenerMethod(String methodName, Object[] arguments) {
        try {
            MethodInvoker methodInvoker = new MethodInvoker();
            methodInvoker.setTargetObject(getDelegate());
            methodInvoker.setTargetMethod(methodName);
            methodInvoker.setArguments(arguments);
            methodInvoker.prepare();
            return methodInvoker.invoke();
        }
        catch (InvocationTargetException ex) {
            Throwable targetEx = ex.getTargetException();
            throw new ListenerExecutionFailedException(
                    String.format("Listener method '%s' threw exception", methodName), targetEx);
        }
        catch (Throwable ex) {
            throw new ListenerExecutionFailedException(
                    String.format("Failed to invoke target method '%s' with arguments %s", methodName, ObjectUtils.nullSafeToString(arguments)), ex
            );
        }
    }

    @Required
    public void setDelegate(Object delegate) {
        this.delegate = delegate;
    }

    public Object getDelegate() {
        return delegate;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public void setListenerMethod(String listenerMethod) {
        this.listenerMethod = listenerMethod;
    }

    public void setPoolsize(int poolsize) {
        this.poolsize = poolsize;
    }

    private static class ListenerExecutionFailedException extends RuntimeException {
        public ListenerExecutionFailedException(String s, Throwable targetEx) {
            super(s, targetEx);
        }
    }
}
