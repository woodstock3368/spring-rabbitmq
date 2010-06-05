package com.rabbitmq.spring.remoting;

import com.rabbitmq.client.*;
import com.rabbitmq.spring.ExchangeType;
import com.rabbitmq.spring.InvalidRoutingKeyException;
import com.rabbitmq.spring.channel.RabbitChannelFactory;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.remoting.support.RemoteInvocation;
import org.springframework.remoting.support.RemoteInvocationBasedExporter;
import org.springframework.remoting.support.RemoteInvocationResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RabbitInvokerServiceExporter extends RemoteInvocationBasedExporter implements InitializingBean, DisposableBean, ShutdownListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitInvokerServiceExporter.class);

    private RabbitChannelFactory channelFactory;
    private String exchange;
    private ExchangeType exchangeType;
    private String queueName;
    private String routingKey;

    private Object proxy;
    private List<RpcServer> rpcServerPool;
    private int poolsize = 1;

    public void afterPropertiesSet() {

        if (exchangeType.equals(ExchangeType.FANOUT)) {
            throw new InvalidRoutingKeyException(
                    String.format("Exchange type %s not allowed for service exporter", exchangeType));
        }

        exchangeType.validateRoutingKey(routingKey);

        proxy = getProxyForService();

        rpcServerPool = new ArrayList<RpcServer>(poolsize);

        startRpcServer();
    }

    private void startRpcServer() {
        try {
            LOGGER.info("Creating channel and rpc server");
            Channel tmpChannel = channelFactory.createChannel();
            tmpChannel.getConnection().addShutdownListener(this);
            tmpChannel.queueDeclare(queueName, false, false, false, true, null);
            if (exchange != null) {
                tmpChannel.exchangeDeclare(exchange, exchangeType.toString());
                tmpChannel.queueBind(queueName, exchange, routingKey);
            }


            for (int i = 1; i <= poolsize; i++) {
                try {
                    Channel channel = channelFactory.createChannel();

                    LOGGER.info("Starting rpc server {} on exchange [{}({})] - queue [{}] - routingKey [{}]",
                            new Object[]{i, exchange, exchangeType, queueName, routingKey});
                    final RpcServer rpcServer = createRpcServer(channel);
                    rpcServerPool.add(rpcServer);

                    Runnable main = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                throw rpcServer.mainloop();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    };
                    new Thread(main).start();
                } catch (IOException e) {
                    LOGGER.warn("Unable to create rpc server", e);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected error trying to start rpc servers", e);
        }
    }

    private RpcServer createRpcServer(Channel channel) throws IOException {
        return new RpcServer(channel, queueName) {

            @Override
            public byte[] handleCall(byte[] requestBody, AMQP.BasicProperties replyProperties) {

                RemoteInvocation invocation = (RemoteInvocation) SerializationUtils.deserialize(requestBody);
                RemoteInvocationResult result = invokeAndCreateResult(invocation, proxy);
                return SerializationUtils.serialize(result);

            }
        };
    }

    public void setChannelFactory(RabbitChannelFactory channelFactory) {
        this.channelFactory = channelFactory;
    }

    @Required
    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public Object getProxy() {
        return proxy;
    }

    @Override
    public void destroy() throws Exception {
        clearRpcServers();
    }

    private void clearRpcServers() {
        LOGGER.info("Closing {} rpc servers", rpcServerPool.size());

        for (RpcServer rpcServer : rpcServerPool) {
            try {
                rpcServer.terminateMainloop();
                rpcServer.close();
            } catch (Exception e) {
                LOGGER.warn("Error termination rpcserver loop", e);
            }
        }
        rpcServerPool.clear();
        LOGGER.info("Rpc servers closed");

    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        LOGGER.info("Channel connection lost for reason [{}]", cause.getReason());
        LOGGER.info("Reference [{}]", cause.getReference());

        if (cause.isInitiatedByApplication()) {
            LOGGER.info("Shutdown initiated by application");
        } else if (cause.isHardError()) {
            LOGGER.error("Shutdown is a hard error, trying to restart the RPC server...");
            startRpcServer();
        }
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    @Required
    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public void setPoolsize(int poolsize) {
        this.poolsize = poolsize;
    }

    @Required
    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
    }
}
