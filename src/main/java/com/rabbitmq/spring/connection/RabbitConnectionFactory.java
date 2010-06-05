package com.rabbitmq.spring.connection;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RabbitConnectionFactory implements DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitConnectionFactory.class);

    // spring injected
    private ConnectionFactory connectionFactory;
    private String[] hosts;
    private ShutdownListener[] shutdownListeners;

    private Connection connection;
    private Address[] knownHosts;

    public synchronized Connection getConnection() throws IOException {

        if (knownHosts == null) {
            collectInitialKnownHosts();
        }

        while (isConnectionOpened()) {
            ConnectionParameters connectionParameters = connectionFactory.getParameters();

            LOGGER.info("Establishing connection to one of [{}] using virtualhost [{}]",
                    hosts, connectionParameters.getVirtualHost());

            try {
                connection = connectionFactory.newConnection(knownHosts);
                LOGGER.debug("New known hosts list is [{}]", connection.getKnownHosts());

                addShutdownListeners();

                LOGGER.info("Connected to [{}:{}]", connection.getHost(), connection.getPort());
            } catch (IOException e) {
                LOGGER.error("Error connecting, trying again in 5 seconds...", e);
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e1) {
                    LOGGER.warn("Interrupted while waiting for reconnecnting");
                    break;
                }
            }

        }

        return connection;

    }

    private boolean isConnectionOpened() {
        return connection == null || !connection.isOpen();
    }

    private void collectInitialKnownHosts() {
        knownHosts = new Address[hosts.length];
        for (int index = 0; index < hosts.length; index++) {
            knownHosts[index] = Address.parseAddress(hosts[index]);

        }
    }

    private void addShutdownListeners() {
        if (shutdownListeners != null) {
            for (ShutdownListener shutdownListener : shutdownListeners) {
                connection.addShutdownListener(shutdownListener);
            }
        }
    }

    @Override
    public void destroy() throws Exception {
        if (connection != null && connection.isOpen()) {
            connection.close();
        }
    }

    @Required
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    @Required
    public void setHosts(String[] hosts) {
        this.hosts = hosts;
    }

    public void setShutdownListeners(ShutdownListener... shutdownListeners) {
        this.shutdownListeners = shutdownListeners;
    }
}
