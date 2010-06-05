package com.rabbitmq.spring.connection;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.util.*;
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

        while (connection == null || !connection.isOpen()) {
            ConnectionParameters connectionParameters = connectionFactory.getParameters();

            LOGGER.info("Establishing connection to one of [{}] using virtualhost [{}]",
                    ObjectUtils.nullSafeToString(hosts), connectionParameters.getVirtualHost());

            try {
                connection = connectionFactory.newConnection(knownHosts);

                // always keep the original hosts
                Set<Address> hosts = new HashSet<Address>(Arrays.asList(knownHosts));
                hosts.addAll(Arrays.asList(connection.getKnownHosts()));
                knownHosts = hosts.toArray(new Address[hosts.size()]);

                LOGGER.debug("New known hosts list is [{}]", ObjectUtils.nullSafeToString(knownHosts));

                addShutdownListeners();

                LOGGER.info("Connected to [{}:{}]", connection.getHost(), connection.getPort());
            } catch (Exception e) {
                LOGGER.error("Error connecting, trying again in 5 seconds...", e);
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e1) {
                    LOGGER.warn("Interrupted while waiting");
                }
            }

        }

        return connection;

    }

    private void collectInitialKnownHosts() {
        List<Address> addresses = new ArrayList<Address>(hosts.length);
        for (String host : hosts) {
            addresses.add(Address.parseAddress(host));
        }
        knownHosts = addresses.toArray(new Address[hosts.length]);
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
