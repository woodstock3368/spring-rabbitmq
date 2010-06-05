package com.rabbitmq.spring.channel;

import com.rabbitmq.client.*;
import com.rabbitmq.spring.connection.RabbitConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Set;

public class RabbitChannelFactory implements DisposableBean, ShutdownListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitChannelFactory.class);

    public static final int DEFAULT_CLOSE_CODE = AMQP.REPLY_SUCCESS;
    public static final String DEFAULT_CLOSE_MESSAGE = "Goodbye";

    private RabbitConnectionFactory connectionFactory;
    private int closeCode = DEFAULT_CLOSE_CODE;
    private String closeMessage = DEFAULT_CLOSE_MESSAGE;

    private final Set<Reference<Channel>> channelReferenceSet = new HashSet<Reference<Channel>>();

    public void setConnectionFactory(RabbitConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public void setCloseCode(int closeCode) {
        this.closeCode = closeCode;
    }

    public void setCloseMessage(String closeMessage) {
        this.closeMessage = closeMessage;
    }

    public Channel createChannel() throws IOException {

        LOGGER.debug("Creating channel");

        Connection connection = connectionFactory.getConnection();
        connection.addShutdownListener(this);
        Channel channel = connection.createChannel();
        channelReferenceSet.add(new WeakReference<Channel>(channel));

        LOGGER.debug("Created channel nr. {}", channel.getChannelNumber());
        return channel;
    }

    @Override
    public void destroy() throws Exception {
        closeChannels();
    }

    private void closeChannels() {
        LOGGER.debug("Closing '{}' channels", channelReferenceSet.size());

        for (Reference<Channel> channelReference : channelReferenceSet) {

            try {
                Channel channel = channelReference.get();
                if (channel != null && channel.isOpen()) {
                    if (channel.getConnection().isOpen()) {
                        channel.close(closeCode, closeMessage);
                    }
                }
            } catch (IOException e) {
                LOGGER.error("Error closing channel", e);
            }
        }

        LOGGER.debug("All channels closed");
        channelReferenceSet.clear();

    }

    @Override
    public void shutdownCompleted(ShutdownSignalException cause) {
        if (cause.isInitiatedByApplication()) {
            LOGGER.debug("Shutdown by application completed for reference [{}] - reason [{}]",
                    cause.getReference(), cause.getReason());

        } else if (cause.isHardError()) {
            LOGGER.error("Hard error shutdown completed for reference [{}] - reason [{}]",
                    cause.getReference(), cause.getReason());
        }

        LOGGER.debug("Shutdown completed");
    }
}
