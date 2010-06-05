package com.rabbitmq.spring.utils;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by IntelliJ IDEA.
 * User: Sargis Harutyunyan
 * Date: 5 juin 2010
 * Time: 22:03:13
 */
public class Utils {
    public static void shutdownTreadPool(ThreadPoolExecutor messageSender, long timeout, TimeUnit unit) {
        messageSender.shutdown();
        try {
            if (messageSender.awaitTermination(timeout, unit)) {
                messageSender.shutdownNow();
            }
        } catch (InterruptedException ignore) {
        }
    }
}
