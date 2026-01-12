package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.AppConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveMqService implements MessageSender {
    private static final Logger log = Logger.getLogger(ActiveMqService.class.getName());

    private final PooledConnectionFactory pooledFactory;
    private final String queueName;
    private final boolean isStubMode;

    public ActiveMqService(AppConfig config) {
        this.queueName = config.queue();

        this.isStubMode = true;

        if (isStubMode) {
            this.pooledFactory = null;
            log.warning("!!! ActiveMQ Service is in STUB MODE. No real connection. !!!");
        } else {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(config.activeMqUrl());
            connectionFactory.setUserName(config.credentials().username());
            connectionFactory.setPassword(new String(config.credentials().password()));

            this.pooledFactory = new PooledConnectionFactory(connectionFactory);
            this.pooledFactory.setMaxConnections(10);
            this.pooledFactory.setMaximumActiveSessionPerConnection(50);
            log.info("ActiveMQ Pool initialized for: " + config.activeMqUrl());
        }
    }

    @FunctionalInterface
    public interface BatchAction {
        void execute(TransactionalSender sender) throws Exception;
    }

    @FunctionalInterface
    public interface TransactionalSender {
        void send(String message) throws JMSException;
    }

    @Override
    public void send(String message) throws JMSException {
        try {
            sendBatch(sender -> sender.send(message));
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            throw new JMSException("Send failed: " + e.getMessage());
        }
    }

    public void sendBatch(BatchAction action) throws Exception {
        if (isStubMode) {
            log.info(">>> [BATCH START] (Stub transaction)");
            try {
                action.execute(msg -> log.info(">>> [MOCK SEND] " + msg));
                log.info(">>> [BATCH COMMIT] (Stub)");
            } catch (Exception e) {
                log.warning(">>> [BATCH ROLLBACK] (Stub) due to: " + e.getMessage());
                throw e;
            }
            return;
        }

        if (pooledFactory == null) throw new IllegalStateException("ActiveMQ Pool is not initialized");

        Connection connection = null;
        Session session = null;

        try {
            connection = pooledFactory.createConnection();
            connection.start();

            session = connection.createSession(true, Session.SESSION_TRANSACTED);

            Destination destination = session.createQueue(queueName);

            MessageProducer finalProducer = session.createProducer(destination);
            Session finalSession = session;

            TransactionalSender sender = message -> {
                TextMessage textMessage = finalSession.createTextMessage(message);
                finalProducer.send(textMessage);
            };

            action.execute(sender);

            session.commit();
            log.info("JMS Transaction Committed successfully.");

        } catch (Exception e) {
            if (session != null) {
                try {
                    log.warning("Rolling back JMS transaction due to error: " + e.getMessage());
                    session.rollback();
                } catch (JMSException rollbackEx) {
                    log.log(Level.SEVERE, "Failed to rollback transaction", rollbackEx);
                }
            }
            throw e;
        } finally {
            if (session != null) {
                try { session.close(); } catch (JMSException e) { /* ignore */ }
            }
            if (connection != null) {
                try { connection.close(); } catch (JMSException e) { /* ignore */ }
            }
        }
    }

    @Override
    public void close() {
        if (pooledFactory != null) {
            pooledFactory.stop();
        }
    }
}