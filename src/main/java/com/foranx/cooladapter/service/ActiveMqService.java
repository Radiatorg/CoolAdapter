package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.AppConfig;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveMqService implements MessageSender {
    private static final Logger log = Logger.getLogger(ActiveMqService.class.getName());

    private final PooledConnectionFactory pooledFactory;
    private final String queueName;

    public ActiveMqService(AppConfig config) {
        this.queueName = config.queue();

        // --- ЗАГЛУШКА (STUB MODE) ---
        // Мы пока не инициализируем соединение, чтобы приложение работало без MQ
        this.pooledFactory = null;
        log.warning("!!! ActiveMQ Service is in STUB MODE. No real connection will be established. !!!");
        log.info("Target URL (ignored): " + config.activeMqUrl());

    /*
    // КОД ДЛЯ ПРОДАКШЕНА (Раскомментировать, когда поднимете ActiveMQ)
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(config.activeMqUrl());
    connectionFactory.setUserName(config.credentials().username());
    connectionFactory.setPassword(new String(config.credentials().password()));

    this.pooledFactory = new PooledConnectionFactory(connectionFactory);
    this.pooledFactory.setMaxConnections(10);
    this.pooledFactory.setMaximumActiveSessionPerConnection(50);
    log.info("ActiveMQ Pool initialized for: " + config.activeMqUrl());
    */
    }

    @Override
    public void send(String message) throws JMSException {
        // --- ЗАГЛУШКА ---
        // Просто пишем в лог, что "якобы" отправили
        log.info(">>> [MOCK SEND] To Queue [" + queueName + "]: " + message);

    /*
    // КОД ДЛЯ ПРОДАКШЕНА
    if (pooledFactory == null) throw new IllegalStateException("ActiveMQ Pool is not initialized");

    try (Connection connection = pooledFactory.createConnection()) {
        connection.start();
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);

            TextMessage textMessage = session.createTextMessage(message);
            producer.send(textMessage);
        }
    }
    */
    }

    @Override
    public void close() {
        if (pooledFactory != null) {
            pooledFactory.stop();
        }
    }
}