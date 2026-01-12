package com.foranx.cooladapter.service;

public interface MessageSender extends AutoCloseable {
    void send(String message) throws Exception;

    @Override
    void close();
}