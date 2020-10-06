package io.dogy.util;

public interface IFailedMessageHandler {
    void insert(Message<?> message) throws Exception;
}
