package com.example.amqp

import groovy.util.logging.Slf4j
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageListener

@Slf4j
class MessageProcessor implements MessageListener {

    private final String queueName

    MessageProcessor( String queueName ) {
        this.queueName = queueName
    }

    @Override
    void onMessage( Message message ) {
        dumpMessage( queueName, message )
    }

    private static void dumpMessage( String queue, Message message ) {
        def flattened = message.messageProperties.headers.collectMany { key, value ->
            ["${key}: ${value}"]
        }
        log.info( 'From {} {} {}', queue, message.messageProperties.messageId, flattened )
    }
}
