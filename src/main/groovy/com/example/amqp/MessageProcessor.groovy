package com.example.amqp

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.util.logging.Slf4j
import org.springframework.amqp.core.AmqpTemplate
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageBuilder
import org.springframework.amqp.core.MessageListener

@Slf4j
class MessageProcessor implements MessageListener {

    private final String queueName

    /**
     * JSON codec.
     */
    private final ObjectMapper mapper

    private final AmqpTemplate template

    MessageProcessor( String queueName, ObjectMapper mapper, AmqpTemplate template ) {
        this.queueName = queueName
        this.mapper = mapper
        this.template = template
    }

    @Override
    void onMessage( Message message ) {
        dumpMessage( queueName, message )
        def servicePath = mapper.readValue( message.body, ServicePath )
        Thread.sleep( servicePath.latencyMilliseconds )
        servicePath.outbound.each {
            // send the payload onto RabbitMQ
            def payload = mapper.writeValueAsString( it )
            def messageX = MessageBuilder.withBody( payload.bytes )
                                        .setAppId( 'pattern-matching' )
                                        .setContentType( 'text/plain' )
                                        .setMessageId( UUID.randomUUID() as String )
                                        .setType( 'counter' )
                                        .setTimestamp( new Date() )
                                        .setHeader( 'message-type', 'command' )
                                        .setHeader( 'subject', it.label )
                                        .build()
            //log.info( 'Producing command message {}', payload )
            template.send( 'message-router', 'should-not-matter', messageX )

        }
        ''
    }

    private static void dumpMessage( String queue, Message message ) {
        def flattened = message.messageProperties.headers.collectMany { key, value ->
            ["${key}: ${value}"]
        }
        log.info( 'From {} {} {}', queue, message.messageProperties.messageId, flattened )
    }
}
