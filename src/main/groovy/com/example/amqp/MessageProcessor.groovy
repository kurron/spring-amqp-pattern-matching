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
    void onMessage( Message incoming ) {
        dumpMessage( queueName, incoming )
        def servicePath = mapper.readValue( incoming.body, ServicePath )
        log.debug( 'Simulating latency of {} milliseconds', servicePath.latencyMilliseconds )
        Thread.sleep( servicePath.latencyMilliseconds )
        servicePath.outbound.each {
            def payload = mapper.writeValueAsString( it )
            def outgoing = MessageBuilder.withBody( payload.bytes )
                                         .setAppId( 'pattern-matching' )
                                         .setContentType( 'text/plain' )
                                         .setMessageId( UUID.randomUUID() as String )
                                         .setType( 'counter' )
                                         .setTimestamp( new Date() )
                                         .setHeader( 'message-type', 'command' )
                                         .setHeader( 'subject', it.label )
                                         .build()
            //log.info( 'Producing command message {}', payload )
            template.send( 'message-router', 'should-not-matter', outgoing )
        }
    }

    private static void dumpMessage( String queue, Message message ) {
        def flattened = message.messageProperties.headers.collectMany { key, value ->
            ["${key}: ${value}"]
        }
        log.info( 'From {} {} {}', queue, message.messageProperties.messageId, flattened )
    }
}
