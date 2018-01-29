package com.example.amqp

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.Canonical
import java.util.concurrent.ThreadLocalRandom
import org.springframework.amqp.core.AmqpTemplate
import org.springframework.amqp.core.MessageBuilder
import org.springframework.scheduling.annotation.Scheduled

@Canonical
class MessageProducer {

    private final List<ServicePath> topology

    /**
     * Manages interactions with the AMQP broker.
     */
    private final AmqpTemplate template

    /**
     * JSON codec.
     */
    private final ObjectMapper mapper

    MessageProducer( List<ServicePath> aTopology, AmqpTemplate aTemplate,  ObjectMapper aMapper ) {
        topology = aTopology
        template = aTemplate
        mapper = aMapper
    }

    @Scheduled( fixedRate = 3000L )
    void genericCommandProducer() {
        def selection = topology.get( ThreadLocalRandom.current().nextInt( topology.size() ) )
        def payload = mapper.writeValueAsString( selection )
        def message = MessageBuilder.withBody( payload.bytes )
                                    .setAppId( 'pattern-matching' )
                                    .setContentType( 'text/plain' )
                                    .setMessageId( UUID.randomUUID() as String )
                                    .setType( 'counter' )
                                    .setTimestamp( new Date() )
                                    .setHeader( 'message-type', 'command' )
                                    .setHeader( 'subject', selection.label )
                                    .build()
        //log.info( 'Producing command message {}', payload )
        template.send( 'message-router', 'should-not-matter', message )
    }

    @Scheduled( fixedRate = 2000L )
    void genericEventProducer() {

        def selection = topology.get( ThreadLocalRandom.current().nextInt( topology.size() ) )
        def payload = mapper.writeValueAsString( selection )
        def message = MessageBuilder.withBody( payload.bytes )
                                    .setAppId( 'pattern-matching' )
                                    .setContentType( 'text/plain' )
                                    .setMessageId( UUID.randomUUID() as String )
                                    .setType( 'counter' )
                                    .setTimestamp( new Date() )
                                    .setHeader( 'message-type', 'event' )
                                    .setHeader( 'subject', selection.label )
                                    .build()
        //log.info( 'Producing event message {}', payload )
        template.send( 'message-router', 'should-not-matter', message )
    }

}
