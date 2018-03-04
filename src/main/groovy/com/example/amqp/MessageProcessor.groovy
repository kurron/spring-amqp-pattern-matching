package com.example.amqp

import com.amazonaws.xray.AWSXRay
import com.amazonaws.xray.entities.TraceHeader
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
        try {
            dumpMessage( queueName, incoming )

            def traceString = incoming.messageProperties.headers.get( TraceHeader.HEADER_KEY ) as String
            def incomingHeader = TraceHeader.fromString( traceString )
            def traceId = incomingHeader.rootTraceId
            def parentId = incomingHeader.parentId
            def name = "${incoming.messageProperties.headers.get( 'message-type' ) as String}/${incoming.messageProperties.headers.get( 'subject' ) as String}"
            def created = AWSXRay.globalRecorder.beginSegment( name, traceId, parentId )
            def header = new TraceHeader( created.traceId,
                                          created.sampled ? created.id : null,
                                          created.sampled ? TraceHeader.SampleDecision.SAMPLED : TraceHeader.SampleDecision.NOT_SAMPLED )

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
                                             .setHeader( TraceHeader.HEADER_KEY, header as String )
                                             .build()
                //log.info( 'Producing command message {}', payload )
                template.send( 'message-router', 'should-not-matter', outgoing )
            }
        }
        catch ( Exception e ) {
            AWSXRay.globalRecorder.currentSegment.addException( e )
            throw e
        }
        finally {
            AWSXRay.globalRecorder.endSegment()
        }
    }

    private static void dumpMessage( String queue, Message message ) {
        def flattened = message.messageProperties.headers.collectMany { key, value ->
            ["${key}: ${value}"]
        }
        log.info( 'From {} {} {}', queue, message.messageProperties.messageId, flattened )
    }
}
