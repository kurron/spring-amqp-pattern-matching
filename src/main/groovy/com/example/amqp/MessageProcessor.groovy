package com.example.amqp

import com.amazonaws.xray.AWSXRay
import com.amazonaws.xray.entities.TraceHeader
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.util.logging.Slf4j
import org.springframework.amqp.core.AmqpTemplate
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageBuilder
import org.springframework.amqp.core.MessageListener

import java.util.concurrent.ThreadLocalRandom

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

    void xrayTemplate( Message incoming, Closure logic ) {
        def traceString = incoming.messageProperties.headers.get( TraceHeader.HEADER_KEY ) as String
        def name = "${incoming.messageProperties.headers.get( 'subject' ) as String}"
        def incomingHeader = TraceHeader.fromString( traceString )
        def traceId = incomingHeader.rootTraceId
        def parentId = incomingHeader.parentId
        def segment = AWSXRay.beginSegment( name, traceId, parentId )
        try {
            def header = new TraceHeader( segment.traceId,
                                          segment.sampled ? segment.id : null,
                                          segment.sampled ? TraceHeader.SampleDecision.SAMPLED : TraceHeader.SampleDecision.NOT_SAMPLED )
            segment.putAnnotation( 'subject', name )
            def requestInformation = ['url': 'amqp://example.com', 'method': 'command']
            segment.putHttp( 'request', requestInformation )

            dumpMessage( queueName, incoming )
            logic.call( incoming, header )
            segment.putHttp( 'response', ['status': 200] )
        }
        catch ( Exception e ) {
            segment.addException( e )
            throw e
        }
        finally {
            AWSXRay.endSegment()
        }
    }

    @Override
    void onMessage( Message incoming ) {
        xrayTemplate( incoming ) {  Message message, TraceHeader header ->
            def servicePath = mapper.readValue( message.body, ServicePath )
            log.debug( 'Simulating latency of {} milliseconds', servicePath.latencyMilliseconds )
            Thread.sleep( servicePath.latencyMilliseconds )
            def simulateFailure = ThreadLocalRandom.current().nextInt( 100 ) < servicePath.errorPercentage
            if ( simulateFailure ) {
                throw new IllegalStateException( 'Simulated failure!' )
            }
            def toSend = servicePath.outbound.collect {
                createDownstreamMessage( it, header )
            }
            def responses = toSend.collect {
                template.sendAndReceive('message-router', 'should-not-matter', it )
            }
            //responses.each { if ( !it ) throw new IllegalStateException( 'Reply timed out!' ) }
            def returnAddress = message.messageProperties.replyToAddress
            template.send( returnAddress.exchangeName, returnAddress.routingKey, createResponseMessage( message.messageProperties.correlationIdString, servicePath.label ) )
        }
    }

    Message createDownstreamMessage( ServicePath command, TraceHeader header ) {
        def payload = mapper.writeValueAsString( command )
        MessageBuilder.withBody( payload.bytes )
                .setAppId( 'pattern-matching' )
                .setContentType( 'text/plain' )
                .setMessageId( UUID.randomUUID() as String )
                .setType( 'foo' )
                .setTimestamp( new Date() )
                .setCorrelationIdString( UUID.randomUUID() as String )
                .setHeader( 'message-type', 'command' )
                .setHeader( 'subject', command.label )
                .setHeader( TraceHeader.HEADER_KEY, header as String )
                .build()
    }

    static Message createResponseMessage(String correlationID, String subject ) {
        MessageBuilder.withBody( 'Empty response'.bytes )
                .setAppId( 'pattern-matching' )
                .setContentType( 'text/plain' )
                .setMessageId( UUID.randomUUID() as String )
                .setType( 'foo' )
                .setTimestamp( new Date() )
                .setCorrelationIdString( correlationID )
                .setHeader( 'message-type', 'response' )
                .setHeader( 'subject', subject )
                .build()
    }

    private static void dumpMessage( String queue, Message message ) {
        def flattened = message.messageProperties.headers.collectMany { key, value ->
            ["${key}: ${value}"]
        }
        log.info( 'From {} {} {}', queue, message.messageProperties.messageId, flattened )
    }
}
