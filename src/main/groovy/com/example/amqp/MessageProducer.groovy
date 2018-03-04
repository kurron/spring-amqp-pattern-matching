package com.example.amqp

import com.amazonaws.xray.AWSXRay
import com.amazonaws.xray.entities.Namespace
import com.amazonaws.xray.entities.TraceHeader
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.Canonical
import org.springframework.amqp.core.AmqpTemplate
import org.springframework.amqp.core.MessageBuilder
import org.springframework.scheduling.annotation.Scheduled

import javax.naming.TimeLimitExceededException
import java.util.concurrent.ThreadLocalRandom

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

    static void xrayTemplate( String segmentName, Closure logic ) {
        def segment =  AWSXRay.beginSegment( segmentName )
        try {
            segment.setNamespace( Namespace.REMOTE as String )
            def parentSegment = segment.parentSegment
            def header = new TraceHeader( parentSegment.traceId,
                                          parentSegment.sampled ? segment.id : null,
                                          parentSegment.sampled ? TraceHeader.SampleDecision.SAMPLED : TraceHeader.SampleDecision.NOT_SAMPLED )
            segment.putAnnotation( 'subject', 'front-door' )
            def requestInformation = ['url': 'amqp://example.com', 'method': 'command']
            segment.putHttp( 'request', requestInformation )
            logic.call( header )
            segment.putHttp( 'response', ['status': 200] )
        }
        catch ( TimeLimitExceededException e ) {
            segment.addException( e )
            segment.putHttp( 'response', ['status': 429] )
            //throw e
        }
        catch ( Exception e ) {
            segment.addException( e )
            segment.putHttp( 'response', ['status': 500] )
            //throw e
        }
        finally {
            AWSXRay.endSegment()
        }
    }

    @Scheduled( fixedRate = 3000L )
    void genericCommandProducer() {
        xrayTemplate( 'front-door' ) { TraceHeader header ->
            def selection = topology.get( ThreadLocalRandom.current().nextInt( topology.size() ) )
            def payload = mapper.writeValueAsString( selection )
            def message = MessageBuilder.withBody( payload.bytes )
                                        .setAppId( 'pattern-matching' )
                                        .setContentType( 'text/plain' )
                                        .setMessageId( UUID.randomUUID() as String )
                                        .setType( 'service-call' )
                                        .setTimestamp( new Date() )
                                        .setCorrelationIdString( UUID.randomUUID() as String )
                                        .setHeader( 'message-type', 'command' )
                                        .setHeader( 'subject', selection.label )
                                        .setHeader( TraceHeader.HEADER_KEY, header as String )
                                        .build()
            //log.info( 'Producing command message {}', payload )
            def response = template.sendAndReceive( 'message-router', 'should-not-matter', message )
            if ( !response ) {
                throw new TimeLimitExceededException(  'Reply took too long!' )
            }
        }
    }

    //@Scheduled( fixedRate = 2000L )
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
