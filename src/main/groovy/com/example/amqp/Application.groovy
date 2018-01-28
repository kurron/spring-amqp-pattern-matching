package com.example.amqp

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import java.util.concurrent.ThreadLocalRandom
import org.springframework.amqp.core.*
import org.springframework.amqp.core.Binding as RabbitBinding
import org.springframework.amqp.core.Message as RabbitMessage
import org.springframework.amqp.core.Queue as RabbitQueue
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.Scheduled

import java.util.concurrent.atomic.AtomicInteger

//TODO: showcase a synchronous request-reply scenario.  I want to know how a spy would react in that instance.

@Slf4j
@SpringBootApplication
class Application {

    /**
     * Manages interactions with the AMQP broker.
     */
    @Autowired
    AmqpTemplate template

    /**
     * JSON codec.
     */
    @Autowired
    ObjectMapper mapper

    /**
     * List of all subjects the system supports.
     */
    def subjects = ['dog', 'cat', 'mouse', 'bear', 'spider', 'tiger', 'lion', 'shark']

    @Bean
    HeadersExchange messageRouter() {
        new HeadersExchange( 'message-router' )
    }

    @Bean
    RabbitQueue dogCommandsSpy() {
        new Queue( 'dog-commands-spy' )
    }

    @Bean
    RabbitBinding dogCommandSpyBinding( RabbitQueue dogCommandsSpy, HeadersExchange messageRouter ) {
        // AND logic, all headers must match
        def headers = ['message-type': 'command', 'subject': 'dog'] as Map<String, Object>
        BindingBuilder.bind( dogCommandsSpy ).to( messageRouter ).whereAll( headers ).match()
    }

    @Bean
    RabbitQueue dogCommands() {
        new Queue( 'dog-commands' )
    }

    @Bean
    RabbitBinding dogCommandBinding( RabbitQueue dogCommands, HeadersExchange messageRouter ) {
        // AND logic, all headers must match
        def headers = ['message-type': 'command', 'subject': 'dog'] as Map<String, Object>
        BindingBuilder.bind( dogCommands ).to( messageRouter ).whereAll( headers ).match()
    }

    @Bean
    RabbitQueue allCommands() {
        new Queue( 'all-commands' )
    }

    @Bean
    RabbitBinding lessSpecificCommandBinding( RabbitQueue allCommands, HeadersExchange messageRouter ) {
        // OR logic, only one of the headers must match
        def headers = ['message-type': 'command'] as Map<String, Object>
        BindingBuilder.bind( allCommands ).to( messageRouter ).whereAny( headers ).match()
    }

    @Bean
    RabbitQueue allEvents() {
        new Queue( 'all-events' )
    }

    @Bean
    RabbitBinding lessSpecificEventBinding( RabbitQueue allEvents, HeadersExchange messageRouter ) {
        // OR logic, only one of the headers must match
        def headers = ['message-type': 'event'] as Map<String, Object>
        BindingBuilder.bind( allEvents ).to( messageRouter ).whereAny( headers ).match()
    }

    @Scheduled( fixedRate = 3000L )
    void genericCommandProducer() {
        def selection = topology().get( ThreadLocalRandom.current().nextInt( topology().size() ) )
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
        //log.info( 'Producing message {}', payload )
        template.send( 'message-router', 'should-not-matter', message )
    }

    @Scheduled( fixedRate = 2000L )
    void genericEventProducer() {

        def selection = topology().get( ThreadLocalRandom.current().nextInt( topology().size() ) )
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
        //log.info( 'Producing message {}', payload )
        template.send( 'message-router', 'should-not-matter', message )
    }

    @RabbitListener( queues = 'all-commands' )
    static void allCommands( RabbitMessage message ) {
        dumpMessage( 'all-commands', message )
    }

    @RabbitListener( queues = 'dog-commands' )
    static void dogCommands( RabbitMessage message ) {
        dumpMessage( 'dog-commands', message )
    }

    @RabbitListener( queues = 'dog-commands-spy' )
    static void dogCommandsSpy( RabbitMessage message ) {
        dumpMessage( 'dog-commands-spy', message )
    }

    @RabbitListener( queues = 'all-events' )
    static void allEvents( RabbitMessage message ) {
        dumpMessage( 'all-events', message )
    }

    private static void dumpMessage( String queue, Message message ) {
        def flattened = message.messageProperties.headers.collectMany { key, value ->
            ["${key}: ${value}"]
        }
        log.info( 'From {} {} {}', queue, message.messageProperties.messageId, flattened )
    }

    @Memoized
    private List<ServicePath> topology() {
        def nodeCount = subjects.size(  ) // needs to be a multiple of 4
        int oneQuarter = nodeCount.intdiv( 4 ).intValue()
        int oneHalf = nodeCount.intdiv( 2 ).intValue()
        def nodes = subjects.collect {
            new ServicePath( label: it, errorPercentage: 0, latencyMilliseconds: 0 )
        }
        def bottomTier = (1..oneQuarter).collect { nodes.pop() }.sort()
        def middleTier = (1..oneHalf).collect { nodes.pop() }.sort()
        def topTier = (1..oneQuarter).collect { nodes.pop() }.sort()

        topTier.each { top ->
            def numberToAdd = ThreadLocalRandom.current().nextInt( middleTier.size() ) + 1
            numberToAdd.times {
                top.outbound.add( middleTier.get( ThreadLocalRandom.current().nextInt( middleTier.size() ) ) )
            }
        }

        middleTier.each { middle ->
            def numberToAdd = ThreadLocalRandom.current().nextInt( bottomTier.size() ) + 1
            numberToAdd.times {
                middle.outbound.add( bottomTier.get( ThreadLocalRandom.current().nextInt( bottomTier.size() ) ) )
            }
        }

        topTier
    }

    static void main( String[] args ) {
        SpringApplication.run Application, args
    }
}
