package com.example.amqp

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import java.util.concurrent.ThreadLocalRandom
import org.springframework.amqp.core.*
import org.springframework.amqp.core.Binding as RabbitBinding
import org.springframework.amqp.core.Queue as RabbitQueue
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.scheduling.annotation.Scheduled

//TODO: showcase a synchronous request-reply scenario.  I want to know how a spy would react in that instance.

@Slf4j
@SpringBootApplication
class Application implements RabbitListenerConfigurer {

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
    def subjects = ['dog', 'cat', 'mouse', 'bear']

    @Bean
    HeadersExchange messageRouter() {
        new HeadersExchange( 'message-router' )
    }

    @Bean
    @Qualifier( 'commands' )
    List<RabbitQueue> commandQueues() {
        subjects.collect {
            QueueBuilder.durable( "${it}-commands" ).build()
        }
    }

    @Bean
    @Qualifier( 'events' )
    List<RabbitQueue> eventQueues() {
        subjects.collect {
            QueueBuilder.durable( "${it}-events" ).build()
        }
    }

    @Bean
    RabbitQueue everyCommandQueue() {
        QueueBuilder.durable( 'all-commands' ).build()
    }

    @Bean
    RabbitQueue everyEventQueue() {
        QueueBuilder.durable( 'all-events' ).build()
    }

    @Bean
    List<RabbitBinding> commandBindings( @Qualifier( 'commands' ) List<RabbitQueue> commandQueues, HeadersExchange messageRouter ) {
        commandQueues.collect {
            def headers = ['message-type': 'command', 'subject': (it.name)] as Map<String, Object>
            BindingBuilder.bind( it ).to( messageRouter ).whereAll( headers ).match()
        }
    }

    @Bean
    List<RabbitBinding> eventBindings( @Qualifier( 'events' ) List<RabbitQueue> eventQueues, HeadersExchange messageRouter ) {
        eventQueues.collect {
            def headers = ['message-type': 'event', 'subject': (it.name)] as Map<String, Object>
            BindingBuilder.bind( it ).to( messageRouter ).whereAll( headers ).match()
        }
    }

    @Bean
    RabbitBinding commandSpyBinding( RabbitQueue everyCommandQueue, HeadersExchange messageRouter ) {
        def headers = ['message-type': 'command'] as Map<String, Object>
        BindingBuilder.bind( everyCommandQueue ).to( messageRouter ).whereAll( headers ).match()
    }

    @Bean
    RabbitBinding eventSpyBinding( RabbitQueue everyEventQueue, HeadersExchange messageRouter ) {
        def headers = ['message-type': 'event'] as Map<String, Object>
        BindingBuilder.bind( everyEventQueue ).to( messageRouter ).whereAll( headers ).match()
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
        //log.info( 'Producing command message {}', payload )
        //template.send( 'message-router', 'should-not-matter', message )
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
        log.info( 'Producing event message {}', payload )
        template.send( 'message-router', 'should-not-matter', message )
    }

    @Override
    void configureRabbitListeners( RabbitListenerEndpointRegistrar registrar ) {
        commandQueues().each {
            def endpoint = new SimpleRabbitListenerEndpoint()
            endpoint.id = "${it.name}-listener"
            endpoint.queues = it
            endpoint.messageListener = new MessageProcessor( it.name )
            registrar.registerEndpoint( endpoint )
        }
        eventQueues().each {
            def endpoint = new SimpleRabbitListenerEndpoint()
            endpoint.id = "${it.name}-listener"
            endpoint.queues = it
            endpoint.messageListener = new MessageProcessor( it.name )
            registrar.registerEndpoint( endpoint )
        }
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
