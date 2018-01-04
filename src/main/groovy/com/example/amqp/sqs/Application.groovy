package com.example.amqp.sqs

import groovy.util.logging.Slf4j
import org.springframework.amqp.core.AmqpTemplate
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.HeadersExchange
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.Queue
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.amqp.core.Message as RabbitMessage
import org.springframework.messaging.MessageHeaders
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.amqp.core.MessageBuilder
import org.springframework.amqp.core.Queue as RabbitQueue
import org.springframework.amqp.core.Binding as RabbitBinding
import java.util.concurrent.atomic.AtomicInteger

@Slf4j
@SpringBootApplication
class Application {

    /**
     * Manages interactions with the AMQP broker.
     */
    @Autowired
    AmqpTemplate template

    @Bean
    HeadersExchange messageRouter() {
        new HeadersExchange( 'message-router' )
    }

    @Bean
    RabbitQueue allCommands() {
        new Queue( 'all-commands' )
    }

    @Bean
    RabbitBinding lessSpecificCommandBinding( RabbitQueue allCommands, HeadersExchange messageRouter ) {
        def headers = ['message-type': 'command'] as Map<String,Object>
        BindingBuilder.bind( allCommands ).to( messageRouter ).whereAny( headers ).match()
    }

    @Bean
    RabbitQueue allEvents() {
        new Queue( 'all-events' )
    }

    @Bean
    RabbitBinding lessSpecificEventBinding( RabbitQueue allEvents, HeadersExchange messageRouter ) {
        def headers = ['message-type': 'event'] as Map<String,Object>
        BindingBuilder.bind( allEvents ).to( messageRouter ).whereAny( headers ).match()
    }

    /**
     * Simple counter to show how messages are sequenced in the queue.
     */
    final AtomicInteger counter = new AtomicInteger( 0 )

    @Scheduled( fixedRate = 4000L )
    void userCommandCommandProducer() {

        def payload = Integer.toHexString( counter.getAndIncrement() )
        def message = MessageBuilder.withBody( payload.bytes )
                                    .setAppId( 'pattern-matching' )
                                    .setContentType( 'text/plain' )
                                    .setMessageId( UUID.randomUUID() as String )
                                    .setType( 'counter' )
                                    .setTimestamp( new Date() )
                                    .setHeader( 'message-type', 'command' )
                                    .setHeader( 'subject', 'user' )
                                    .build()
        //log.info( 'Producing message {}', payload )
        template.send(  'message-router', 'should-not-matter', message )
    }

    @Scheduled( fixedRate = 3000L )
    void genericCommandProducer() {

        def payload = Integer.toHexString( counter.getAndIncrement() )
        def message = MessageBuilder.withBody( payload.bytes )
                                    .setAppId( 'pattern-matching' )
                                    .setContentType( 'text/plain' )
                                    .setMessageId( UUID.randomUUID() as String )
                                    .setType( 'counter' )
                                    .setTimestamp( new Date() )
                                    .setHeader( 'message-type', 'command' )
                                    .build()
        //log.info( 'Producing message {}', payload )
        template.send(  'message-router', 'should-not-matter', message )
    }

    @Scheduled( fixedRate = 2000L )
    void genericEventProducer() {

        def payload = Integer.toHexString( counter.getAndIncrement() )
        def message = MessageBuilder.withBody( payload.bytes )
                                    .setAppId( 'pattern-matching' )
                                    .setContentType( 'text/plain' )
                                    .setMessageId( UUID.randomUUID() as String )
                                    .setType( 'counter' )
                                    .setTimestamp( new Date() )
                                    .setHeader( 'message-type', 'event' )
                                    .build()
        //log.info( 'Producing message {}', payload )
        template.send(  'message-router', 'should-not-matter', message )
    }

    @RabbitListener( queues = 'all-commands' )
    static void allCommands(RabbitMessage message ) {
        log.info( 'Consumed from all-commands' )
        dumpMessage( message )
    }

    @RabbitListener( queues = 'all-events' )
    static void allEvents(RabbitMessage message ) {
        log.info( 'Observed from all-events' )
        dumpMessage( message )
    }

    private static void dumpMessage( Message message ) {
        def flattened = message.messageProperties.headers.collectMany { key, value ->
            ["${key}: ${value}"]
        }
        log.info('    {} {}', message.messageProperties.messageId, flattened )
    }

    static void main( String[] args ) {
        SpringApplication.run Application, args
    }
}
