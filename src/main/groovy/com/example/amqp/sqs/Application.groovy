package com.example.amqp.sqs

import groovy.util.logging.Slf4j
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
    RabbitQueue userCommandsSpy() {
        new Queue( 'user-commands-spy' )
    }

    @Bean
    RabbitBinding userCommandSpyBinding( RabbitQueue userCommandsSpy, HeadersExchange messageRouter ) {
        def headers = ['message-type': 'command', 'subject': 'user'] as Map<String,Object>
        BindingBuilder.bind( userCommandsSpy ).to( messageRouter ).whereAll( headers ).match()
    }

    @Bean
    RabbitQueue userCommands() {
        new Queue( 'user-commands' )
    }

    @Bean
    RabbitBinding userCommandBinding( RabbitQueue userCommands, HeadersExchange messageRouter ) {
        def headers = ['message-type': 'command', 'subject': 'user'] as Map<String,Object>
        BindingBuilder.bind( userCommands ).to( messageRouter ).whereAll( headers ).match()
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
        dumpMessage( 'all-commands', message )
    }

    @RabbitListener( queues = 'user-commands' )
    static void userCommands(RabbitMessage message ) {
        dumpMessage( 'user-commands', message )
    }

    @RabbitListener( queues = 'user-commands-spy' )
    static void userCommandsSpy(RabbitMessage message ) {
        dumpMessage( 'user-commands-spy', message )
    }

    @RabbitListener( queues = 'all-events' )
    static void allEvents(RabbitMessage message ) {
        dumpMessage( 'all-events', message )
    }

    private static void dumpMessage( String queue, Message message ) {
        def flattened = message.messageProperties.headers.collectMany { key, value ->
            ["${key}: ${value}"]
        }
        log.info('From {} {} {}', queue, message.messageProperties.messageId, flattened )
    }

    static void main( String[] args ) {
        SpringApplication.run Application, args
    }
}
