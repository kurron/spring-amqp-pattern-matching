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
    RabbitQueue lessSpecificCommand() {
        new Queue( 'less-specific-command' )
    }

    @Bean
    RabbitBinding lessSpecificCommandBinding( RabbitQueue lessSpecificCommand, HeadersExchange messageRouter ) {
        def headers = ['message-type': 'command'] as Map<String,Object>
        BindingBuilder.bind( lessSpecificCommand ).to( messageRouter ).whereAll( headers ).match()
    }

    @Bean
    RabbitQueue lessSpecificEvent() {
        new Queue( 'less-specific-event' )
    }

    @Bean
    RabbitBinding lessSpecificEventBinding( RabbitQueue lessSpecificEvent, HeadersExchange messageRouter ) {
        def headers = ['message-type': 'event'] as Map<String,Object>
        BindingBuilder.bind( lessSpecificEvent ).to( messageRouter ).whereAll( headers ).match()
    }

    /**
     * Simple counter to show how messages are sequenced in the queue.
     */
    final AtomicInteger counter = new AtomicInteger( 0 )

    @Scheduled( fixedRate = 4000L )
    void lessSpecificCommandProducer() {

        def payload = Integer.toHexString( counter.getAndIncrement() )
        def message = MessageBuilder.withBody( payload.bytes )
                                    .setAppId( 'pattern-matching' )
                                    .setContentType( 'text/plain' )
                                    .setMessageId( UUID.randomUUID() as String )
                                    .setType( 'counter' )
                                    .setTimestamp( new Date() )
                                    .setHeader( 'message-type', 'command' )
                                    .build()
        log.info( 'Producing message {}', payload )
        template.send(  'message-router', 'should-not-matter', message )
    }

    @Scheduled( fixedRate = 2000L )
    void lessSpecificEventProducer() {

        def payload = Integer.toHexString( counter.getAndIncrement() )
        def message = MessageBuilder.withBody( payload.bytes )
                                    .setAppId( 'pattern-matching' )
                                    .setContentType( 'text/plain' )
                                    .setMessageId( UUID.randomUUID() as String )
                                    .setType( 'counter' )
                                    .setTimestamp( new Date() )
                                    .setHeader( 'message-type', 'event' )
                                    .build()
        log.info( 'Producing message {}', payload )
        template.send(  'message-router', 'should-not-matter', message )
    }

    @RabbitListener( queues = 'less-specific-command' )
    static void lessSpecificCommand( RabbitMessage message ) {
        log.info( 'Consuming {} from less-specific-command', message.messageProperties.messageId )
        dumpMessage( message )
    }

    @RabbitListener( queues = 'less-specific-event' )
    static void lessSpecificEvent( RabbitMessage message ) {
        log.info( 'Consuming {} from less-specific-event', message.messageProperties.messageId )
        dumpMessage( message )
    }

    private static void dumpMessage( Message message ) {
        message.messageProperties.headers.every { key, value ->
            log.info('    Header {}: {}', key, value)
        }
        message.messageProperties.properties.every { key, value ->
            log.info('    Property {}: {}', key, value)
        }
    }

    static void main( String[] args ) {
        SpringApplication.run Application, args
    }
}
