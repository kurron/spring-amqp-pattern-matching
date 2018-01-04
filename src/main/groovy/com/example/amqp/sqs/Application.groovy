package com.example.amqp.sqs

import groovy.util.logging.Slf4j
import org.springframework.amqp.core.AmqpTemplate
import org.springframework.amqp.core.BindingBuilder
import org.springframework.amqp.core.HeadersExchange
import org.springframework.amqp.core.Queue
import org.springframework.amqp.rabbit.annotation.RabbitListener
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.messaging.handler.annotation.Headers
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
    RabbitQueue lessSpecific() {
        new Queue( 'less-specific' )
    }

    @Bean
    RabbitBinding lessSpecificBinding( RabbitQueue lessSpecific, HeadersExchange messageRouter ) {
        def headers = ['message-type': 'command'] as Map<String,Object>
        BindingBuilder.bind( lessSpecific ).to( messageRouter ).whereAll( headers ).match()
    }

    /**
     * Simple counter to show how messages are sequenced in the queue.
     */
    final AtomicInteger counter = new AtomicInteger( 0 )

    @Scheduled( fixedRate = 1000L )
    void producer() {

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

    @RabbitListener( queues = 'less-specific' )
    static String lessSpecific( @Headers Map<String, String> headers, String payload ) {
        log.info( 'Consuming {} from less-specific', payload )
        headers.every { key, value ->
            log.info( '    {}: {}', key, value )
        }
        payload.toUpperCase()
    }

    static void main( String[] args ) {
        SpringApplication.run Application, args
    }
}
