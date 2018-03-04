package com.example.amqp

import groovy.transform.Canonical

import java.util.concurrent.ThreadLocalRandom

/**
 * Knows how to generate the service topology.
 */
@Canonical
class TopologyGenerator {

    List<ServicePath> generate( List<String> subjects ) {
        def nodeCount = subjects.size(  ) // needs to be a multiple of 4
        int oneQuarter = nodeCount.intdiv( 4 ).intValue()
        int oneHalf = nodeCount.intdiv( 2 ).intValue()
        def nodes = subjects.collect {
            int latency = ThreadLocalRandom.current().nextInt( 250 )
            int error = ThreadLocalRandom.current().nextInt( 100 )
            new ServicePath( label: it, errorPercentage: error, latencyMilliseconds: latency )
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
}
