package org.kurron.amqp

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingDeque
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ThreadLocalRandom

class TopologyLearningTest {

    static void main(String[] args) {

        def nodeCount = 12
        if ( 0 != nodeCount % 4 ) {
            throw new IllegalArgumentException( 'Node count must be multiple of 4' )
        }
        int oneQuarter = nodeCount.intdiv( 4 ).intValue()
        int oneHalf = nodeCount.intdiv( 2 ).intValue()
        def nodes = (1..nodeCount).collect { new Node( it as String ) }
        def bottomTier = (1..oneQuarter).collect { nodes.pop() }.sort()
        def middleTier = (1..oneHalf).collect { nodes.pop() }.sort()
        def topTier = (1..oneQuarter).collect { nodes.pop() }.sort()
        println "Node count is ${nodeCount}"
        println "topTier is ${topTier}"
        println "middleTier is ${middleTier}"
        println "bottomTier is ${bottomTier}"

        // top node gets 1..N middle nodes
        // middle node gets 1..N bottom nodes

        topTier.each { top->
            def numberToAdd = ThreadLocalRandom.current().nextInt( middleTier.size() ) + 1
            numberToAdd.times {
                top.outbound.add( middleTier.get( ThreadLocalRandom.current().nextInt( middleTier.size() ) ) )
            }
        }
        middleTier.each { middle->
            def numberToAdd = ThreadLocalRandom.current().nextInt( bottomTier.size() ) + 1
            numberToAdd.times {
                middle.outbound.add( bottomTier.get( ThreadLocalRandom.current().nextInt( bottomTier.size() ) ) )
            }
        }

        def mapper = new ObjectMapper()
        topTier.each {
            def json = mapper.writeValueAsString( it )
            println json
        }

        ''
    }

    static class Node implements Comparable<Node> {
        @JsonProperty( 'label' )
        final String label

        @JsonProperty( 'error-percentage' )
        final int errorPercentage

        @JsonProperty( 'latency-milliseconds' )
        final int latencyMilliseconds

        @JsonProperty( 'outbound' )
        final SortedSet<Node> outbound = new TreeSet<>()

        Node(String label) {
            this.label = label
        }

        @Override
        String toString() {
            label
        }

        @Override
        int compareTo(Node o) {
            label <=> o.label
        }
    }
}
