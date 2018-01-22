package org.kurron.amqp

class TopologyLearningTest {

    static void main(String[] args) {

        def nodeCount = 28
        if ( 0 != nodeCount % 4 ) {
            throw new IllegalArgumentException( 'Node count must be multiple of 4' )
        }
        int oneQuarter = nodeCount / 4
        int oneHalf = nodeCount / 2
        def nodes = (1..nodeCount).collect { new Node( it as String ) }
        def bottomTier = (1..oneQuarter).collect { nodes.pop() }.sort()
        def middleTier = (1..oneHalf).collect { nodes.pop() }.sort()
        def topTier = (1..oneQuarter).collect { nodes.pop() }.sort()
        println "Node count is ${nodeCount}"
        println "topTier is ${topTier}"
        println "middleTier is ${middleTier}"
        println "bottomTier is ${bottomTier}"
    }

    static class Node {
        final String label

        Node(String label) {
            this.label = label
        }

        @Override
        String toString() {
            label
        }
    }
}
