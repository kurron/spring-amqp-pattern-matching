package org.kurron.amqp

class TopologyLearningTest {

    static void main(String[] args) {

        def rawNodeCount = 49
        def nodeCount = rawNodeCount < 4 ? 4 : 0 == rawNodeCount % 2 ? rawNodeCount : rawNodeCount + 1
        int oneQuarter = nodeCount / 4
        int oneHalf = nodeCount / 2
        def nodes = (1..nodeCount).collect { it }
        def topTier = nodes.take( oneQuarter )
        def middleTier = nodes.take( oneHalf )
        def bottomTier = nodes.take( oneQuarter )
        println "Node count is ${nodeCount}"
    }
}
