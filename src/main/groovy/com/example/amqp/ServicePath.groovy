package com.example.amqp

import com.fasterxml.jackson.annotation.JsonProperty
import groovy.transform.Canonical

/**
 * This object contains the full path that a message must take to be processed.
 */
@Canonical
class ServicePath implements Comparable<ServicePath> {

    /**
     * Names of the service to route to.  Mainly used for logging.
     */
    @JsonProperty( 'label' )
    String label

    /**
     * The percentage of requests that will result in a fault. Range is 0 to 100.
     */
    @JsonProperty( 'error-percentage' )
    int errorPercentage

    /**
     * Number of milliseconds to delay the service, emulating processing time. Range is 0 to 1000.
     */
    @JsonProperty( 'latency-milliseconds' )
    int latencyMilliseconds

    /**
     * Collection of downstream services that this service must wait for before it can return.
     */
    @JsonProperty( 'outbound' )
    final SortedSet<ServicePath> outbound = new TreeSet<>()

    @Override
    int compareTo(ServicePath o) {
        label <=> o.label
    }
}
