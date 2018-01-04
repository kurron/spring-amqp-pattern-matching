# Overview
This project is a sample that helps to showcase how you can use [Spring AMQP](https://projects.spring.io/spring-amqp/) to support the pattern matching as outlined in [The Tao of Microservices](https://www.safaribooksonline.com/library/view/the-tao-of/9781617293146/).  The basic idea is that message passing is the primary communication mechanism within the system and that routing of message to the appropriate services is done by pattern matching.  Luckily, the AMQP specification allows us to do just that.

Routing of messages is done via AMQP headers, not to be confused with AMP Properties which are completely different.  In order for routing to work, your system needs to settle on a handful of key-value pairs -- headers, which AMQP can use to forward the message onto the appropriate queues.  The headers are arbitrary but it seems practical to break things down in this manner:

* message-type -- will be one of `command` or `event`.  A command is an imperative to the system to take some sort of action, such as changing state.  An event is a fact -- an immutable description of something that has already taken place.  A command is meant to be *consumed* while events are meant to be *observed*.  The distinction is that only one consumer can process a command while multiple consumers can process an event.
* subject -- the target of the message.  In the case of a command, which object to apply the command to.  In the case of an event, which object is the fact about?

These two properties provide a lot of flexibility in controlling which queues get a copy of the message.  For example, if all messages are sent to a single header exchange then queues can be configured like so:

* I want all command messages
* I want all event messages
* I want all commands targeting the user
* I want all events targeting the user

One interesting scenario is to have messages flowing to a legacy service to also flow to an in-development version, allowing it to experience production traffic without disrupting existing systems.  Another interesting concept, canary releases, is not supported out of the box but you can simulate it with a little work.

If we adjust our message producers to add an additional boolean property, `canary`, we can flow a percentage of messages to a queue where the value is `true`.  Unfortunately, the producer has to be aware that a canary release is in play and toggle a percentage of the generated messages to have a `true` value.  This implies that a common message producer library is deployed throughout the system, allowing Operations this level of control.  It is possible that a service mesh, such as [Istio](https://istio.io/), might be able handle setting of the canary property, freeing the producer have having to know about deployment-time decisions.

# Guidebook
Details about this project are contained in the [guidebook](guidebook/guidebook.md) and should be considered mandatory reading prior to contributing to this project.

# Prerequisites
* [JDK 8](http://zulu.org/) installed and working
* a RabbitMQ instance up and running (the default configuration expects it to be at localhost)

# Building
`./gradlew` will pull down any dependencies, compile the source and package everything up.

# Installation
Nothing to install.

# Tips and Tricks
## Starting The Server
`./gradlew clean bootRun` will start the server on port `8080` and begin producing and cosuming messages. You should see something similar to this:

```
2018-01-04 16:43:07.396  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From user-commands 6a152daa-ef70-4630-9c92-21332115d005 [subject: user, message-type: command]
2018-01-04 16:43:07.396  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From all-commands 6a152daa-ef70-4630-9c92-21332115d005 [subject: user, message-type: command]
2018-01-04 16:43:07.399  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From user-commands-spy 6a152daa-ef70-4630-9c92-21332115d005 [subject: user, message-type: command]
2018-01-04 16:43:07.400  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From all-events e831e103-cc44-4373-9727-b134c5865f24 [message-type: event]
2018-01-04 16:43:07.400  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From all-commands 4d76a40b-6d8e-4671-adbe-cef04ac265fb [message-type: command]
2018-01-04 16:43:09.371  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From all-events 8397efe7-99c1-4a8d-adc9-89dfecdfa682 [message-type: event]
2018-01-04 16:43:10.372  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From all-commands ed13af8f-b778-4b6e-8c19-d7c83462d3e2 [message-type: command]
2018-01-04 16:43:11.371  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From all-commands 8ec56c5e-04f9-442a-aa5d-81ae535cb6f1 [subject: user, message-type: command]
2018-01-04 16:43:11.371  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From user-commands-spy 8ec56c5e-04f9-442a-aa5d-81ae535cb6f1 [subject: user, message-type: command]
2018-01-04 16:43:11.372  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From user-commands 8ec56c5e-04f9-442a-aa5d-81ae535cb6f1 [subject: user, message-type: command]
2018-01-04 16:43:11.373  INFO 21112 --- [cTaskExecutor-1] com.example.amqp.Application             : From all-events 407234c9-1fba-44b3-84f9-599739ed1431 [message-type: event]
```

# Troubleshooting

# Contributing

# License and Credits
* This project is licensed under the [Apache License Version 2.0, January 2004](http://www.apache.org/licenses/).
* The guidebook structure was created by [Simon Brown](http://simonbrown.je/) as part of his work on the [C4 Architectural Model](https://c4model.com/).  His books can be [purchased from LeanPub](https://leanpub.com/b/software-architecture).
* Patrick Kua offered [his thoughts on a travel guide to a software system](https://www.safaribooksonline.com/library/view/oreilly-software-architecture/9781491985274/video315451.html) which has been [captured in this template](travel-guide/travel-guide.md).
* [The Tao of Microservices](https://www.safaribooksonline.com/library/view/the-tao-of/9781617293146/) by Richard Rodger.

# List of Changes
