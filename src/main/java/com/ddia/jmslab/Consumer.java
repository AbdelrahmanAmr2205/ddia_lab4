package com.ddia.jmslab;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Consumer implements ExceptionListener {
    private Connection myConnection;
    private Session mySession;
    private MessageConsumer myConsumer;
    private JmsMetrics metrics;

    public Consumer(JmsMetrics metrics) {
        this.metrics = metrics;
    }

    public void start() {
        this.start("TEST.FOO");
    }

    public void start(String queueName) {
        try {
            // Create a Connection
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            this.myConnection = connectionFactory.createConnection();
            this.myConnection.start();

            this.myConnection.setExceptionListener(this);

            // Create a Session
            this.mySession = this.myConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = this.mySession.createQueue(queueName);
            
            // Create a MessageConsumer from the Session to the Topic or Queue
            this.myConsumer = this.mySession.createConsumer(destination);

        } catch (Exception e) {
            System.err.println("Failed to connect to ActiveMQ broker. Is it running?");
            e.printStackTrace();
            // Fail fast rather than returning silently and causing NullPointerExceptions later
            throw new RuntimeException("JMS Initialization failed", e);
        }
    }

    public void receiveMessage() {
        try {
            // Wait for a message
            long start = System.nanoTime();
            Message message = this.myConsumer.receive(1000);
            long end = System.nanoTime();
            
            // 1. Handle Timeouts Immediately
            if (message == null) {
                // No message received within 1000ms. Do not record metrics.
                return; 
            }

            // 2. Message successfully received, record primary metrics
            metrics.recordResponseTime(end - start);
            metrics.incrementMessageCount();

            // 3. Handle specific message types and latency calculation
            if (message instanceof TextMessage) {
                long receiveTime = System.nanoTime();
                
                // Safely check if the property exists before attempting to use it
                if (message.propertyExists("sendTime")) {
                    long sendTime = message.getLongProperty("sendTime");
                    long delay = receiveTime - sendTime;
                    metrics.recordLatency(delay);
                }

                TextMessage textMessage = (TextMessage) message;
                // System.out.println("Received: " + textMessage.getText());

            } else {
                System.out.println("Received non-text message: " + message);
            }
            
        } catch (Exception e) {
            System.out.println("Caught Exception during receive: " + e);
            e.printStackTrace();
        }
    }

    public void stop() {
        // Clean up safely
        try {
            if (this.myConsumer != null) this.myConsumer.close();
            if (this.mySession != null) this.mySession.close();
            if (this.myConnection != null) this.myConnection.close();
        } catch (Exception e) {
            System.out.println("Caught Exception during shutdown: " + e);
            e.printStackTrace();
        }
    }

    @Override
    public void onException(JMSException exception) {
        System.out.println("JMS Exception occurred:");
        exception.printStackTrace();
    }
}