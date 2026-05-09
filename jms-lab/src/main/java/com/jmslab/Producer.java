package com.jmslab;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Producer {
    private Connection myConnection;
    private Session mySession;
    private MessageProducer myProducer;
    private JmsMetrics myMetrics;

    public Producer(JmsMetrics metrics) {
        this.myMetrics = metrics;
    }

    public void start() {
        this.start("TEST.FOO");
    }
    public void start(String queueName){
        try {
            // Create a Connection
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
            this.myConnection = connectionFactory.createConnection();
            this.myConnection.start();

            // Create a Session
            this.mySession = this.myConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = this.mySession.createQueue(queueName);

            // Create a MessageProducer from the Session to the Topic or Queue
            this.myProducer = this.mySession.createProducer(destination);
            this.myProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

        }catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }
    public void sendMessage(String text) throws Exception {
        try {
            long sendTime = System.nanoTime();

            // Create a messages
            TextMessage message = this.mySession.createTextMessage(text);
            message.setLongProperty("sendTime", sendTime);

            // Tell the producer to send the message
            // System.out.println("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
            this.myProducer.send(message);
            long end = System.nanoTime();

            // Response time
            myMetrics.recordResponseTime(end - sendTime);
            myMetrics.incrementMessageCount();
        }
        catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
            throw e;
        }
    }

    public void stop(){
        // Clean up
        try{
            this.mySession.close();
            this.myConnection.close();
        }catch (Exception e){
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

}

