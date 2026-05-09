package com.jmslab;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Consumer implements ExceptionListener{
    private Connection myConnection;
    private Session mySession;
    private MessageConsumer myConsumer;
    private JmsMetrics metrics;

    public Consumer(JmsMetrics metrics) {
        this.metrics = metrics;
    }

    public void start(){
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

        }catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    public void receiveMessage() throws Exception{
        try {
            // Wait for a message
            long start = System.nanoTime();
            Message message = this.myConsumer.receive(1000);
            long end = System.nanoTime();
            
            metrics.recordResponseTime(end - start);
            metrics.incrementMessageCount();

            if (message instanceof TextMessage) {
                long receiveTime = System.nanoTime();
                long sendTime = message.getLongProperty("sendTime");

                long delay = receiveTime - sendTime;

                metrics.recordLatency(delay);

                TextMessage textMessage = (TextMessage) message;
                // System.out.println("Received: " + textMessage.getText());

            } else {
                // if(message == null) {
                //     System.out.println("No message received");
                // }else{
                //     System.out.println("Received: " + message);
                // }
            }
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
            throw e;
        }
    }

    public void stop(){
        // Clean up
        try{
            this.myConsumer.close();
            this.mySession.close();
            this.myConnection.close();
        }catch (Exception e){
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }
    @Override
    public void onException(JMSException exception) {
        System.out.println("JMS Exception occurred:");
        exception.printStackTrace();
    }


}
