package com.oracle.automation;

import java.sql.*;
import javax.jms.*;
import org.apache.activemq.ActiveMQConnectionFactory;

public class AutomationTest {

    private static final String ORACLE_URL =
            "jdbc:oracle:thin:@localhost:1521:FREE";
    private static final String ORACLE_USER = "System";
    private static final String ORACLE_PASS = "Bismah786";

    private static final String ACTIVEMQ_URL = "tcp://localhost:61616";
    private static final String OUTPUT_QUEUE = "output-queue";

    public static void main(String[] args) throws Exception {

        // 1️⃣ Fetch latest DB record
        String tmisId;
        String status;

        try (java.sql.Connection conn = DriverManager.getConnection(
                ORACLE_URL, ORACLE_USER, ORACLE_PASS);
             PreparedStatement ps = conn.prepareStatement(
                "SELECT tmisId, sentToTarget FROM tmisaudit " +
                "ORDER BY createdDate DESC FETCH FIRST 1 ROWS ONLY");
             ResultSet rs = ps.executeQuery()) {

            if (!rs.next()) {
                throw new RuntimeException("No DB record found");
            }
            tmisId = rs.getString("tmisId");
            status = rs.getString("sentToTarget");
        }

        System.out.println("TMIS ID : " + tmisId);
        System.out.println("DB Status : " + status);

        // 2️⃣ Validate DB
        if ("E".equals(status)) {
            System.out.println("NEGATIVE CASE CONFIRMED");
            System.out.println("Queue Validation SKIPPED");
            return;
        }

        if (!"Y".equals(status)) {
            throw new RuntimeException("Unexpected DB status: " + status);
        }

        System.out.println("POSITIVE CASE CONFIRMED");

        // 3️⃣ Validate queue (CONSUME, not browse)
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory(ACTIVEMQ_URL);

        try (javax.jms.Connection connection = factory.createConnection()) {
            connection.start();

            Session session =
                    connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            String selector = "JMSCorrelationID = '" + tmisId + "'";
            MessageConsumer consumer =
                    session.createConsumer(
                        session.createQueue(OUTPUT_QUEUE), selector);

            Message msg = consumer.receive(15000);

            if (msg == null) {
                throw new RuntimeException("Queue Validation FAILED");
            }

            System.out.println("Queue Validation PASSED");
        }
    }
}
