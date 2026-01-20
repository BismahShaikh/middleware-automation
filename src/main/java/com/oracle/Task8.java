package com.oracle;

import org.apache.activemq.spring.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jdbc.JdbcComponent;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.dbcp2.BasicDataSource;

import com.oracle.util.MessageProcessor;


public class Task8 {
	
	public static void main(String[] args) throws Exception {
		CamelContext context = new DefaultCamelContext();
		ActiveMQConnectionFactory connFact = new ActiveMQConnectionFactory();
		context.addComponent("jms", JmsComponent.jmsComponentAutoAcknowledge(connFact));
		
		MessageProcessor messageProcessor = new MessageProcessor();
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName("");
		ds.setUrl("");
		ds.setUsername("");
		ds.setPassword("");

		JdbcComponent jdbcComponent = new JdbcComponent();
		jdbcComponent.setDataSource(ds);
		context.addComponent("jdbc", jdbcComponent);
		context.getRegistry().bind("dataSource", ds);
		
		
		
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				
				from("file-watch:C:\\camel-files\\input"
						+ "?events=MODIFY"
					    + "&recursive=false")
				.filter(header("CamelFileName").isEqualTo("testfile1.txt"))
				.routeId("stage-0")
				.log("[Step - 1.0] Application Started")			
				.convertBodyTo(String.class)
				.log("[Step - 1.1] Pushing message to the input queue.")
				.to("jms:queue:input-queue")
				.log("[Step - 1.1] Message sent to the input queue.");
				
				from("jms:queue:input-queue")
				.routeId("stage-1")
				.log("[Step - 2.1] Received message from input queue")

				.doTry()

				    // ================= VALIDATION =================
				    .process(exchange -> {
				        String jsonPayload = exchange.getIn().getBody(String.class);
				        messageProcessor.receiveValidateTransform(jsonPayload, exchange);
				    })

				    // ================= INSERT P =================
				    .log("[Step - 3.1] Validation success")
				    .setBody(simple(
				        "Insert into tmisaudit (tmisId, correlationId, payload, sentToTarget, createdDate) " +
				        "VALUES ('${header.tmisId}', '${header.correlationId}', '${header.payload}', 'P', SYSTIMESTAMP)"
				    ))
				    .to("jdbc:dataSource")
				    .log("[Step - 3.2] Data inserted with status P")

				    // ================= SEND TO OUTPUT =================
				    .log("[Step - 4.1] Sending transformed data to output queue")
				    .setHeader("JMSCorrelationID", header("tmisId"))
				    .setBody(header("payload"))
				    .to("jms:queue:output-queue")

				    .log("[Step - 4.2] Transformed data sent to output queue")

				    // ================= UPDATE Y =================
				    .setBody(simple(
				        "update tmisaudit set sentToTarget = 'Y' " +
				        "where tmisId = '${header.tmisId}' AND correlationId = '${header.correlationId}'"
				    ))
				    .to("jdbc:dataSource")
				    .log("[Step - 4.3] Data updated with status Y")

				.doCatch(IllegalArgumentException.class)

				    // ================= INSERT E =================
				    .log("[ERROR] Validation failed: ${exception.message}")
				    .setBody(simple(
				        "Insert into tmisaudit (tmisId, correlationId, payload, sentToTarget, createdDate) " +
				        "VALUES ('ERR-${date:now:yyyyMMddHHmmss}', 'NA', '${body}', 'E', SYSTIMESTAMP)"
				    ))
				    .to("jdbc:dataSource")
				    .log("[Step - 3.E] Error entry inserted with status E")

				.end();

				
			}
		});

		context.start();
		Thread.currentThread().join();
		context.stop();
	}
}
