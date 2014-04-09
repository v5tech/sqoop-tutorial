package net.aimeizi.sqoop.tutorial;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.validation.Status;

public class SqoopTutorial {

	public static void main(String[] args) {
		
		String url = "http://master:12000/sqoop/";
		SqoopClient client = new SqoopClient(url);
		
		//createConnection(client);
		
		//modifyConnection(client);
		
	}

	/**
	 * 更新Connection
	 * @param client
	 */
	private static void modifyConnection(SqoopClient client) {
		MConnection connection = client.getConnection(2);
		connection.setName("mycon");
		client.updateConnection(connection);
	}

	/**
	 * 创建Connection
	 * @param client
	 */
	private static void createConnection(SqoopClient client) {
		//Dummy connection object
		MConnection newCon = client.newConnection(1);

		//Get connection and framework forms. Set name for connection
		MConnectionForms conForms = newCon.getConnectorPart();
		MConnectionForms frameworkForms = newCon.getFrameworkPart();
		newCon.setName("MyConnection");

		//Set connection forms values
		conForms.getStringInput("connection.connectionString").setValue("jdbc:mysql://master/sqoop");
		conForms.getStringInput("connection.jdbcDriver").setValue("com.mysql.jdbc.Driver");
		conForms.getStringInput("connection.username").setValue("sqoop");
		conForms.getStringInput("connection.password").setValue("sqoop");

		frameworkForms.getIntegerInput("security.maxConnections").setValue(10);

		Status status  = client.createConnection(newCon);
		if(status.canProceed()) {
		 System.out.println("Created. New Connection ID : " +newCon.getPersistenceId());
		} else {
		 System.out.println("Check for status and forms error ");
		}
	}

}
