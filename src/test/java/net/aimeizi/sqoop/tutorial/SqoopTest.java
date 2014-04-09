package net.aimeizi.sqoop.tutorial;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SqoopTest{

	String url = "http://master:12000/sqoop/";
	SqoopClient client;
	
	@Before
	public void init(){
		client = new SqoopClient(url);
	}
	
	/**
	 * 创建Connection
	 */
	@Test
	public void createConnection(){
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
	
	/**
	 * 修改Connection
	 */
	@Test
	public void modifyConnection() {
		MConnection connection = client.getConnection(2);
		connection.setName("mycon");
		client.updateConnection(connection);
		System.out.println(connection.getName());
	}

	/**
	 * 打印ValidationMessage
	 */
	@Test
	public void printValidationMessage(){
		MConnection connection = client.getConnection(1);
		// List<MForm> formList = connection.getConnectorPart().getForms();
		List<MForm> formList = connection.getFrameworkPart().getForms();
		System.out.println(formList.size());
		for (MForm form : formList) {
			List<MInput<?>> inputlist = form.getInputs();
			if (form.getValidationMessage() != null) {
				System.out.println("Form message: "
						+ form.getValidationMessage());
			}
			for (MInput<?> minput : inputlist) {
				if (minput.getValidationStatus() == Status.ACCEPTABLE) {
					System.out.println("Warning:"
							+ minput.getValidationMessage());
				} else if (minput.getValidationStatus() == Status.UNACCEPTABLE) {
					System.out
							.println("Error:" + minput.getValidationMessage());
				} else if (minput.getValidationStatus() == Status.FINE) {
					System.out.println("FINE:" + minput.getValidationMessage());
				}
			}
		}
	}
	
	/**
	 * 获取所有的job
	 */
	@Test
	public void getJobs(){
		List<MJob> jobs = client.getJobs();
		for (MJob mJob : jobs) {
			System.out.println(mJob.getName()+","+client.getConnection(mJob.getConnectionId()).getName()+","+mJob.getType()+","+client.getConnector(mJob.getConnectorId()).getClassName());
		}
	}
	
	
	/**
	 * 获取job
	 * @return 
	 */
	public MJob getJob(long jid){
		return client.getJob(jid);
	}
	
	
	/**
	 * 创建Import Job
	 */
	@Test
	public void createImportJob(){
		
		//Creating dummy job object
		MJob newjob = client.newJob(1, org.apache.sqoop.model.MJob.Type.IMPORT);
		MJobForms connectorForm = newjob.getConnectorPart();
		MJobForms frameworkForm = newjob.getFrameworkPart();
		
		newjob.setName("ImportJob");
		//Database configuration
		connectorForm.getStringInput("table.schemaName").setValue("sqoop");
		//Input either table name or sql
		connectorForm.getStringInput("table.tableName").setValue("cities");
		//connectorForm.getStringInput("table.sql").setValue("select id,country,city from cities where ${CONDITIONS}");
		connectorForm.getStringInput("table.columns").setValue("id,country,city");
		connectorForm.getStringInput("table.partitionColumn").setValue("id");
		//Set boundary value only if required
		//connectorForm.getStringInput("table.boundaryQuery").setValue("");

		//Output configurations
		frameworkForm.getEnumInput("output.storageType").setValue("HDFS");
		frameworkForm.getEnumInput("output.outputFormat").setValue("TEXT_FILE");//Other option: SEQUENCE_FILE
		frameworkForm.getStringInput("output.outputDirectory").setValue("/output");

		//Job resources
		frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
		frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

		Status status = client.createJob(newjob);
		if(status.canProceed()) {
		 System.out.println("New Job ID: "+ newjob.getPersistenceId());
		} else {
		 System.out.println("Check for status and forms error ");
		}

		//Print errors or warnings
		printMessage(newjob.getConnectorPart().getForms());
		printMessage(newjob.getFrameworkPart().getForms());
		
		System.out.println(getJob(newjob.getPersistenceId()).getName());
		
	}
	
	
	/**
	 * 创建Export Job
	 */
	@Test
	public void createExportJob(){
		MJob newjob = client.newJob(1, org.apache.sqoop.model.MJob.Type.EXPORT);
		MJobForms connectorForm = newjob.getConnectorPart();
		MJobForms frameworkForm = newjob.getFrameworkPart();

		newjob.setName("ExportJob");
		//Database configuration
		connectorForm.getStringInput("table.schemaName").setValue("sqoop");
		//Input either table name or sql
		connectorForm.getStringInput("table.tableName").setValue("cities");
		//connectorForm.getStringInput("table.sql").setValue("select id,country,city from cities where ${CONDITIONS}");
		connectorForm.getStringInput("table.columns").setValue("id,country,city");

		//Input configurations
		frameworkForm.getStringInput("input.inputDirectory").setValue("/output");

		//Job resources
		frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
		frameworkForm.getIntegerInput("throttling.loaders").setValue(1);

		Status status = client.createJob(newjob);
		if(status.canProceed()) {
		  System.out.println("New Job ID: "+ newjob.getPersistenceId());
		} else {
		  System.out.println("Check for status and forms error ");
		}

		//Print errors or warnings
		printMessage(newjob.getConnectorPart().getForms());
		printMessage(newjob.getFrameworkPart().getForms());
		
		System.out.println(getJob(newjob.getPersistenceId()).getName());
		
	}
	
	/**
	 * 异步执行一个Job
	 */
	@Test
	public void submissionAsynchronousJob(){
		//Job submission start
		MSubmission submission = client.startSubmission(1);
		System.out.println("Status : " + submission.getStatus());
		if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
		  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
		System.out.println("Hadoop job id :" + submission.getExternalId());
		System.out.println("Job link : " + submission.getExternalLink());
		Counters counters = submission.getCounters();
		if(counters != null) {
		  System.out.println("Counters:");
		  for(CounterGroup group : counters) {
		    System.out.print("\t");
		    System.out.println(group.getName());
		    for(Counter counter : group) {
		      System.out.print("\t\t");
		      System.out.print(counter.getName());
		      System.out.print(": ");
		      System.out.println(counter.getValue());
		    }
		  }
		}
		if(submission.getExceptionInfo() != null) {
		  System.out.println("Exception info : " +submission.getExceptionInfo());
		}

		//Check job status
		checkJobStatus();

		//Stop a running job
		stopJob();
	}

	
	
	/**
	 * 同步执行一个Job
	 */
	@Test
	public void submissionSynchronousJob(){
		
		//Job submission start
		try {
			client.startSubmission(1,new SubmissionCallback() {
				
				@Override
				public void updated(MSubmission submission) {
					
					if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
					  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
					}
					
				}
				
				@Override
				public void submitted(MSubmission submission) {
					
					if(submission.getExceptionInfo() != null) {
					  System.out.println("Exception info : " +submission.getExceptionInfo());
					}
					
					System.out.println("Status : " + submission.getStatus());
					
//					if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
//					  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
//					}
					
				}
				
				@Override
				public void finished(MSubmission submission) {
					
					System.out.println("Hadoop job id :" + submission.getExternalId());
					System.out.println("Job link : " + submission.getExternalLink());
					
					Counters counters = submission.getCounters();
					if(counters != null) {
					  System.out.println("Counters:");
					  for(CounterGroup group : counters) {
					    System.out.print("\t");
					    System.out.println(group.getName());
					    for(Counter counter : group) {
					      System.out.print("\t\t");
					      System.out.print(counter.getName());
					      System.out.print(": ");
					      System.out.println(counter.getValue());
					    }
					  }
					}
					
					//client.stopSubmission(1);
					
				}
			},3600);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * Stop a running job
	 */
	private void stopJob() {
		client.stopSubmission(1);
	}

	/**
	 * Check job status
	 */
	@Test
	public void checkJobStatus() {
		MSubmission submission;
		submission = client.getSubmissionStatus(1);
		if(submission.getStatus().isRunning() && submission.getProgress() != -1) {
		  System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		}
	}
	
	
	@Test
	public void describeForms(){
		//Use getJob(jid) for describing job.
		//While printing connection forms, pass connector id to getResourceBundle(cid).
		describe(client.getConnection(2).getConnectorPart().getForms(), client.getResourceBundle(1));
		System.out.println("---------------------------------------------------------------");
		describe(client.getConnection(1).getFrameworkPart().getForms(), client.getFrameworkResourceBundle());
	}
	
	private void describe(List<MForm> forms, ResourceBundle resource) {
	  for (MForm mf : forms) {
	    System.out.println(resource.getString(mf.getLabelKey())+":");
	    List<MInput<?>> mis = mf.getInputs();
	    for (MInput<?> mi : mis) {
	      System.out.println(resource.getString(mi.getLabelKey()) + " : " + mi.getValue());
	    }
	    System.out.println();
	  }
	}
	
	private void printMessage(List<MForm> forms) {
		System.out.println(forms.size());
		for (MForm form : forms) {
			List<MInput<?>> inputlist = form.getInputs();
			if (form.getValidationMessage() != null) {
				System.out.println("Form message: "
						+ form.getValidationMessage());
			}
			for (MInput<?> minput : inputlist) {
				if (minput.getValidationStatus() == Status.ACCEPTABLE) {
					System.out.println("Warning:"
							+ minput.getValidationMessage());
				} else if (minput.getValidationStatus() == Status.UNACCEPTABLE) {
					System.out
							.println("Error:" + minput.getValidationMessage());
				} else if (minput.getValidationStatus() == Status.FINE) {
					System.out.println("FINE:" + minput.getValidationMessage());
				}
			}
		}
	}

	@After
	public void end(){
		client.clearCache();
	}
	
}