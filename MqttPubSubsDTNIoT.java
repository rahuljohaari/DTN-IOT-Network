import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import java.util.*;
import java.sql.*;
//Routing in IoT using MQTT Protocol-Publisher Scriber Approach
public class MqttPubSubsDTNIoT 
{
public static void main(String[] args) 
{
	long startTime = System.nanoTime();
    String topic        = "Weather Record";
    String content       =  args[0];
	String county_fips_code = args[1];
    String country = args[2];
    String time_initiated_utc = args[3];
    String forecast_date = args[4];
    String forecast_day_of_year = args[5];
    String min_temperature_air_2m_f = args[6];
    String avg_temperature_air_2m_f = args[7];
    String max_temperature_air_2m_f = args[8];
    String min_humidity_relative_2m_pct = args[9];
    String avg_humidity_relative_2m_pct = args[10];
    String max_humidity_relative_2m_pct = args[11];
    String min_humidity_specific_2m_gpkg = args[12];
    String avg_humidity_specific_2m_gpkg = args[13];
    String max_humidity_specific_2m_gpkg = args[14];
    int qos0              =  0;
	int qos1              =  1;
	int qos2              =  2;
    String broker       = "tcp://localhost:1882";
    //String SubscriberID   = "127.0.0.1";
    MemoryPersistence persistence = new MemoryPersistence();
        try {
	    String SubscriberID=MqttClient.generateClientId();//AutoGeneration of Subscriber
		System.out.println("Generated SubscriberID is"+ SubscriberID);
        MqttClient Subscriber = new MqttClient(broker, SubscriberID, persistence);
        
		String PublisherID=MqttClient.generateClientId();//AutoGeneration of Publisher
        System.out.println("Generated Publisher Id is"+ PublisherID);
        MqttClient Publisher = new MqttClient(broker, PublisherID, persistence);

        System.out.println("Choose one of the type of POSOP Contact");
		System.out.println("1.Opportunistic Contact");
		System.out.println("2.Scheduled Contact");
		System.out.println("3.OnDemand Contact");
		Scanner sc= new Scanner(System.in); 
		System.out.print("Please enter your choice (1-3): ");
		String str1=sc.next();
		int num2= Integer.parseInt(str1);
        switch (num2) 
		{
        case 1:
		try{
		System.out.println("Opportunistic Contact Selection");
		MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setConnectionTimeout(60);
	    connOpts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);
        connOpts.setCleanSession(true);
        //Subscriber connects to broker
        System.out.println("Connecting to broker: "+broker);
        Subscriber.connect(connOpts);//Connection
        Subscriber.subscribe(topic);//Subscription
        System.out.println("Sub Connected");

        //Publisher connects to broker
        Publisher.connect(connOpts);
        MqttMessage message = new MqttMessage(content.getBytes());
        message.setQos(qos0);
        Publisher.publish(topic, message);
        System.out.println("Message published");
        Publisher.disconnect();
        //Publisher publishes and exits
        System.out.println("Disconnected");
        
        //Subscriber receives the topic and message
        System.out.println(Subscriber.getTopic(topic));
        System.out.println("Received message is "+ Subscriber.getTopic(content)); 
      Class.forName("com.mysql.cj.jdbc.Driver");
    Connection connection=DriverManager.getConnection("jdbc:mysql://localhost:3306/world","root","toor"); 
    Statement statement = connection.createStatement();
	 int num1=statement.executeUpdate("insert into county_day_forecast values('"+county_fips_code+"','"+country+"', '"+time_initiated_utc+"','"+forecast_date+"','"+forecast_day_of_year+"','"+min_temperature_air_2m_f+"','"+avg_temperature_air_2m_f+"','"+max_temperature_air_2m_f+"','"+min_humidity_relative_2m_pct+"', '"+avg_humidity_relative_2m_pct+"','"+max_humidity_relative_2m_pct+"','"+min_humidity_specific_2m_gpkg+"','"+avg_humidity_specific_2m_gpkg+"','"+ max_humidity_specific_2m_gpkg+"','"+Publisher+"','"+broker+"', '"+Subscriber+"','"+topic+"','"+content+"','Opportunistic Contact')");
 	System.out.println("No of rows entered"+num1);
	connection.close();
	long endTime   = System.nanoTime();
		long totalTime = endTime - startTime;
		System.out.println("Total time in nanosecond consumed during programm execution is "+ totalTime);
   }catch(Exception ex)
   {
        System.out.println(ex);
       
    }
	break;
	case 2 :
		try
		{
		System.out.println("Scheduled Contact Selection");
		MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setConnectionTimeout(60);
	    connOpts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);
        connOpts.setCleanSession(true);
        //Subscriber connects to broker
        System.out.println("Connecting to broker: "+broker);
        Subscriber.connect(connOpts);//Connection
        Subscriber.subscribe(topic);//Subscription
        System.out.println("Sub Connected");

        //Publisher connects to broker
        Publisher.connect(connOpts);
        MqttMessage message = new MqttMessage(content.getBytes());
        message.setQos(qos1);
        Publisher.publish(topic, message);
        System.out.println("Message published");
        Publisher.disconnect();
        //Publisher publishes and exits
        System.out.println("Disconnected");
        
        //Subscriber receives the topic and message
        System.out.println(Subscriber.getTopic(topic));
        System.out.println("Received message is "+ Subscriber.getTopic(content)); 
      Class.forName("com.mysql.cj.jdbc.Driver");
    Connection connection=DriverManager.getConnection("jdbc:mysql://localhost:3306/world","root","toor"); 
    Statement statement = connection.createStatement();
    int num1=statement.executeUpdate("insert into county_day_forecast values('"+county_fips_code+"','"+country+"', '"+time_initiated_utc+"','"+forecast_date+"','"+forecast_day_of_year+"','"+min_temperature_air_2m_f+"','"+avg_temperature_air_2m_f+"','"+max_temperature_air_2m_f+"','"+min_humidity_relative_2m_pct+"', '"+avg_humidity_relative_2m_pct+"','"+max_humidity_relative_2m_pct+"','"+min_humidity_specific_2m_gpkg+"','"+avg_humidity_specific_2m_gpkg+"','"+ max_humidity_specific_2m_gpkg+"','"+Publisher+"','"+broker+"', '"+Subscriber+"','"+topic+"','"+content+"','Scheduled Contact')");
	System.out.println("No of rows entered"+num1);
	connection.close();
	long endTime   = System.nanoTime();
		long totalTime = endTime - startTime;
		System.out.println("Total time in nanosecond consumed during programm execution is "+ totalTime);
  }catch(Exception ex)
   {
        System.out.println(ex);
       
    }
	break;
	case 3 :
		try{	
        System.out.println("On-Demand Contact Selection");		
	    MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setConnectionTimeout(60);
	    connOpts.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);
        connOpts.setCleanSession(true);
        //Subscriber connects to broker
        System.out.println("Connecting to broker: "+broker);
        Subscriber.connect(connOpts);//Connection
        Subscriber.subscribe(topic);//Subscription
        System.out.println("Sub Connected");

        //Publisher connects to broker
        Publisher.connect(connOpts);
        MqttMessage message = new MqttMessage(content.getBytes());
        message.setQos(qos2);
        Publisher.publish(topic, message);
        System.out.println("Message published");
        Publisher.disconnect();
        //Publisher publishes and exits
        System.out.println("Disconnected");
        
        //Subscriber receives the topic and message
        System.out.println(Subscriber.getTopic(topic));
        System.out.println("Received message is "+ Subscriber.getTopic(content)); 
      Class.forName("com.mysql.cj.jdbc.Driver");
    Connection connection=DriverManager.getConnection("jdbc:mysql://localhost:3306/world","root","toor"); 
    Statement statement = connection.createStatement();
   int num1=statement.executeUpdate("insert into county_day_forecast values('"+county_fips_code+"','"+country+"', '"+time_initiated_utc+"','"+forecast_date+"','"+forecast_day_of_year+"','"+min_temperature_air_2m_f+"','"+avg_temperature_air_2m_f+"','"+max_temperature_air_2m_f+"','"+min_humidity_relative_2m_pct+"', '"+avg_humidity_relative_2m_pct+"','"+max_humidity_relative_2m_pct+"','"+min_humidity_specific_2m_gpkg+"','"+avg_humidity_specific_2m_gpkg+"','"+ max_humidity_specific_2m_gpkg+"','"+Publisher+"','"+broker+"', '"+Subscriber+"','"+topic+"','"+content+"','OnDemand Contact')");
  System.out.println("No of rows entered"+num1);
	connection.close();
	long endTime   = System.nanoTime();
		long totalTime = endTime - startTime;
		System.out.println("Total time in nanosecond consumed during programm execution is "+ totalTime);
  }catch(Exception ex)
   {
        System.out.println(ex);
       
    }
	break;
	default :
	System.out.println("Invalid Contact Selection");
	break;
		}
	}catch(Exception ex)
   {
        System.out.println(ex);
       
    }

}
}


/*create table MQTT
(
PublisherId char(50) not null,
BrokerID char(50) not null,
SubsriberId char(50) not null,
Topic char(50) not null,
Content char(50) not null)
Contact char(50) not null*/

/*
CREATE TABLE county_day_forecast(
    county_fips_code varchar(255),
    country varchar(255),
    time_initiated_utc varchar(255),
    forecast_date varchar(255),
   forecast_day_of_year varchar(255),
 min_temperature_air_2m_f varchar(25),
 avg_temperature_air_2m_f varchar(25),
 max_temperature_air_2m_f varchar(255),
 min_humidity_relative_2m_pct varchar(25),
 avg_humidity_relative_2m_pct varchar(25),
 max_humidity_relative_2m_pct varchar(255),
 min_humidity_specific_2m_gpkg varchar(25),
 avg_humidity_specific_2m_gpkg varchar(25),
 max_humidity_specific_2m_gpkg varchar(255),
PublisherId char(50) not null,
BrokerID char(50) not null,
SubsriberId char(50) not null,
Topic char(50) not null,
Content char(50) not null,
Contact char(50) not null
);
*/
/* Records
24003,US,2021-06-28 12:00:00 UTC,2021-07-01,182,70.9,77.6,91.0,48,78,97,14.7,15.5,17.0
24003,US,2021-06-28 12:00:00 UTC,2021-07-03,184,65.5,69.1,72.7,97,98,99,13.7,14.7,16.0
21215,US,2021-06-28 12:00:00 UTC,2021-07-05,186,60.4,69.0,78.5,67,85,98,11.3,13.1,14.8
21215,US,2021-06-28 12:00:00 UTC,2021-07-06,187,64.8,70.3,78.8,70,87,94,12.4,13.9,15.6
21215,US,2021-06-28 12:00:00 UTC,2021-07-13,194,67.0,72.6,78.3,87,93,97,13.5,16.2,18.4
URL :
//https://console.cloud.google.com/bigquery?project=predictive-net-180410&ws=!1m9!1m3!3m2!1sbigquery-public-data!2sghcn_d!1m4!4m3!1sbigquery-public-data!2scovid19_weathersource_com!3scounty_day_forecast
*/
//For execution using following records :-
//24003 US 2021-06-28 12:00:00 UTC 2021-07-01 182 70.9 77.6 91.0 48 78 97 14.7 15.5 17.0
//24003 US 2021-06-28 12:00:00 UTC 2021-07-03 184 65.5 69.1 72.7 97 98 99 13.7 14.7 16.0
//21215 US 2021-06-28 12:00:00 UTC 2021-07-05 186 60.4 69.0 78.5 67 85 98 11.3 13.1 14.8