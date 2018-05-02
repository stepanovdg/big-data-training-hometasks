package bdcc.kafka;

import bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;

import java.util.Arrays;
import java.util.Collection;

import static bdcc.utils.PropertiesLoader.*;

public class KafkaHelper {

  public static KafkaProducer<String, MonitoringRecord> createProducer() {
    return new KafkaProducer<>( getKafkaProducerProperties() );
  }

  public static KafkaProducer<String, MonitoringRecord> createAnomallyProducer() {
    return new KafkaProducer<>( getKafkaAnomalyProducerProperties() );
  }

  public static ConsumerStrategy<String, MonitoringRecord> createConsumerStrategy( String topics ) {
    Collection<String> topicsList = Arrays.asList( topics.split( "," ) );
    return ConsumerStrategies.Subscribe( topicsList, getKafkaConsumerProperties() );
  }

  public static String getKey( MonitoringRecord record ) {
    return record.getStateCode() + "-" + record.getCountyCode() + "-" + record.getSiteNum() + "-"
      + record.getParameterCode() + "-" + record.getPoc();
  }
}
