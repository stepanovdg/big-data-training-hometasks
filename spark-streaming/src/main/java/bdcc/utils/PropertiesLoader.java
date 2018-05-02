package bdcc.utils;

import bdcc.kafka.MonitoringRecordPartitioner;
import bdcc.serde.KafkaJsonMonitoringRecordSerDe;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static bdcc.utils.GlobalConstants.PROPERTIES_FILE;
import static bdcc.utils.GlobalConstants.SPARK_SERDE_KEY;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class PropertiesLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger( PropertiesLoader.class );

  private static Properties globalProperties = new Properties();
  private static Properties kafkaProducerProperties = new Properties();
  private static Properties kafkaAnomallyProducerProperties = new Properties();
  private static Map<String, Object> kafkaConsumerProperties = new HashMap<>();

  static {
    try ( InputStream input = PropertiesLoader.class.getClassLoader().getResourceAsStream( PROPERTIES_FILE ) ) {
      globalProperties.load( input );
      LOGGER.info( String.valueOf( globalProperties ) );

      if ( !globalProperties.isEmpty() ) {
        kafkaProducerProperties
          .put( BOOTSTRAP_SERVERS_CONFIG, globalProperties.getProperty( BOOTSTRAP_SERVERS_CONFIG ) );
        kafkaProducerProperties.put( ACKS_CONFIG, globalProperties.getProperty( ACKS_CONFIG ) );
        kafkaProducerProperties.put( RETRIES_CONFIG, globalProperties.getProperty( RETRIES_CONFIG ) );
        kafkaProducerProperties.put( BATCH_SIZE_CONFIG, globalProperties.getProperty( BATCH_SIZE_CONFIG ) );
        kafkaProducerProperties.put( LINGER_MS_CONFIG, globalProperties.getProperty( LINGER_MS_CONFIG ) );
        kafkaProducerProperties.put( BUFFER_MEMORY_CONFIG, globalProperties.getProperty( BUFFER_MEMORY_CONFIG ) );
        kafkaProducerProperties.put( KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class );
        kafkaProducerProperties.put( SPARK_SERDE_KEY, GlobalConstants.SerDeModes.KRYO );
        kafkaProducerProperties.put( VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonMonitoringRecordSerDe.class );
        kafkaProducerProperties.put( PARTITIONER_CLASS_CONFIG, MonitoringRecordPartitioner.class );

        //IGNORED AS old kafka
        kafkaProducerProperties.put( "enable.idempotence", "true" );
        kafkaProducerProperties.put( "processing.guarantee", "exactly_once" );

        kafkaConsumerProperties
          .put( BOOTSTRAP_SERVERS_CONFIG, globalProperties.getProperty( BOOTSTRAP_SERVERS_CONFIG ) );
        kafkaConsumerProperties.put( GROUP_ID_CONFIG, globalProperties.getProperty( GROUP_ID_CONFIG ) );
        kafkaConsumerProperties
          .put( AUTO_OFFSET_RESET_CONFIG, globalProperties.getProperty( AUTO_OFFSET_RESET_CONFIG ) );
        kafkaConsumerProperties
          .put( ENABLE_AUTO_COMMIT_CONFIG, globalProperties.getProperty( ENABLE_AUTO_COMMIT_CONFIG ) );
        kafkaConsumerProperties.put( KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
        kafkaConsumerProperties.put( VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonMonitoringRecordSerDe.class );
        //ignored
        kafkaConsumerProperties.put( "isolation.level", "read_committed" );

        kafkaAnomallyProducerProperties = new Properties( kafkaProducerProperties );
        kafkaAnomallyProducerProperties.put( SPARK_SERDE_KEY, GlobalConstants.SerDeModes.JSON );
        kafkaAnomallyProducerProperties.put( VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonMonitoringRecordSerDe.class );
      }
    } catch ( IOException e ) {
      LOGGER.error( "Sorry, unable to read properties file", e );
    }
  }

  public static Properties getGlobalProperties() {
    return globalProperties;
  }

  public static Properties getKafkaProducerProperties() {
    return kafkaProducerProperties;
  }

  public static Properties getKafkaAnomalyProducerProperties() {
    return kafkaAnomallyProducerProperties;
  }

  public static Map<String, Object> getKafkaConsumerProperties() {
    return kafkaConsumerProperties;
  }
}
