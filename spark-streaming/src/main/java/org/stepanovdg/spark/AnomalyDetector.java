package org.stepanovdg.spark;

import bdcc.htm.HTMNetwork;
import bdcc.htm.MonitoringRecord;
import bdcc.htm.ResultState;
import bdcc.kafka.KafkaHelper;
import bdcc.utils.GlobalConstants;
import bdcc.utils.PropertiesLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.stepanovdg.spark.kafka.PartitionContextImpl;
import scala.Tuple2;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class AnomalyDetector implements GlobalConstants {

  private static final Map<String, HTMNetwork> map = new ConcurrentHashMap<>();
  private static Function3<String, Optional<MonitoringRecord>, State<HTMNetwork>, MonitoringRecord> mappingFunc =
    ( deviceID, recordOpt, state ) -> {
      // case 0: timeout
      if ( !recordOpt.isPresent() ) {
        return null;
      }

      // either new or existing device
      if ( !state.exists() ) {
        state.update( new HTMNetwork( deviceID ) );
      }
      HTMNetwork htmNetwork = state.get();
      String stateDeviceID = htmNetwork.getId();
      if ( !stateDeviceID.equals( deviceID ) ) {
        throw new Exception(
          "Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s" );
      }
      MonitoringRecord record = recordOpt.get();
      return getMonitoringRecordEnriched( htmNetwork, record );
    };

  public static void main( String[] args ) throws Exception {
    //load a properties file from class path, inside static method
    final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
    if ( args.length == 1 && args[ 0 ] != null && !args[ 0 ].isEmpty() ) {
      applicationProperties.load( new FileReader( args[ 0 ] ) );
    }
    if ( !applicationProperties.isEmpty() ) {
      final Boolean use_state = Boolean.parseBoolean( applicationProperties.getProperty( SPARK_STATE_USE_CONFIG ) );
      final String master = applicationProperties.getProperty( SPARK_MASTER_CONFIG );
      final String appName = applicationProperties.getProperty( SPARK_APP_NAME_CONFIG );
      final String rawTopicName = applicationProperties.getProperty( KAFKA_RAW_TOPIC_CONFIG );
      final String enrichedTopicName = applicationProperties.getProperty( KAFKA_ENRICHED_TOPIC_CONFIG );
      final String checkpointDir = applicationProperties.getProperty( SPARK_CHECKPOINT_DIR_CONFIG );
      final Duration batchDuration =
        Duration.apply( Long.parseLong( applicationProperties.getProperty( SPARK_BATCH_DURATION_CONFIG ) ) );
      final Duration windowDuration =
        Duration.apply( Long.parseLong( applicationProperties.getProperty( SPARK_WINDOW_DURATION_CONFIG ) ) );
      final Duration checkpointInterval =
        Duration
          .apply( Long.parseLong( applicationProperties.getProperty( SPARK_CHECKPOINT_INTERVAL_CONFIG, "60000" ) ) );

      JavaStreamingContext jssc = JavaStreamingContext.getOrCreate( checkpointDir, () -> {
        JavaStreamingContext jsscL =

          //Set only executor env (((
          //new JavaStreamingContext( master, appName, batchDuration, null, null, getEnvForSparkContext() );
          new JavaStreamingContext( createSparkConf( master, appName, getEnvForSparkContext() ), batchDuration );

        JavaDStream<ConsumerRecord<String, MonitoringRecord>> stream =
          KafkaUtils.createDirectStream( jsscL, LocationStrategies.PreferConsistent(),
            KafkaHelper.createConsumerStrategy( rawTopicName ) );

        if ( use_state ) {
          PairFunction<ConsumerRecord<String, MonitoringRecord>, String, MonitoringRecord> pairFunction =
            ( r ) -> new Tuple2<>( r.key(), r.value() );
          JavaPairDStream<String, MonitoringRecord> pairs = stream.mapToPair( pairFunction );
          pairs.checkpoint( checkpointInterval );

          JavaMapWithStateDStream<String, MonitoringRecord, HTMNetwork, MonitoringRecord>
            enriched = pairs.mapWithState( StateSpec.function( mappingFunc ) );

          //      enriched.dstream().window( windowDuration ).foreachRDD( AnomalyDetector::forOutPartition );
          enriched.foreachRDD( rdd -> rdd.foreachPartitionAsync( iter -> {
            try ( PartitionContextImpl context = PartitionContextImpl.getPartitionContext() ) {
              while ( iter.hasNext() ) {
                MonitoringRecord record = iter.next();
                if ( record == null ) {
                  return;
                }
                ProducerRecord<String, MonitoringRecord> producerRecord =
                  new ProducerRecord<>( enrichedTopicName, KafkaHelper.getKey( record ), record );
                KafkaProducer<String, MonitoringRecord> kafkaProducer = context.getKafkaProducer();
                kafkaProducer.send( producerRecord, getCallbackForRecord( kafkaProducer, producerRecord ) );
              }
            }
          } ) );
        } else {
          stream.checkpoint( checkpointInterval );
          stream.foreachRDD( rdd -> rdd.foreachPartitionAsync( iter -> {
            try ( PartitionContextImpl context = PartitionContextImpl.getPartitionContext() ) {
              while ( iter.hasNext() ) {
                ConsumerRecord<String, MonitoringRecord> record = iter.next();
                if ( record.value() == null ) {
                  return;
                }

                MonitoringRecord enriche = process( record.key(), record.value() );
                KafkaProducer<String, MonitoringRecord> kafkaProducer = context.getKafkaProducer();
                ProducerRecord<String, MonitoringRecord> producerRecord =
                  new ProducerRecord<>( enrichedTopicName, record.key(), enriche );
                kafkaProducer.send( producerRecord, getCallbackForRecord( kafkaProducer, producerRecord ) );
              }
            }
          } ) );
        }
        jsscL.checkpoint( checkpointDir );
        return jsscL;
      } );
      Runtime.getRuntime().addShutdownHook( new Thread( jssc::close ) );

      jssc.start();
      jssc.awaitTermination();
    }
  }

  private static SparkConf createSparkConf( String master, String appName,
                                            HashMap<String, String> envForSparkContext ) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.setMaster( master );
    sparkConf.setAppName( appName );
    envForSparkContext.forEach( sparkConf::set );

    return sparkConf;
  }

  private static MonitoringRecord process( String key, MonitoringRecord value ) {
    HTMNetwork htmNetwork = map.get( key );
    // todo add sync on creation or not lazy
    if ( htmNetwork == null ) {
      htmNetwork = new HTMNetwork( key );
    }
    String stateDeviceID = htmNetwork.getId();
    if ( !stateDeviceID.equals( key ) ) {
      throw new RuntimeException(
        "Wrong behaviour of Spark: stream key is $deviceID%s, while the actual state key is $stateDeviceID%s" );
    }
    return getMonitoringRecordEnriched( htmNetwork, value );
  }

  protected static Callback getCallbackForRecord( final KafkaProducer producer,
                                                  final ProducerRecord<String, MonitoringRecord> producerRecord ) {
    return ( metadata, exception ) -> {
      if ( exception != null ) {
        producer.send( producerRecord, getCallbackForRecord( producer, producerRecord ) );
      }
    };
  }

  private static HashMap<String, String> getEnvForSparkContext() {
    HashMap<String, String> stringStringHashMap = new HashMap<>();
    stringStringHashMap.put( "spark.streaming.backpressure.enabled", "true" );
    stringStringHashMap.put( SPARK_INTERNAL_SERIALIZER_CONFIG, "org.apache.spark.serializer.KryoSerializer" );
    //stringStringHashMap.put( SPARK_INTERNAL_SERIALIZER_CONFIG, "org.apache.spark.serializer.JavaSerializer" );
    stringStringHashMap.put( SPARK_KRYO_REGISTRATOR_CONFIG, "bdcc.serde.SparkKryoHTMRegistrator" );
    stringStringHashMap.put( SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG, "false" );
    return stringStringHashMap;
  }

  private static MonitoringRecord getMonitoringRecordEnriched( HTMNetwork htmNetwork, MonitoringRecord record ) {
    // get the value of DT and Measurement and pass it to the HTM
    HashMap<String, Object> m = new HashMap<>();
    m.put( "DT", DateTime
      .parse( record.getDateGMT() + " " + record.getTimeGMT(), DateTimeFormat.forPattern( "YY-MM-dd HH:mm" ) ) );
    m.put( "Measurement", Double.parseDouble( record.getSampleMeasurement() ) );
    ResultState rs = htmNetwork.compute( m );
    record.setPrediction( rs.getPrediction() );
    record.setError( rs.getError() );
    record.setAnomaly( rs.getAnomaly() );
    record.setPredictionNext( rs.getPredictionNext() );

    return record;
  }

  /*private static BoxedUnit toKafka( Iterator<MonitoringRecord> partitionOfRecords ) {
    KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createProducer();
    partitionOfRecords.foreach( ( record ) ->
      producer.send( new ProducerRecord<>( KafkaHelper.getKey( record ), record ) )
    );
    producer.close();
    return BoxedUnit.UNIT;
  }

  private static BoxedUnit forOutPartition( RDD<MonitoringRecord> rdd ) {
    rdd.foreachPartition(
      AnomalyDetector::toKafka
    );
    return BoxedUnit.UNIT;
  }*/
}