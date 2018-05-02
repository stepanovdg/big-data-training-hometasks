package bdcc.kafka;

import bdcc.htm.MonitoringRecord;
import bdcc.utils.GlobalConstants;
import bdcc.utils.PropertiesLoader;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class TopicGenerator implements GlobalConstants {

  public static final AtomicInteger CALLBACKS = new AtomicInteger( 0 );
  private static final Logger LOGGER = LoggerFactory.getLogger( TopicGenerator.class );

  public static void main( String[] args ) throws InterruptedException {
    // load a properties file from class path, inside static method
    Properties applicationProperties = PropertiesLoader.getGlobalProperties();
    if ( !applicationProperties.isEmpty() ) {
      final boolean skipHeader = Boolean
        .parseBoolean( applicationProperties.getProperty( GENERATOR_SKIP_HEADER_CONFIG ) );
      //TODO understand real need of batch size and sleep - in case we have separate config for linger and etc
      final long batchSleep = Long.parseLong( applicationProperties.getProperty( GENERATOR_BATCH_SLEEP_CONFIG ) );
      final int batchSize = Integer.parseInt( applicationProperties.getProperty( BATCH_SIZE_CONFIG ) );

      final String sampleFile;
      if ( args[ 0 ] == null ) {
        sampleFile = applicationProperties.getProperty( GENERATOR_SAMPLE_FILE_CONFIG );
      } else {
        sampleFile = args[ 0 ];
      }
      final String topicName = applicationProperties.getProperty( KAFKA_RAW_TOPIC_CONFIG );
      final boolean sync = Boolean.parseBoolean( applicationProperties.getProperty( GENERATOR_SYNC_CONFIG ) );

      Reader reader = null;
      try {
        reader = Files.newBufferedReader( Paths.get( sampleFile ) );
      } catch ( IOException e ) {
        e.printStackTrace();
      }
      Iterable<MonitoringRecord> records = getMonitoringRecords( skipHeader, reader );

      KafkaProducer producer = KafkaHelper.createProducer();
      Runtime.getRuntime().addShutdownHook( new Thread( producer::close ) );

      Iterator<MonitoringRecord> iterator = records.iterator();
      AtomicInteger i = new AtomicInteger( 0 );
      AtomicInteger full = new AtomicInteger( 0 );
      while ( iterator.hasNext() ) {
        ProducerRecord<String, MonitoringRecord> producerRecord =
          getStringMonitoringRecordProducerRecord( topicName, iterator );


        if ( sync ) {
          syncLoad( producer, producerRecord );
          if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "sync=" + true + " full=" + full.incrementAndGet() );
          }
        } else {
          for ( i.set( 0 ); i.get() < batchSize; i.incrementAndGet() ) {
            producer.send( producerRecord, getCallbackForRecord( producer, producerRecord, i ) );
            if ( iterator.hasNext() ) {
              producerRecord = getStringMonitoringRecordProducerRecord( topicName, iterator );
            } else {
              break;
            }
          }
          Thread.sleep( batchSleep );
          if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "batch sleep sync=" + false + " i=" + i.get() + "full=" + full.incrementAndGet() );
          }
          //Write to log as batch send for debug
        }
      }
      // producer.flush();

      while ( CALLBACKS.get() != 0 ) {
        if ( LOGGER.isTraceEnabled() ) {
          LOGGER.trace( "Waiting for " + CALLBACKS.get() + " to finish" );
        }
      }

    }
  }

  protected static ProducerRecord<String, MonitoringRecord> getStringMonitoringRecordProducerRecord( String topicName,
                                                                                                     Iterator<MonitoringRecord> iterator ) {
    MonitoringRecord record = iterator.next();
    return new ProducerRecord<>( topicName, KafkaHelper.getKey( record ), record );
  }

  protected static Callback getCallbackForRecord( final KafkaProducer producer,
                                                  final ProducerRecord<String, MonitoringRecord> producerRecord,
                                                  AtomicInteger i ) {
    CALLBACKS.incrementAndGet();
    return new Callback() {
      @Override public void onCompletion( RecordMetadata metadata, Exception exception ) {
        if ( exception != null ) {
          i.incrementAndGet();
          producer.send( producerRecord, getCallbackForRecord( producer, producerRecord, i ) );

        }
        CALLBACKS.decrementAndGet();
      }
    };
  }

  protected static void syncLoad( KafkaProducer producer, ProducerRecord<String, MonitoringRecord> producerRecord ) {
    // as The order of the entries for the device must be saved.
    // use synchronous load
    RecordMetadata metadata = null;
    do {
      try {
        Future<RecordMetadata> send = producer.send( producerRecord );
        metadata = send.get();

      } catch ( InterruptedException | ExecutionException e ) {
        e.printStackTrace();
      }
    } while ( metadata == null );
  }

  protected static Iterable<MonitoringRecord> getMonitoringRecords( boolean skipHeader, Reader reader ) {
    ///TODO check and replace to line read in case not suit in memory as found constructor String[]
    ColumnPositionMappingStrategy<MonitoringRecord> strategy = new ColumnPositionMappingStrategy();
    strategy.setType( MonitoringRecord.class );
    Field[] fields = MonitoringRecord.class.getDeclaredFields();
    List<String> memberFieldsToBindTo = new ArrayList<String>();
    for ( Field f : fields ) {
      if ( f.getType().equals( String.class ) ) {
        memberFieldsToBindTo.add( f.getName() );
      }
    }
    strategy.setColumnMapping( memberFieldsToBindTo.toArray( new String[ memberFieldsToBindTo.size() ] ) );

    CsvToBeanBuilder csvReaderBuilder = new CsvToBeanBuilder<MonitoringRecord>( reader )
      .withMappingStrategy( strategy )
      .withIgnoreLeadingWhiteSpace( true );
    CsvToBean csvReader;
    if ( skipHeader ) {
      csvReader = csvReaderBuilder.withSkipLines( 1 ).build();
    } else {
      csvReader = csvReaderBuilder.build();
    }
    return (List<MonitoringRecord>) csvReader.parse();
  }
}
