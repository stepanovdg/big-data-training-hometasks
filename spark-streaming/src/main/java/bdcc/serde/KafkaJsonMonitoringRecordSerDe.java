package bdcc.serde;

import bdcc.htm.MonitoringRecord;
import bdcc.utils.GlobalConstants;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

import static bdcc.utils.GlobalConstants.SPARK_SERDE_KEY;

public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger( KafkaJsonMonitoringRecordSerDe.class );
  private ObjectOutput out;
  private ByteArrayOutputStream bos;
  private ByteArrayInputStream bis;
  private ObjectInput in;
  private GlobalConstants.SerDeModes modeE = GlobalConstants.SerDeModes.JSON;
  private Kryo kryo;
  private Output output;
  private Input input;
  private ObjectMapper objectMapper = new ObjectMapper();

  private static void closeCloseable( AutoCloseable out ) {
    if ( out != null ) {
      try {
        out.close();
      } catch ( Exception e ) {
        //ignore
      }
    }
  }

  @Override
  public void configure( Map<String, ?> configs, boolean isKey ) {
    //Is key would be false as MonitoringRecord is always value in our case
    Object mode = configs.get( SPARK_SERDE_KEY );
    if ( mode != null && mode instanceof String ) {
      try {
        modeE = GlobalConstants.SerDeModes.valueOf( (String) mode );
      } catch ( IllegalArgumentException e ) {
        modeE = GlobalConstants.SerDeModes.JSON;
      }
    }
    switch ( modeE ) {
      case KRYO: {
        kryo = new Kryo();
        kryo.register( MonitoringRecord.class );
      }
    }
  }

  @Override
  public byte[] serialize( String topic, MonitoringRecord data ) {
    switch ( modeE ) {
      default:
      case JAVA: {
        bos = new ByteArrayOutputStream( 512 );
        try {
          out = new ObjectOutputStream( bos );
          out.writeObject( data );
          out.flush();
          return bos.toByteArray();
        } catch ( IOException e ) {
          LOGGER.error( "Failed to serialize monitoring record " + data, e );
          throw new SerializationException( e );
        }
      }
      case KRYO: {
        bos = new ByteArrayOutputStream( 512 );
        output = new Output( bos );
        kryo.writeClassAndObject( output, data );

        output.flush();
        return bos.toByteArray();
      }
      case JSON: {
        try {
          return objectMapper.writeValueAsBytes( data );
        } catch ( JsonProcessingException e ) {
          LOGGER.error( "Failed to serialize monitoring record " + data, e );
          throw new SerializationException( e );
        }
      }
      case WRITABLE: {
        //TODO is slow if required rewrite using hadoop writable way
        throw new RuntimeException( "Not implemented yet" );
      }
    }

  }

  @Override
  public MonitoringRecord deserialize( String topic, byte[] data ) {
    MonitoringRecord record = null;
    switch ( modeE ) {
      default:
      case JAVA: {
        bis = new ByteArrayInputStream( data );
        try {
          in = new ObjectInputStream( bis );
          record = (MonitoringRecord) in.readObject();
        } catch ( IOException e ) {
          LOGGER.error(
            "Failed to deserialize monitoring record topic=" + topic + " data=" + StringUtils.byteToHexString( data ),
            e );
        } catch ( ClassNotFoundException e ) {
          throw new SerializationException( "Class not found could not proceed", e );
        }
        return record;
      }
      case KRYO: {
        bis = new ByteArrayInputStream( data );
        input = new Input( bis );
        return (MonitoringRecord) kryo.readClassAndObject( input );
      }
      case JSON: {
        try {
          return objectMapper.readValue( data, MonitoringRecord.class );
        } catch ( IOException e ) {
          LOGGER.error(
            "Failed to deserialize monitoring record topic=" + topic + " data=" + StringUtils.byteToHexString( data ),
            e );
        }
      }
      case WRITABLE: {
        //TODO is slow if required rewrite using hadoop writable way
        throw new RuntimeException( "Not implemented yet" );
      }
    }
  }

  @Override
  public void close() {
    closeCloseable( out );
    closeCloseable( bos );
    closeCloseable( in );
    closeCloseable( bis );
    closeCloseable( input );
    closeCloseable( output );
  }
}