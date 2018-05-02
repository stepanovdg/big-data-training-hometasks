package bdcc.serde;

import bdcc.htm.MonitoringRecord;
import bdcc.utils.GlobalConstants;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static bdcc.utils.GlobalConstants.SPARK_SERDE_KEY;
import static org.junit.Assert.assertEquals;

/**
 * Created by Dmitriy Stepanov on 02.05.18.
 */
public class KafkaJsonMonitoringRecordSerDeTest {

  private String line =
    "10,001,0002,44201,1,38.986672,-75.5568,WGS84,Ozone,2014-01-01,00:00,2014-01-01,05:00,0.016,Parts per"
      + " million,0.005,,,FEM,047,INSTRUMENTAL - ULTRA VIOLET,Delaware,Kent,2014-02-12";

  private KafkaJsonMonitoringRecordSerDe kafkaJsonMonitoringRecordSerDe;
  private HashMap<String, String> kvHashMap = new HashMap<>();
  private MonitoringRecord record = new MonitoringRecord( line.split( "," ) );

  @Before
  public void setUp() {
    kafkaJsonMonitoringRecordSerDe = new KafkaJsonMonitoringRecordSerDe();
  }

  @Test
  public void testJsonSerde() {
    kvHashMap.put( SPARK_SERDE_KEY, GlobalConstants.SerDeModes.JSON.toString() );
    testSerDe();
  }

  @Test
  public void testKryoSerde() {
    kvHashMap.put( SPARK_SERDE_KEY, GlobalConstants.SerDeModes.KRYO.toString() );
    testSerDe();
  }

  @Test
  public void testJavaSerde() {
    kvHashMap.put( SPARK_SERDE_KEY, GlobalConstants.SerDeModes.JAVA.toString() );
    testSerDe();
  }

  protected void testSerDe() {
    kafkaJsonMonitoringRecordSerDe.configure( kvHashMap, true );

    String topic = "";
    byte[] bytes = kafkaJsonMonitoringRecordSerDe.serialize( topic, record );
    MonitoringRecord rec = kafkaJsonMonitoringRecordSerDe.deserialize( topic, bytes );

    assertEquals( record.toString(), rec.toString() );
  }
}