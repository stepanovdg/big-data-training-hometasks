package bdcc.kafka;

import bdcc.htm.MonitoringRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class MonitoringRecordPartitioner extends DefaultPartitioner {
  private static final Logger LOGGER = LoggerFactory.getLogger( MonitoringRecordPartitioner.class );

  private final AtomicInteger counter = new AtomicInteger( new Random().nextInt() );

  /**
   * A cheap way to deterministically convert a number to a positive value. When the input is
   * positive, the original value is returned. When the input number is negative, the returned
   * positive value is the original value bit AND against 0x7fffffff which is not its absolutely
   * value.
   * <p>
   * Note: changing this method in the future will possibly cause partition selection not to be
   * compatible with the existing messages already placed on a partition.
   *
   * @param number a given number
   * @return a positive number.
   */
  private static int toPositive( int number ) {
    return number & 0x7fffffff;
  }

  public int partition( String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster ) {
    if ( value instanceof MonitoringRecord ) {
      List<PartitionInfo> partitions = cluster.partitionsForTopic( topic );
      int numPartitions = partitions.size();
      if ( keyBytes == null ) {
        int nextValue = counter.getAndIncrement();
        List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic( topic );
        if ( availablePartitions.size() > 0 ) {
          int part = toPositive( nextValue ) % availablePartitions.size();
          return availablePartitions.get( part ).partition();
        } else {
          // no partitions are available, give a non-available partition
          return toPositive( nextValue ) % numPartitions;
        }
      } else {
        // hash the keyBytes to choose a partition
        return toPositive( Utils.murmur2( keyBytes ) ) % numPartitions;
      }
    } else {
      return super.partition( topic, key, keyBytes, value, valueBytes, cluster );
    }
  }

  public void close() {
  }

  public void configure( Map<String, ?> map ) {
  }
}