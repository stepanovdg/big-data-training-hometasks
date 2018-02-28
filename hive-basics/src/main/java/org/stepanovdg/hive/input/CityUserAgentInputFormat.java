package org.stepanovdg.hive.input;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Reporter;
import org.stepanovdg.mapreduce.task3.writable.IntTextPairWritable;

import java.io.IOException;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 * <p>
 * Key - CityId
 * Value - BID price
 */
public class CityUserAgentInputFormat extends FileInputFormat<LongWritable, IntTextPairWritable> implements
  InputFormat<LongWritable, IntTextPairWritable>, JobConfigurable {

  private CompressionCodecFactory compressionCodecs = null;

  public void configure( JobConf conf ) {
    compressionCodecs = new CompressionCodecFactory( conf );
  }

  protected boolean isSplitable( FileSystem fs, Path file ) {
    final CompressionCodec codec = compressionCodecs.getCodec( file );
    if ( null == codec ) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  @Override public org.apache.hadoop.mapred.RecordReader<LongWritable, IntTextPairWritable> getRecordReader(
    org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter ) throws IOException {
    reporter.setStatus( split.toString() );
    return new ImpressionRecordReader( job, split );
  }
}
