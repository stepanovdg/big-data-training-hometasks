package org.stepanovdg.mapreduce.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.stepanovdg.mapreduce.task1.LongestCombinerComplex;
import org.stepanovdg.mapreduce.task1.LongestMapper;
import org.stepanovdg.mapreduce.task1.LongestMapperComplex;
import org.stepanovdg.mapreduce.task1.LongestReducer;
import org.stepanovdg.mapreduce.task1.LongestReducerComplex;
import org.stepanovdg.mapreduce.task1.writable.ComplexIntTextWritable;
import org.stepanovdg.mapreduce.task1.writable.DescendingIntWritable;
import org.stepanovdg.mapreduce.task2.LogsCombiner;
import org.stepanovdg.mapreduce.task2.LogsCombinerCustom;
import org.stepanovdg.mapreduce.task2.LogsMapper;
import org.stepanovdg.mapreduce.task2.LogsMapperCustom;
import org.stepanovdg.mapreduce.task2.LogsReducer;
import org.stepanovdg.mapreduce.task2.LogsReducerCustom;
import org.stepanovdg.mapreduce.task2.writable.TotalAndAverageWritable;
import org.stepanovdg.mapreduce.task2.writable.TotalAndCountWritable;
import org.stepanovdg.mapreduce.task3.HighBidCombiner;
import org.stepanovdg.mapreduce.task3.HighBidMapper;
import org.stepanovdg.mapreduce.task3.HighBidReducer;
import org.stepanovdg.mapreduce.task3.input.HighBidInputFormat;

import java.net.URI;
import java.util.ResourceBundle;

import static org.stepanovdg.mapreduce.task2.Constants.OUT_SEPARATOR;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class Runner extends Configured implements Tool {

  private static final String JOB_NAME = "stepanovdg";
  public static final String WORDCOUNT_INPUT = "/wordcount/input";
  public static final String WORDCOUNT_OUTPUT = "/wordcount/output";
  public static final String CITY_DICTIONARY = "/wordcount/city";
  private static ResourceBundle bundle;

  public static void main( String[] args ) throws Exception {
    printHelp();
    int res = ToolRunner.run( new Configuration(), new Runner(), args );
    System.exit( res );
  }

  private static void printHelp() {
    bundle = ResourceBundle.getBundle( "runner" );
    System.out.println( String.format( bundle.getString( "usage" ), Mode.generateModeHelp() ) );
  }

  public int run( String[] args ) throws Exception {
    Configuration conf = getConf();

    if ( args.length != 0 && args.length < 3 || args.length > 5 ) {
      return 2;
    }

    String inputDir = WORDCOUNT_INPUT;
    String outputDir = WORDCOUNT_OUTPUT;

    FileSystem fs = FileSystem.get( conf );
    Mode m = Mode.LONGEST_WORD_v1;
    Path home = fs.getHomeDirectory();
    Path inputPath = null;
    Path outputPath;
    String cityFileOnHDFS_EN = CITY_DICTIONARY;
    String cityFileOnHDFS_CN = null;
    if ( args.length != 0 ) {
      try {
        int ordinalMode = Integer.parseInt( args[ 0 ] );
        if ( ordinalMode - 1 > Mode.values().length ) {
          return 4;
        }
        m = Mode.values()[ ordinalMode ];
      } catch ( IllegalArgumentException e ) {
        m = Mode.LONGEST_WORD_v1;
      }
      inputDir = args[ 1 ];
      outputDir = args[ 2 ];
      if ( !inputDir.contains( "," ) ) {
        inputPath = new Path( inputDir );
      }
      outputPath = new Path( outputDir );
      if ( args.length == 4 ) {
        cityFileOnHDFS_EN = args[ 3 ];
      }
      if ( args.length == 5 ) {
        cityFileOnHDFS_EN = args[ 3 ];
        cityFileOnHDFS_CN = args[ 4 ];
      }
    } else {
      inputPath = new Path( home, inputDir );
      outputPath = new Path( home, outputDir );
    }

    if ( inputPath != null && !fs.exists( inputPath ) ) {
      return 3;
    }

    if ( fs.exists( outputPath ) ) {
      fs.delete( outputPath, true );
    }

    Job job = Job.getInstance( conf, JOB_NAME );
    job.setJarByClass( getClass() );

    if ( inputPath != null ) {
      FileInputFormat.addInputPath( job, inputPath );
    } else {
      FileInputFormat.addInputPaths( job, inputDir );
    }
    FileOutputFormat.setOutputPath( job, outputPath );
    job.setJobName( JOB_NAME + m );
    boolean showCounters = false;
    switch ( m ) {
      case LONGEST_WORD_v1:
        job.setMapperClass( LongestMapper.class );
        job.setReducerClass( LongestReducer.class );

        job.setMapOutputKeyClass( DescendingIntWritable.class );
        job.setOutputKeyClass( IntWritable.class );
        job.setOutputValueClass( Text.class );
        job.setNumReduceTasks( 1 );
        break;
      case LONGEST_WORD_v2:
        job.setMapperClass( LongestMapperComplex.class );
        job.setCombinerClass( LongestCombinerComplex.class );
        job.setReducerClass( LongestReducerComplex.class );

        job.setMapOutputKeyClass( ComplexIntTextWritable.class );
        job.setMapOutputValueClass( NullWritable.class );
        job.setOutputKeyClass( DescendingIntWritable.class );
        job.setOutputValueClass( Text.class );
        job.setNumReduceTasks( 1 );
        break;
      case PARSE_LOGS_FORIP_v1:
        job.setMapperClass( LogsMapper.class );
        job.setCombinerClass( LogsCombiner.class );
        job.setReducerClass( LogsReducer.class );

        job.setOutputFormatClass( TextOutputFormat.class );
        job.getConfiguration().set( "mapred.textoutputformat.separator", OUT_SEPARATOR );

        job.setMapOutputKeyClass( IntWritable.class );
        job.setMapOutputValueClass( Text.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );

        showCounters = true;
        break;
      case PARSE_LOGS_FORIP_v2:

        job.setMapperClass( LogsMapperCustom.class );
        job.setCombinerClass( LogsCombinerCustom.class );
        job.setReducerClass( LogsReducerCustom.class );

        //SequenceFileAsBinaryOutputFormat.class
        job.setOutputFormatClass( SequenceFileOutputFormat.class );
       /* job.getConfiguration().set( "mapreduce.output.fileoutputformat.compress", "true" );
        job.getConfiguration()
          .set( "mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec" );
        job.getConfiguration().set( "mapreduce.output.fileoutputformat.compress.type", "BLOCK" );
*/
        FileOutputFormat.setCompressOutput( job, true );
        FileOutputFormat.setOutputCompressorClass( job, SnappyCodec.class );
        SequenceFileOutputFormat.setOutputCompressionType( job, SequenceFile.CompressionType.BLOCK );

        job.setMapOutputKeyClass( IntWritable.class );
        job.setMapOutputValueClass( TotalAndCountWritable.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( TotalAndAverageWritable.class );


        //SnappyCodec.checkNativeCodeLoaded();
        showCounters = true;
        break;
      case AMOUNT_HIGH_BID_v1:
        job.setMapperClass( HighBidMapper.class );
        job.setCombinerClass( HighBidCombiner.class );
        job.setReducerClass( HighBidReducer.class );

        job.setInputFormatClass( HighBidInputFormat.class );

        job.setMapOutputKeyClass( IntWritable.class );
        job.setMapOutputValueClass( LongWritable.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( LongWritable.class );

        job.addCacheFile(
          new URI( cityFileOnHDFS_EN + "#" + org.stepanovdg.mapreduce.task3.Constants.CITY_DICTIONARY_FILE_NAME_EN ) );
        if ( cityFileOnHDFS_CN != null ) {
          job.addCacheFile(
            new URI(
              cityFileOnHDFS_CN + "#" + org.stepanovdg.mapreduce.task3.Constants.CITY_DICTIONARY_FILE_NAME_CN ) );
        }

        break;
      case AMOUNT_HIGH_BID_v2:

        break;
    }


    int i = job.waitForCompletion( true ) ? 0 : 1;
    if ( showCounters ) {
      System.out.println( job.getCounters() );
    }
    return i;
  }

}
