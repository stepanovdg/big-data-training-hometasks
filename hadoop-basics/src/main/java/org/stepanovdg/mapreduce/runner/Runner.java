package org.stepanovdg.mapreduce.runner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.stepanovdg.mapreduce.task1.LongestCombinerComplex;
import org.stepanovdg.mapreduce.task1.LongestMapper;
import org.stepanovdg.mapreduce.task1.LongestMapperComplex;
import org.stepanovdg.mapreduce.task1.LongestReducer;
import org.stepanovdg.mapreduce.task1.LongestReducerComplex;
import org.stepanovdg.mapreduce.task1.writable.ComplexIntTextWritable;
import org.stepanovdg.mapreduce.task1.writable.DescendingIntWritable;

import java.util.ResourceBundle;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class Runner extends Configured implements Tool {

  private static final String JOB_NAME = "longest_word";
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

    if ( args.length != 0 && args.length != 3 ) {
      return 2;
    }

    String inputDir = "/wordcount/input";
    String outputDir = "/wordcount/output";

    FileSystem fs = FileSystem.get( conf );
    Mode m = Mode.LONGEST_WORD_v1;
    Path home = fs.getHomeDirectory();
    Path inputPath;
    Path outputPath;
    if ( args.length == 3 ) {
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
      inputPath = new Path( inputDir );
      outputPath = new Path( outputDir );
    } else {
      inputPath = new Path( home, inputDir );
      outputPath = new Path( home, outputDir );
    }

    if ( !fs.exists( inputPath ) ) {
      return 3;
    }

    if ( fs.exists( outputPath ) ) {
      fs.delete( outputPath, true );
    }

    Job job = Job.getInstance( conf, JOB_NAME );
    job.setJarByClass( getClass() );

    FileInputFormat.addInputPath( job, inputPath );
    FileOutputFormat.setOutputPath( job, outputPath );
    switch ( m ) {
      case LONGEST_WORD_v1:
        job.setMapperClass( LongestMapper.class );
        job.setCombinerClass( LongestReducer.class );
        job.setReducerClass( LongestReducer.class );

        job.setMapOutputKeyClass( DescendingIntWritable.class );
        job.setOutputKeyClass( IntWritable.class );
        job.setOutputValueClass( Text.class );
        break;
      case LONGEST_WORD_v2:
        job.setMapperClass( LongestMapperComplex.class );
        job.setCombinerClass( LongestCombinerComplex.class );
        job.setReducerClass( LongestReducerComplex.class );

        job.setMapOutputKeyClass( ComplexIntTextWritable.class );
        job.setOutputKeyClass( DescendingIntWritable.class );
        job.setOutputValueClass( Text.class );
        break;
    }

    job.setNumReduceTasks( 1 );

    return job.waitForCompletion( true ) ? 0 : 1;
  }
}
