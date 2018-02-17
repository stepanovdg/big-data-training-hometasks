package org.stepanovdg.mapreduce.runner;

import java.util.ResourceBundle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.stepanovdg.mapreduce.task1.LongestMapper;
import org.stepanovdg.mapreduce.task1.LongestReducer;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class Runner extends Configured implements Tool {

  public static final String JOB_NAME = "longest_word";
  private static ResourceBundle bundle;

  public static void main(String[] args) throws Exception {
    printHelp();
    int res = ToolRunner.run(new Configuration(), new Runner(), args);
    System.exit(res);
  }

  private static void printHelp() {
    bundle = ResourceBundle.getBundle("runner");
    System.out.println(bundle.getString("usage"));
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    if (args.length != 0 && args.length != 2) {
      return 2;
    }

    String inputDir = "/wordcount/input";
    String outputDir = "/wordcount/output";

    FileSystem fs = FileSystem.get(conf);
    Path home = fs.getHomeDirectory();
    Path inputPath;
    Path outputPath;
    if (args.length == 2) {
      inputDir = args[0];
      outputDir = args[1];
      inputPath = new Path(inputDir);
      outputPath = new Path(outputDir);
    } else {
      inputPath = new Path(home, inputDir);
      outputPath = new Path(home, outputDir);
    }

    if (!fs.exists(inputPath)) {
      return 3;
    }

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    Job job = Job.getInstance(conf, JOB_NAME);
    job.setJarByClass(getClass());

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setMapperClass(LongestMapper.class);
    job.setCombinerClass(LongestReducer.class);
    job.setReducerClass(LongestReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
