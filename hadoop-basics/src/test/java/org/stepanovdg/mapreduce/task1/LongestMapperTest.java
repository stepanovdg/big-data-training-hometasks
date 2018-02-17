package org.stepanovdg.mapreduce.task1;

import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;

/**
 * Created by Dmitriy Stepanov on 16.02.18.
 */
public class LongestMapperTest {

  @Before
  public void setUp() throws Exception {
    LongestMapper mapper = new LongestMapper();
    new MapDriver<LongWritable,Text,IntWritable,Text>();
  }

  @After
  public void tearDown() throws Exception {
  }
}