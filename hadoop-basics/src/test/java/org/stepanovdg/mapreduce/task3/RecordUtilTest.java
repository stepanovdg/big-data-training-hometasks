package org.stepanovdg.mapreduce.task3;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by Dmitriy Stepanov on 19.02.18.
 */
public class RecordUtilTest extends AbstractBenchmark {

  public static final String IMPRESSION =
    "2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT"
      + " 5.1; SV1)\t113.117.187.*\t216\t234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae"
      + "\tnull\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t10057,13800,13496,10079,10076,10075,"
      + "10093,10129,10024,10006,10110,13776,10146,10120,10115,10063";
  private static final int ROUNDS = 3000000;
  private static final int ROUND_WARM = 10;
  private static final String EXPECTED = "234 277";

  @Before
  public void setUp() {
    //System.out.println(IMPRESSION);
  }

  @Test
  @BenchmarkOptions( benchmarkRounds = ROUNDS, warmupRounds = ROUND_WARM, concurrency = 0 )
  public void parseImpressionLinePattern() {
    assertEquals( EXPECTED, RecordUtil.parseImpressionLinePattern( IMPRESSION ) );
  }

  @Test
  @BenchmarkOptions( benchmarkRounds = ROUNDS, warmupRounds = ROUND_WARM, concurrency = 0 )
  public void parseImpressionLinePatternName() {
    assertEquals( EXPECTED, RecordUtil.parseImpressionLinePatternName( IMPRESSION ) );
  }

  @Test
  @BenchmarkOptions( benchmarkRounds = ROUNDS, warmupRounds = ROUND_WARM, concurrency = 0 )
  public void parseImpressionSplit() {
    assertEquals( EXPECTED, RecordUtil.parseImpressionSplit( IMPRESSION ) );
  }

  @Test
  @BenchmarkOptions( benchmarkRounds = ROUNDS, warmupRounds = ROUND_WARM, concurrency = 0 )
  public void parseImpressionLineIndexOf() {
    assertEquals( EXPECTED, RecordUtil.parseImpressionLineIndexOf( IMPRESSION ) );
  }

  @Test
  @BenchmarkOptions( benchmarkRounds = ROUNDS, warmupRounds = ROUND_WARM, concurrency = 0 )
  public void parseImpressionLineTokenaizer() {
    assertEquals( EXPECTED, RecordUtil.parseImpressionLineTokenaizer( IMPRESSION ) );
  }

}