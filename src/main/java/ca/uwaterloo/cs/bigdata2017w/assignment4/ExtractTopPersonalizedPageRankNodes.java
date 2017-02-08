
package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.pair.PairOfIntFloat;
import tl.lin.data.queue.TopScoredInts;

import java.io.IOException;
import java.util.*;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);
  private static final java.lang.String SOURCES = "sources";

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, IntWritable, PairOfIntFloat> {

    private List<TopScoredInts> queue = new ArrayList<>();
    private String[] sources;

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      sources = context.getConfiguration().getStrings(SOURCES);
      for (int i = 0; i < sources.length; i ++) {
        queue.add(new TopScoredInts(k));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      ArrayListOfFloatsWritable pageRanks = node.getPageRanks();
      for (int i = 0; i < sources.length; i ++) {
        queue.get(i).add(node.getNodeId(), (float) StrictMath.exp(pageRanks.get(i)));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();


      for (int i = 0; i < sources.length; i ++) {
        for (PairOfIntFloat pair : queue.get(i).extractAll()) {
          key.set(i);
          context.write(key, pair);
        }
      }
    }
  }

  private static class MyReducer extends
      Reducer<IntWritable, PairOfIntFloat, IntWritable, FloatWritable> {
    private static List<TopScoredInts> queue = new ArrayList<>();
    private String[] sources;

    @Override
    public void setup(Context context) throws IOException {
      int k = context.getConfiguration().getInt("n", 100);
      this.sources = context.getConfiguration().getStrings(SOURCES);
      for (int i = 0; i < sources.length; i ++) {
        queue.add(new TopScoredInts(k));
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PairOfIntFloat> iterable, Context context)
        throws IOException {
      Iterator<PairOfIntFloat> iter = iterable.iterator();

      while (iter.hasNext()) {
        PairOfIntFloat pair = iter.next();
        queue.get(nid.get()).add(pair.getLeftElement(), pair.getRightElement());
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();
      for (int i = 0; i < sources.length; i ++) {
        for (PairOfIntFloat pair : queue.get(i).extractAll()) {
          key.set(pair.getLeftElement());
          value.set(pair.getRightElement());
          context.write(key, value);
        }
      }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String TOP = "top";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("top n").create(TOP));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
            .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sourcesString = cmdline.getOptionValue(SOURCES);
    String[] sources = sourcesString.split(",");
    for (int i = 0; i < sources.length; i ++) {
      sources[i] = sources[i].trim();
    }

    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - use sources: " + sourcesString);

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);
    conf.setStrings(SOURCES, sources);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PairOfIntFloat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PairOfIntFloat.class);
    // Text instead of FloatWritable so we can control formatting

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    FileSystem fs = FileSystem.get(getConf());
    FSDataInputStream inputStream = fs.open(new Path(outputPath + "/part-r-00000"));
    Scanner scanner = new Scanner(inputStream);
    for (int i = 0; i < sources.length; i++) {
      System.out.println("Source: " + sources[i]);
      for (int j = 0; j < n; j++) {
        int nid = scanner.nextInt();
        float pageRank = scanner.nextFloat();
        System.out.printf("%.5f %d\n", pageRank, nid);
      }
      System.out.println();
    }
    inputStream.close();
    scanner.close();
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
