package ca.uwaterloo.cs.bigdata2017w.assignment1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.pair.PairOfStrings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);
    private static final String INTERMEDIATE_FILE = "pair_intermediate_file";
    public static final String THRESHOLD_NAME = "threshold";

    private static final class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final Text KEY = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Set<String> uniqueWords = new HashSet<>();
            int count = 0;
            for (String word : Tokenizer.tokenize(value.toString())) {
                count ++;
                if (uniqueWords.add(word)) {
                    KEY.set(word);
                    context.write(KEY, ONE);
                }
                if (count >= 40) break;
            }
            // count line number, even if no tokens
            KEY.set("*");
            context.write(KEY, ONE);
        }
    }

    private static final class WordCombiner extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class WordReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private static final IntWritable SUM = new IntWritable();
        private int threshold;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.threshold = context.getConfiguration().getInt(THRESHOLD_NAME, 0);
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            if (sum >= threshold) {
                SUM.set(sum);
                context.write(key, SUM);
            }
        }
    }

    private static final class WordPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private static final class PairMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        private static final PairOfStrings PAIR = new PairOfStrings();
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Set<String> uniqueWords = new HashSet<>();
            int count = 0;
            for (String word : Tokenizer.tokenize(value.toString())) {
                uniqueWords.add(word);
                count++;
                if (count >= 40) break;
            }
            for (String w1 : uniqueWords) {
                for (String w2 : uniqueWords) {
                    if (!w1.equals(w2)) {
                        PAIR.set(w1, w2);
                        context.write(PAIR, ONE);
                    }
                }
            }
        }
    }

    private static final class PairCombiner extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }

    private static final class PairReducer extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, PairOfFloatInt> {

        private int threshold = 0;
        private float numberOfLines = 0;
        private static Map<String, Integer> wordCounts = new HashMap<>();
        private static final PairOfFloatInt PAIR_OF_FLOAT_INT = new PairOfFloatInt();

        @Override
        public void setup(Context context) throws IOException {
            this.threshold = context.getConfiguration().getInt(THRESHOLD_NAME, 0);
            FileSystem hdfs = FileSystem.get(context.getConfiguration());
            RemoteIterator<LocatedFileStatus> files = hdfs.listFiles(new Path(INTERMEDIATE_FILE), false);
            while (files.hasNext()) {
                try (Scanner in = new Scanner(hdfs.open(files.next().getPath()), StandardCharsets.UTF_8.name())){
                    while (in.hasNextLine()) {
                        String key = in.next();
                        Integer count = in.nextInt();
                        in.nextLine();
                        if (key.equals("*")) {
                            numberOfLines = count;
                        } else {
                            wordCounts.put(key, count);
                        }
                    }
                }
            }

        }

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> iter = values.iterator();
            int sum = 0;
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            if (sum < threshold) return;
            int count1 = this.wordCounts.get(key.getLeftElement());
            int count2 = this.wordCounts.get(key.getRightElement());
            float pmi = (float) Math.log10(numberOfLines / count1 * sum / count2);
            PAIR_OF_FLOAT_INT.set(pmi, sum);
            context.write(key, PAIR_OF_FLOAT_INT);
        }
    }

    private static final class PairPartitioner extends Partitioner<PairOfStrings, IntWritable> {
        @Override
        public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private PairsPMI() {}

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-imc", metaVar = "[num]", usage = "use in-mapper combining")
        boolean imc = false;

        @Option(name = "-threshold", metaVar = "[num]", usage = "threshold for pairs")
        int threshold = 0;
    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - number of reducers: " + args.numReducers);
        LOG.info(" - imc: " + args.imc);
        LOG.info(" - threshold: " + args.threshold);

        Job wordJob = Job.getInstance(getConf());

        Configuration wordJobConfiguration = wordJob.getConfiguration();
        wordJobConfiguration.setInt(THRESHOLD_NAME, args.threshold);
        wordJobConfiguration.setInt("mapred.max.split.size", 1024 * 1024 * 32);
        wordJobConfiguration.set("mapreduce.map.memory.mb", "3072");
        wordJobConfiguration.set("mapreduce.map.java.opts", "-Xmx3072m");
        wordJobConfiguration.set("mapreduce.reduce.memory.mb", "3072");
        wordJobConfiguration.set("mapreduce.reduce.java.opts", "-Xmx3072m");

        wordJob.setJobName(PairsPMI.class.getSimpleName());
        wordJob.setJarByClass(PairsPMI.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem hdfs = FileSystem.get(getConf());
        hdfs.delete(outputDir, true);

        Path intermediatePath = new Path(INTERMEDIATE_FILE);
        hdfs.delete(intermediatePath, true);

        wordJob.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(wordJob, new Path(args.input));
        FileOutputFormat.setOutputPath(wordJob, intermediatePath);

        wordJob.setMapOutputKeyClass(Text.class);
        wordJob.setMapOutputValueClass(IntWritable.class);
        wordJob.setOutputKeyClass(Text.class);
        wordJob.setOutputValueClass(IntWritable.class);

        wordJob.setMapperClass(WordMapper.class);
        if (args.imc) {
            wordJob.setCombinerClass(WordCombiner.class);
        }
        wordJob.setReducerClass(WordReducer.class);
        wordJob.setPartitionerClass(WordPartitioner.class);

        wordJob.waitForCompletion(true);
        long startTime = System.currentTimeMillis();


        Job pairJob = Job.getInstance(getConf());

        Configuration pairJobConfiguration = pairJob.getConfiguration();
        pairJobConfiguration.setInt(THRESHOLD_NAME, args.threshold);
        pairJobConfiguration.setInt("mapred.max.split.size", 1024 * 1024 * 32);
        pairJobConfiguration.set("mapreduce.map.memory.mb", "3072");
        pairJobConfiguration.set("mapreduce.map.java.opts", "-Xmx3072m");
        pairJobConfiguration.set("mapreduce.reduce.memory.mb", "3072");
        pairJobConfiguration.set("mapreduce.reduce.java.opts", "-Xmx3072m");

        pairJob.setJobName(PairsPMI.class.getSimpleName());
        pairJob.setJarByClass(PairsPMI.class);

        pairJob.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(pairJob, new Path(args.input));
        FileOutputFormat.setOutputPath(pairJob, new Path(args.output));

        pairJob.setMapOutputKeyClass(PairOfStrings.class);
        pairJob.setMapOutputValueClass(IntWritable.class);
        pairJob.setOutputKeyClass(PairOfStrings.class);
        pairJob.setOutputValueClass(IntWritable.class);

        pairJob.setMapperClass(PairMapper.class);
        if (args.imc) {
            pairJob.setCombinerClass(PairCombiner.class);
        }
        pairJob.setReducerClass(PairReducer.class);
        pairJob.setPartitionerClass(PairPartitioner.class);

        pairJob.waitForCompletion(true);

        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        hdfs.delete(intermediatePath, true);
        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
    }
}