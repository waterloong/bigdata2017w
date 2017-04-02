package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.IOException;

public class InsertCollectionHBase extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(InsertCollectionHBase.class);
    public static final String[] FAMILIES = { "c" };
    public static final byte[] CF = FAMILIES[0].getBytes();
    public static final byte[] TEXT = "text".getBytes();

    private Table table;

    private InsertCollectionHBase() {}

    protected static class MyPartitioner extends Partitioner<LongWritable, Text> {

        @Override
        public int getPartition(LongWritable key, Text value, int numReduceTasks) {
            return ((int)(key.get()) & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private static final class MyReducer extends
            TableReducer<LongWritable, Text, ImmutableBytesWritable> {

        private static byte[] toBytes(LongWritable intWritable) {
            long val = intWritable.get();
            return new byte[] {
                    (byte) (val >> 24),
                    (byte) (val >> 16),
                    (byte) (val >> 8),
                    (byte) val
            };
        }

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text t: values) {
                Put put = new Put(toBytes(key));
                put.addColumn(CF, TEXT, t.copyBytes());
                context.write(null, put);
            }
        }
    }

    private static final class Args {

        @Option(name = "-input", metaVar = "[path]", required = true, usage = "collection path")
        public String collection;

        @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
        public String config;

        @Option(name = "-index", metaVar = "[name]", required = true, usage = "HBase index to query from")
        public String table;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        public int numReducers = 1;
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

        if (args.collection.endsWith(".gz")) {
            System.out.println("gzipped collection is not seekable: use compressed version!");
            return -1;
        }

// If the index doesn't already exist, create it.
        Configuration conf = getConf();
        conf.addResource(new Path(args.config));

        Configuration hbaseConfig = HBaseConfiguration.create(conf);
        Connection connection = ConnectionFactory.createConnection(hbaseConfig);
        Admin admin = connection.getAdmin();

        if (admin.tableExists(TableName.valueOf(args.table))) {
            LOG.info(String.format("Table '%s' exists: dropping index and recreating.", args.table));
            LOG.info(String.format("Disabling index '%s'", args.table));
            admin.disableTable(TableName.valueOf(args.table));
            LOG.info(String.format("Droppping index '%s'", args.table));
            admin.deleteTable(TableName.valueOf(args.table));
        }

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(args.table));
        for (int i = 0; i < FAMILIES.length; i++) {
            HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
            tableDesc.addFamily(hColumnDesc);
        }
        admin.createTable(tableDesc);
        LOG.info(String.format("Successfully created index '%s'", args.table));

        admin.close();

        Job job = Job.getInstance(getConf());
        job.setJobName(BuildInvertedIndexHBase.class.getSimpleName());
        job.setJarByClass(BuildInvertedIndexHBase.class);

        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.collection));

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        TableMapReduceUtil.initTableReducerJob(args.table, MyReducer.class, job);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new InsertCollectionHBase(), args);
    }
}
