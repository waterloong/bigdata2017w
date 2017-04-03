package ca.uwaterloo.cs.bigdata2017w.assignment7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.IOException;
import java.util.*;

public class BooleanRetrievalHBase extends Configured implements Tool {

  private Stack<Set<Integer>> stack;
  private Table indexTable;
  private Table collectionTable;
  private static final Logger LOG = Logger.getLogger(BooleanRetrievalHBase.class);

  public static final String[] FAMILIES = { "c" };
  public static final byte[] CF = FAMILIES[0].getBytes();
  public static final byte[] TEXT = "text".getBytes();

  private BooleanRetrievalHBase() {}

  private void initialize() throws IOException {
    stack = new Stack<>();
  }

  private void runQuery(String q) throws IOException {
    String[] terms = q.split("\\s+");

    for (String t : terms) {
      if (t.equals("AND")) {
        performAND();
      } else if (t.equals("OR")) {
        performOR();
      } else {
        pushTerm(t);
      }
    }

    Set<Integer> set = stack.pop();

    for (Integer i : set) {
      String line = fetchLine(i);
      System.out.println(i + "\t" + line);
    }
  }

  private void pushTerm(String term) throws IOException {
    stack.push(fetchDocumentSet(term));
  }

  private void performAND() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      if (s2.contains(n)) {
        sn.add(n);
      }
    }

    stack.push(sn);
  }

  private void performOR() {
    Set<Integer> s1 = stack.pop();
    Set<Integer> s2 = stack.pop();

    Set<Integer> sn = new TreeSet<>();

    for (int n : s1) {
      sn.add(n);
    }

    for (int n : s2) {
      sn.add(n);
    }

    stack.push(sn);
  }

  private Set<Integer> fetchDocumentSet(String term) throws IOException {
    Set<Integer> set = new LinkedHashSet<>();
    List<Cell> cells = fetchPostings(term);
    if (cells != null) {
      for (Cell cell : cells) {
        int docid = Bytes.toInt(CellUtil.cloneQualifier(cell));
        set.add(docid);
      }
    }
    return set;
  }

  private List<Cell> fetchPostings(String term) throws IOException {
    Get get = new Get(Bytes.toBytes(term));
    Result result = indexTable.get(get);
    return result.listCells();
  }

  private static byte[] toBytes(long val) {
    return new byte[] {
            (byte) (val >> 24),
            (byte) (val >> 16),
            (byte) (val >> 8),
            (byte) val
    };
  }

  public String fetchLine(long offset) throws IOException {
    Get get = new Get(toBytes(offset));
    Result result = collectionTable.get(get);
    String d = new String(CellUtil.cloneValue(result.getColumnCells(CF, TEXT).get(0)));
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    public String collection;

    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
    public String config;

    @Option(name = "-index", metaVar = "[name]", required = true, usage = "HBase indexTable to query from")
    public String index;
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

    Configuration conf = getConf();
    conf.addResource(new Path(args.config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    Connection connection = ConnectionFactory.createConnection(hbaseConfig);
    this.indexTable = connection.getTable(TableName.valueOf(args.index));
    this.collectionTable = connection.getTable(TableName.valueOf(args.collection));

    initialize();

    System.out.println("Query: " + args.query);
    long startTime = System.currentTimeMillis();
    runQuery(args.query);
    System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

    return 1;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BooleanRetrievalHBase(), args);
  }
}
