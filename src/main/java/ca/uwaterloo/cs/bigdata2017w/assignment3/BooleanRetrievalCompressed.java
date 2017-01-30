package ca.uwaterloo.cs.bigdata2017w.assignment3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.pair.PairOfInts;

import java.io.*;
import java.util.*;

public class BooleanRetrievalCompressed extends Configured implements Tool {

  private FSDataInputStream collection;
  private Stack<Set<Integer>> stack;
  private List<Path> paths = new ArrayList<>();
  private int numberOfReducers = 1;
  private FileSystem dfs;

  private BooleanRetrievalCompressed() {}

  private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
    for (FileStatus f: fs.listStatus(new Path(indexPath))) {
      Path path = f.getPath();
      if (path.getName().startsWith("part-r-")) {
        paths.add(path);
      }
    }
    // fuck you altiscale
//    paths.sort(Comparator.comparing(Path::getName));
    Collections.sort(paths, new Comparator<Path>() {
      @Override
      public int compare(Path p1, Path p2) {
        return p1.getName().compareTo(p2.getName());
      }
    });
    this.numberOfReducers = paths.size();
    dfs = fs;
    collection = fs.open(new Path(collectionPath));
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
    Set<Integer> set = new TreeSet<>();

    for (PairOfInts pair : fetchPostings(term)) {
      set.add(pair.getLeftElement());
    }

    return set;
  }

  private ArrayListWritable<PairOfInts> fetchPostings(String term) throws IOException {
    Text key = new Text();
    key.set(term);
    BytesWritable value = new BytesWritable();
    int partition = (term.hashCode() & Integer.MAX_VALUE) % numberOfReducers;

    MapFile.Reader index = new MapFile.Reader(paths.get(partition), dfs.getConf());
    index.get(key, value);
    index.close();

    ByteArrayInputStream compressedPostings = new ByteArrayInputStream(value.getBytes());
    DataInputStream dataInputStream = new DataInputStream(compressedPostings);
    int df = WritableUtils.readVInt(dataInputStream);

    ArrayListWritable<PairOfInts> postings = new ArrayListWritable<>();
    int prevDoc = 0;
    for(int i = 0; i < df; i++) {
      int docId = WritableUtils.readVInt(dataInputStream) + prevDoc;
      prevDoc = docId;
      int tf = WritableUtils.readVInt(dataInputStream);
//      System.out.println("" + docId + " " + tf);
      postings.add(new PairOfInts(docId, tf));
    }

    compressedPostings.close();
    dataInputStream.close();

    return postings;
  }

  public String fetchLine(long offset) throws IOException {
    collection.seek(offset);
    BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

    String d = reader.readLine();
    return d.length() > 80 ? d.substring(0, 80) + "..." : d;
  }

  private static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    String collection;

    @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
    String query;
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

    FileSystem fs = FileSystem.get(new Configuration());

    initialize(args.index, args.collection, fs);

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
    ToolRunner.run(new BooleanRetrievalCompressed(), args);
  }
}
