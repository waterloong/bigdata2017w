package ca.uwaterloo.cs.bigdata2017w.assignment7;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

/**
 * Created by William on 2017-04-02.
 */
public class HBaseSearchEndpoint  extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(HBaseSearchEndpoint.class);
    public static final String[] FAMILIES = { "c" };
    public static final byte[] CF = FAMILIES[0].getBytes();
    public static final byte[] TEXT = "text".getBytes();

    private Table indexTable;
    private Table collectionTable;
    private Stack<Set<Integer>> stack;


    private String runQuery(String q) throws IOException {
        this.stack = new Stack<>();
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

        ObjectMapper mapper = new ObjectMapper();
        ArrayNode arrayNode = mapper.createArrayNode();
        for (Integer i : set) {
            String line = fetchLine(i);
            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put("docid", i);
            objectNode.put("text", line);
            arrayNode.add(objectNode);
        }
        return arrayNode.toString();
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
        @Option(name = "-port", metaVar = "[num]", required = true, usage = "server port number")
        int port = 0;

        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
        public String collection;

        @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
        public String config;

        @Option(name = "-index", metaVar = "[name]", required = true, usage = "HBase indexTable to query from")
        public String index;
    }

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

        Server server = new Server(args.port);
//        server.setHandler(new HelloHandler());
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        context.addServlet(new ServletHolder(new SearchServlet()),"/search");
        server.start();
        server.join();

        return 0;
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HBaseSearchEndpoint(), args);
    }


    private class SearchServlet extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp)
                throws ServletException, IOException {
            resp.setStatus(HttpStatus.OK_200);
            String query = req.getParameter("query");
            String result = runQuery(query);
            resp.getWriter().println(result);
            System.out.println(result);
            resp.setStatus(200);
        }
    }
}
