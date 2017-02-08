package ca.uwaterloo.cs.bigdata2017w.assignment4;

import org.apache.hadoop.io.Writable;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.*;


public class PageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

  private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

  private Type type;
  private int nodeid;
  private ArrayListOfIntsWritable adjacenyList;
  private ArrayListOfFloatsWritable pageRanks;

  public PageRankNode() {}

  public ArrayListOfFloatsWritable getPageRanks() {
    return this.pageRanks;
  }

  public void setPageRanks(ArrayListOfFloatsWritable p) {
    this.pageRanks = p;
  }

  public int getNodeId() {
    return nodeid;
  }

  public void setNodeId(int n) {
    this.nodeid = n;
  }

  public ArrayListOfIntsWritable getAdjacenyList() {
    return adjacenyList;
  }

  public void setAdjacencyList(ArrayListOfIntsWritable list) {
    this.adjacenyList = list;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  /**
   * Deserializes this object.
   *
   * @param in source for raw byte representation
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int b = in.readByte();
    type = mapping[b];
    nodeid = in.readInt();

    if (type.equals(Type.Mass)) {
      pageRanks = new ArrayListOfFloatsWritable();
      pageRanks.readFields(in);
      return;
    }

    if (type.equals(Type.Complete)) {
      pageRanks = new ArrayListOfFloatsWritable();
      pageRanks.readFields(in);
    }

    adjacenyList = new ArrayListOfIntsWritable();
    adjacenyList.readFields(in);
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the raw byte representation
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(type.val);
    out.writeInt(nodeid);

    if (type.equals(Type.Mass)) {
      pageRanks.write(out);
      return;
    }

    if (type.equals(Type.Complete)) {
      pageRanks.write(out);
    }

    adjacenyList.write(out);
  }

  @Override
  public String toString() {
    return String.format("{%d %.4f %s}", nodeid, (pageRanks == null ? "[]" : pageRanks.toString(10)), (adjacenyList == null ? "[]"
        : adjacenyList.toString(10)));
  }

  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(DataInput in) throws IOException {
    PageRankNode m = new PageRankNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
