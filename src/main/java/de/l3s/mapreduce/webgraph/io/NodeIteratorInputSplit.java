package de.l3s.mapreduce.webgraph.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NodeIteratorInputSplit extends InputSplit implements Writable {
    private int from;
    private long length;
    private String[] hosts;

    public NodeIteratorInputSplit() { }

    public NodeIteratorInputSplit(int from, long length, String[] hosts) {
        this.from = from;
        this.length = length;
        this.hosts = hosts;
    }

    public int getFrom() {
        return from;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(from);
        out.writeLong(length);
        out.writeInt(hosts.length);
        for (String host : hosts) Text.writeString(out, host);
    }

    public void readFields(DataInput in) throws IOException {
        from = in.readInt();
        length = in.readLong();
        hosts = new String[in.readInt()];
        for (int i = 0; i < hosts.length; i++) hosts[i] = Text.readString(in);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return hosts;
    }
}
