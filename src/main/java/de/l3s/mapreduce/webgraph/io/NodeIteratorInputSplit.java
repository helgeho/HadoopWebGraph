package de.l3s.mapreduce.webgraph.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NodeIteratorInputSplit extends InputSplit implements Writable {
    private String basename;
    private int splits;
    private int index;

    public NodeIteratorInputSplit() {
    }

    public NodeIteratorInputSplit(String basename, int splits, int index) {
        this.basename = basename;
        this.splits = splits;
        this.index = index;
    }

    public String getBasename() {
        return basename;
    }

    public int getSplits() {
        return splits;
    }

    public int getIndex() {
        return index;
    }

    public String toString() {
        return basename + " / " + splits + ": " + index;
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, basename);
        out.writeInt(splits);
        out.writeInt(index);
    }

    public void readFields(DataInput in) throws IOException {
        basename = Text.readString(in);
        splits = in.readInt();
        index = in.readInt();
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }
}
