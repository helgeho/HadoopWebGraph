package de.l3s.mapreduce.webgraph.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
        super(IntWritable.class);
    }

    public IntArrayWritable(IntWritable[] values) {
        super(IntWritable.class, values);
    }

    @Override
    public IntWritable[] get() {
        return (IntWritable[])super.get();
    }

    public int[] getValues() {
        IntWritable[] writables = get();
        int[] values = new int[writables.length];
        for (int i = 0; i < writables.length; i++) {
            values[i] = writables[i].get();
        }
        return values;
    }

    public int[] values() {
        return getValues();
    }

    public void set(int[] values) {
        IntWritable[] writables = new IntWritable[values.length];
        for (int i = 0; i < values.length; i++) {
            writables[i] = new IntWritable(values[i]);
        }
        set(writables);
    }
}