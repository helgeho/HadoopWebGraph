package de.l3s.mapreduce.webgraph.io;

import de.l3s.mapreduce.webgraph.patched.HdfsBVGraph;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WebGraphInputFormat extends InputFormat<IntWritable, IntArrayWritable> {
    public static final String BASENAME_PROPERTY = "de.l3s.mapreduce.webgraph.basename";
    public static final String SPLITS_PROPERTY = "de.l3s.mapreduce.webgraph.splits";
    public static final int DEFAULT_SPLITS = 100;

    public static class WebGraphRecordReader extends RecordReader<IntWritable, IntArrayWritable> {
        private FileSystem fs = null;
        private NodeIterator iterator = null;
        private IntWritable key = new IntWritable();
        private IntArrayWritable values = new IntArrayWritable();
        private BVGraph graph = null;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            NodeIteratorInputSplit split = (NodeIteratorInputSplit)inputSplit;
            Configuration conf = context.getConfiguration();

            if (graph == null) {
                if (fs == null) fs = new Path(split.getBasename() + BVGraph.GRAPH_EXTENSION).getFileSystem(conf);
                graph = HdfsBVGraph.load(fs, split.getBasename());
            }

            NodeIterator[] iterators = graph.splitNodeIterators(split.getSplits());
            int index = split.getIndex();
            iterator = index < iterators.length ? iterators[index] : null;

            for (int i = 0; i < iterators.length; i++) {
                if (i != index) closeIterator(iterators[i]);
            }
        }

        private void closeIterator(NodeIterator iterator) {
            if (iterator != null) {
                try {
                    ((HdfsBVGraph.HdfsBVGraphNodeIterator)iterator).close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (iterator.hasNext()) {
                key.set(iterator.nextInt());
                int[] successors = Arrays.copyOf(iterator.successorArray(), iterator.outdegree());
                values.set(successors);
                return true;
            } else {
                closeIterator(iterator);
                iterator = null;
                return false;
            }
        }

        @Override
        public IntWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public IntArrayWritable getCurrentValue() throws IOException, InterruptedException {
            return values;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return iterator == null ? 1.0f : 0.5f;
        }

        @Override
        public void close() throws IOException {
            closeIterator(iterator);
            if (fs != null) {
                fs.close();
                fs = null;
            }
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        String basename = getBasename(context);
        int numberSplits = getNumberOfSplits(context);
        List<InputSplit> splits = new ArrayList<>(numberSplits);
        for (int i = 0; i < numberSplits; i++) {
            splits.add(new NodeIteratorInputSplit(basename, numberSplits, i));
        }
        return splits;
    }

    @Override
    public RecordReader<IntWritable, IntArrayWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new WebGraphRecordReader();
    }

    public static void setBasename(Configuration conf, String basename) throws IOException {
        conf.set(BASENAME_PROPERTY, basename);
    }

    public static void setBasename(Job job, String basename) throws IOException {
        setBasename(job.getConfiguration(), basename);
    }

    public static String getBasename(JobContext context) {
        return context.getConfiguration().get(BASENAME_PROPERTY, "");
    }

    public static void setNumberOfSplits(Configuration conf, int splits) throws IOException {
        conf.setInt(SPLITS_PROPERTY, splits);
    }

    public static void setNumberOfSplits(Job job, int splits) throws IOException {
        setNumberOfSplits(job.getConfiguration(), splits);
    }

    public static int getNumberOfSplits(JobContext context) {
        return context.getConfiguration().getInt(SPLITS_PROPERTY, DEFAULT_SPLITS);
    }
}
