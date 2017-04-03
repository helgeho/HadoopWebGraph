package de.l3s.mapreduce.webgraph.io;

import de.l3s.mapreduce.webgraph.patched.HdfsBVGraph;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

public class WebGraphInputFormat extends InputFormat<IntWritable, IntArrayWritable> {
    public static final String BASENAME_PROPERTY = "de.l3s.mapreduce.webgraph.basename";
    public static final String SPLITS_PROPERTY = "de.l3s.mapreduce.webgraph.splits";
    public static final int DEFAULT_SPLITS = 100;

    public static class WebGraphRecordReader extends RecordReader<IntWritable, IntArrayWritable> {
        private FileSystem fs = null;
        private NodeIterator iterator = null;
        private IntWritable key = new IntWritable();
        private IntArrayWritable values = new IntArrayWritable();
        private HdfsBVGraph graph = null;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            NodeIteratorInputSplit split = (NodeIteratorInputSplit)inputSplit;
            Configuration conf = context.getConfiguration();

            String basename = getBasename(context);
            int numSplits = getNumberOfSplits(context);

            if (graph == null) {
                if (fs == null) fs = FileSystem.get(conf);
                graph = HdfsBVGraph.load(fs, basename);
            }

            int splitSize = (int)Math.ceil((double)graph.numNodes() / (double)numSplits);
            int from = split.getFrom();
            iterator = graph.nodeIterator(from).copy(from + splitSize);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (iterator.hasNext()) {
                key.set(iterator.nextInt());
                int[] successors = Arrays.copyOf(iterator.successorArray(), iterator.outdegree());
                values.set(successors);
                return true;
            } else {
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
            if (fs != null) {
                fs.close();
                fs = null;
            }
        }
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        String basename = getBasename(context);
        int numSplits = getNumberOfSplits(context);
        List<InputSplit> splits = new ArrayList<>(numSplits);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        HdfsBVGraph graph = HdfsBVGraph.load(fs, basename);

        int numNodes = graph.numNodes();
        if(numNodes == 0 && numSplits == 0) {
            return splits;
        } else if(numSplits < 1) {
            throw new IllegalArgumentException("numberOfSplits < 0");
        } else {
            if (!graph.hasCopiableIterators() || !graph.randomAccess()) {
                throw new RuntimeException("random access is not supported by this graph, please provide an offset file");
            } else {
                int splitSize = (int)Math.ceil((double)numNodes / (double)numSplits);

                FileStatus graphFile = fs.getFileStatus(new Path(basename + BVGraph.GRAPH_EXTENSION));
                long fileLength = graphFile.getLen();

                int from = 0;
                long pos = 0;
                for (int nextFrom = splitSize; nextFrom < numNodes; nextFrom += splitSize) {
                    long nextPos = graph.offsets().getLong(nextFrom) >> 3; //  >> 3, see it.unimi.dsi.io.InputBitStream#position
                    long length = nextPos - pos;
                    BlockLocation[] locations = fs.getFileBlockLocations(graphFile, pos, length);
                    Set<String> hostSet = new HashSet<>();
                    for (BlockLocation location : locations) hostSet.addAll(Arrays.asList(location.getHosts()));
                    splits.add(new NodeIteratorInputSplit(from, length, hostSet.toArray(new String[0])));
                    pos = nextPos;
                    from = nextFrom;
                }

                long length = fileLength - pos;
                BlockLocation[] locations = fs.getFileBlockLocations(graphFile, pos, length);
                Set<String> hostSet = new HashSet<>();
                for (BlockLocation location : locations) hostSet.addAll(Arrays.asList(location.getHosts()));
                splits.add(new NodeIteratorInputSplit(from, length, hostSet.toArray(new String[0])));

                return splits;
            }
        }
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
