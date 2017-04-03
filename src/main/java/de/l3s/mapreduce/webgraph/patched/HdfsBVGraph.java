package de.l3s.mapreduce.webgraph.patched;

import de.l3s.mapreduce.webgraph.io.HdfsRepositionableStream;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.FastMultiByteArrayInputStream;
import it.unimi.dsi.fastutil.longs.AbstractLongIterator;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import it.unimi.dsi.webgraph.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Properties;

import it.unimi.dsi.bits.Fast;

/**
 * Large parts of this code are copied from the original BVGraph class and patched to read data from HDFS.
 * @see <a href="http://webgraph.di.unimi.it/docs/it/unimi/dsi/webgraph/BVGraph.html">it.unimi.dsi.webgraph.BVGraph</a>
 */
@SuppressWarnings("resource")
public class HdfsBVGraph extends BVGraph {
    public static final int STD_BUFFER_SIZE = 1024 * 1024;

    private int flags = 0;
    private InputBitStream outdegreeIbs;

    public BVGraph copy() {
        final HdfsBVGraph result = new HdfsBVGraph(this.fs);
        result.basename = basename;
        result.n = n;
        result.m = m;
        result.isMemory = isMemory;
        result.isMapped = isMapped;
        result.graphMemory = graphMemory;
        result.graphStream = graphStream != null ? new FastMultiByteArrayInputStream( graphStream ) : null;
        result.mappedGraphStream = mappedGraphStream != null ? mappedGraphStream.copy() : null;
        result.offsets = offsets;
        result.maxRefCount = maxRefCount;
        result.windowSize = windowSize;
        result.minIntervalLength = minIntervalLength;
        result.offsetType = offsetType;
        result.zetaK = zetaK;
        result.outdegreeCoding = outdegreeCoding;
        result.blockCoding = blockCoding;
        result.residualCoding = residualCoding;
        result.referenceCoding = referenceCoding;
        result.blockCountCoding = blockCountCoding;
        result.offsetCoding = offsetCoding;
        result.flags = flags;
        result.outdegreeIbs = new InputBitStream(graphStream(), 0 );
        return result;
    }

    private FileSystem fs;

    protected HdfsBVGraph(FileSystem fs) {
        super();
        this.fs = fs;
    }

    public HdfsRepositionableStream graphStream() {
        return openFile(basename + GRAPH_EXTENSION);
    }

    private HdfsRepositionableStream openFile(String filename) {
        try {
            return new HdfsRepositionableStream(fs.open(new Path(filename)));
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }

    @Override
    public int outdegree( final int x ) throws IllegalStateException {
        if ( x == cachedNode ) return cachedOutdegree;
        if ( x < 0 || x >= n ) throw new IllegalArgumentException( "Node index out of range: " + x );

        try {
            if ( offsetType <= 0 ) throw new IllegalStateException( "You cannot compute the outdegree of a random node without offsets" );
            outdegreeIbs.position( offsets.getLong( cachedNode = x ) );
            cachedOutdegree = readOutdegree( outdegreeIbs );
            cachedPointer = outdegreeIbs.position();
            return cachedOutdegree;
        }
        catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

    private int outdegreeInternal( final int x ) throws IOException {
        if ( x == cachedNode ) return cachedOutdegree;
        outdegreeIbs.position( offsets.getLong( cachedNode = x ) );
        cachedOutdegree = readOutdegree( outdegreeIbs );
        cachedPointer = outdegreeIbs.position();
        return cachedOutdegree;
    }

    public LazyIntIterator successors( final int x ) {
        if ( x < 0 || x >= n ) throw new IllegalArgumentException( "Node index out of range: " + x );
        final InputBitStream ibs = new InputBitStream(openFile(basename + GRAPH_EXTENSION), 0 );
        return successors( x, ibs, null, null );
    }

    protected LazyIntIterator successors( final int x, final InputBitStream ibs, final int window[][], final int outd[] ) throws IllegalStateException {
        final int ref, refIndex;
        int i, extraCount, blockCount = 0;
        int[] block = null, left = null, len = null;

        if ( x < 0 || x >= n ) throw new IllegalArgumentException( "Node index out of range:" + x );

        try {
            final int d;
            final int cyclicBufferSize = windowSize + 1;

            if ( window == null ) {
                d = outdegreeInternal( x );
                ibs.position( cachedPointer );
            }
            else d = outd[ x % cyclicBufferSize ] = readOutdegree( ibs );

            if ( d == 0 ) return LazyIntIterators.EMPTY_ITERATOR;

            // We read the reference only if the actual window size is larger than one (i.e., the one specified by the user is larger than 0).
            if ( windowSize > 0 ) ref = readReference( ibs );
            else ref = -1;

            refIndex = ( x - ref + cyclicBufferSize ) % cyclicBufferSize; // The index in window[] of the node we are referring to (it makes sense only if ref>0).

            if ( ref > 0 ) { // This catches both no references at all and no reference specifically for this node.
                if ( ( blockCount = readBlockCount( ibs ) ) !=  0 ) block = new int[ blockCount ];

                int copied = 0, total = 0; // The number of successors copied, and the total number of successors specified in some copy block.
                for( i = 0; i < blockCount; i++ ) {
                    block[ i ] = readBlock( ibs ) + ( i == 0 ? 0 : 1 );
                    total += block[ i ];
                    if ( i % 2 == 0 ) copied += block[ i ];
                }
                // If the block count is even, we must compute the number of successors copied implicitly.
                //if ( window == null ) nextOffset = offsets.getLong( x - ref );
                if ( blockCount % 2 == 0 ) copied += ( window != null ? outd[ refIndex ] : outdegreeInternal( x - ref ) ) - total;
                extraCount = d - copied;
            }
            else extraCount = d;

            int intervalCount = 0; // Number of intervals

            if ( extraCount > 0 ) {

                // Prepare to read intervals, if any
                if ( minIntervalLength != NO_INTERVALS && ( intervalCount = ibs.readGamma() ) != 0 ) {

                    int prev = 0; // Holds the last integer in the last interval.
                    left = new int[ intervalCount ];
                    len = new int[ intervalCount ];

                    // Now we read intervals
                    left[ 0 ] = prev = (int)( Fast.nat2int( ibs.readLongGamma() ) + x );
                    len[ 0 ] = ibs.readGamma() + minIntervalLength;

                    prev += len[ 0 ];
                    extraCount -= len[ 0 ];

                    for ( i = 1; i < intervalCount; i++ ) {
                        left[ i ] = prev = ibs.readGamma() + prev + 1;
                        len[ i ] = ibs.readGamma() + minIntervalLength;
                        prev += len[ i ];
                        extraCount -= len[ i ];
                    }
                }
            }

            final int residualCount = extraCount; // Just to be able to use an anonymous class.

            final LazyIntIterator residualIterator = residualCount == 0 ? null : new ResidualIntIterator( this, ibs, residualCount, x );

            // The extra part is made by the contribution of intervals, if any, and by the residuals iterator.
            final LazyIntIterator extraIterator = intervalCount == 0
                    ? residualIterator
                    : ( residualCount == 0
                    ? (LazyIntIterator)new IntIntervalSequenceIterator( left, len )
                    : (LazyIntIterator)new MergedIntIterator( new IntIntervalSequenceIterator( left, len ), residualIterator )
            );

            final LazyIntIterator blockIterator = ref <= 0
                    ? null
                    : new MaskedIntIterator(
                    // ...block for masking copy and...
                    block,
                    // ...the reference list (either computed recursively or stored in window)...
                    window != null
                            ? LazyIntIterators.wrap( window[ refIndex ], outd[ refIndex ] )
                            :
                            // This is the recursive lazy part of the construction.
                            successors( x - ref, new InputBitStream(graphStream(), 0 ), null, null )
            );

            if ( ref <= 0 ) return extraIterator;
            else return extraIterator == null
                    ? blockIterator
                    : (LazyIntIterator)new MergedIntIterator( blockIterator, extraIterator );

        }
        catch ( IOException e ) {
            throw new RuntimeException( "Exception while accessing node " + x + ", stream position " + ibs.position(), e );
        }
    }

    public class HdfsBVGraphNodeIterator extends NodeIterator {
        @SuppressWarnings("hiding")
        final private int n = numNodes();
        final InputBitStream ibs;
        final private int cyclicBufferSize = windowSize + 1;
        final private int[][] window = new int[ cyclicBufferSize ][ INITIAL_SUCCESSOR_LIST_LENGTH ];
        final private int[] outd = new int[ cyclicBufferSize ];
        final private int from;
        private int curr;
        private int upperBound;

        private HdfsBVGraphNodeIterator(  final int from, final int upperBound, final long streamPosition, final int[][] window, final int[] outd ) throws IOException {
            if ( from < 0 || from > n ) throw new IllegalArgumentException( "Node index out of range: " + from );
            this.from = from;
            ibs = createInputBitStream();
            ibs.position( streamPosition );
            if ( window != null ) {
                for ( int i = 0; i < window.length; i++ ) System.arraycopy( window[ i ], 0, this.window[ i ] = IntArrays.grow( this.window[ i ], outd[ i ], 0 ), 0, outd[ i ] );
                System.arraycopy( outd, 0, this.outd, 0, outd.length );
            } else if ( from != 0 ) {
                int pos;
                for( int i = 1; i < Math.min( from + 1, cyclicBufferSize ); i++ ) {
                    pos = ( from - i + cyclicBufferSize ) % cyclicBufferSize;
                    this.outd[ pos ] = HdfsBVGraph.this.outdegreeInternal( from - i );
                    System.arraycopy( HdfsBVGraph.this.successorArray( from - i ), 0, this.window[ pos ] = IntArrays.grow( this.window[ pos ], this.outd[ pos ], 0 ), 0, this.outd[ pos ] );
                }
                this.ibs.position( offsets.getLong( from ) ); // We must fix the bit stream position so that we are *before* the outdegree.
            }
            curr = from - 1;
            this.upperBound = upperBound;
        }

        private HdfsBVGraphNodeIterator(  final int from ) throws IOException {
            this( from, Integer.MAX_VALUE, 0, null, null );
        }

        public int nextInt() {
            if ( ! hasNext() ) throw new NoSuchElementException();

            final int currIndex = ++curr % cyclicBufferSize;
            final LazyIntIterator i = HdfsBVGraph.this.successors( curr, ibs, window, outd );

            final int d = outd[ currIndex ];
            if ( window[ currIndex ].length < d ) window[ currIndex ] = new int[ d ];
            final int[] w = window[ currIndex ];
            for( int j = 0; j < d; j++ ) w[ j ] = i.nextInt();

            return curr;
        }

        public boolean hasNext() {
            return curr < Math.min( n - 1, upperBound - 1 );
        }

        public LazyIntIterator successors() {
            if ( curr == from - 1 ) throw new IllegalStateException();

            final int currIndex = curr % cyclicBufferSize;
            return LazyIntIterators.wrap( window[ currIndex ], outd[ currIndex ] );
        }

        public int[] successorArray() {
            if ( curr == from - 1 ) throw new IllegalStateException();

            return window[ curr % cyclicBufferSize ];
        }

        public int outdegree() {
            if ( curr == from - 1 ) throw new IllegalStateException();
            return outd[ curr % cyclicBufferSize ];
        }

        @Override
        public NodeIterator copy( final int upperBound ) {
            try {
                return new HdfsBVGraphNodeIterator( curr + 1, upperBound, ibs.position(), window, outd );
            }
            catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }

        private InputBitStream createInputBitStream() throws FileNotFoundException {
            return new InputBitStream(graphStream(), 0 );
        }
    };

    @Override
    public NodeIterator nodeIterator( final int from ) {
        try {
            return new HdfsBVGraphNodeIterator( from );
        } catch ( FileNotFoundException e ) {
            throw new IllegalStateException( "The graph file \"" + basename + GRAPH_EXTENSION + "\" cannot be found" );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }

    private void setFlags( final int flags ) {
        this.flags = flags;
        if ( ( flags & 0xF ) != 0 ) outdegreeCoding = flags & 0xF;
        if ( ( ( flags >>> 4 ) & 0xF ) != 0 ) blockCoding = ( flags >>> 4 ) & 0xF;
        if ( ( ( flags >>> 8 ) & 0xF ) != 0 ) residualCoding = ( flags >>> 8 ) & 0xF;
        if ( ( ( flags >>> 12 ) & 0xF ) != 0 ) referenceCoding = ( flags >>> 12 ) & 0xF;
        if ( ( ( flags >>> 16 ) & 0xF ) != 0 ) blockCountCoding = ( flags >>> 16 ) & 0xF;
        if ( ( ( flags >>> 20 ) & 0xF ) != 0 ) offsetCoding = ( flags >>> 20 ) & 0xF;
    }

    public static HdfsBVGraph load(FileSystem fs, CharSequence basename, ProgressLogger pl) throws IOException {
        return new HdfsBVGraph(fs).loadInternal( basename, pl );
    }

    public static HdfsBVGraph load(FileSystem fs, CharSequence basename) throws IOException {
        return load(fs, basename, null );
    }

    private static int string2Flags( final String flagString ) throws IOException {
        int flags = 0;

        if ( flagString != null && flagString.length() != 0 ) {
            String f[] = flagString.split( "\\|" );
            for( int i = 0; i < f.length; i++ ) {
                try {
                    flags |= BVGraph.class.getField( f[ i ].trim() ).getInt( BVGraph.class );
                }
                catch ( Exception notFound ) {
                    throw new IOException( "Compression flag " + f[ i ] + " unknown." );
                }
            }
        }
        return flags;
    }

    public LongBigList offsets() {
        return offsets;
    }

    private HdfsBVGraph loadInternal( final CharSequence basename, final ProgressLogger pl ) throws IOException {
        final InputStream propertyFile = openFile(basename + PROPERTIES_EXTENSION);
        final Properties properties = new Properties();
        properties.load( propertyFile );
        propertyFile.close();

        this.offsetType = 2; // memory-mapped, random-access
        this.basename = new MutableString(basename);

        if ( ! getClass().getSuperclass().getName().equals( properties.getProperty( ImmutableGraph.GRAPHCLASS_PROPERTY_KEY ).replace( "it.unimi.dsi.big.webgraph", "it.unimi.dsi.webgraph" ) ) )
            throw new IOException( "This class (" + getClass().getSuperclass().getName() + ") cannot load a graph stored using class \"" + properties.getProperty( ImmutableGraph.GRAPHCLASS_PROPERTY_KEY ) + "\"" );

        setFlags( string2Flags( properties.getProperty( "compressionflags" ) ) );
        if ( properties.getProperty( "version" ) == null ) throw new IOException( "Missing format version information" );
        else if ( Integer.parseInt( properties.getProperty( "version" ) ) > BVGRAPH_VERSION ) throw new IOException( "This graph uses format " + properties.getProperty( "version" ) + ", but this class can understand only graphs up to format " + BVGRAPH_VERSION );;

        final long nodes = Long.parseLong( properties.getProperty( "nodes" ) );
        if ( nodes > Integer.MAX_VALUE ) throw new IllegalArgumentException( "The standard version of WebGraph cannot handle graphs with " + nodes + " (>2^31) nodes" );
        n = (int)nodes;
        m = Long.parseLong( properties.getProperty( "arcs" ) );
        windowSize = Integer.parseInt( properties.getProperty( "windowsize" ) );
        maxRefCount = Integer.parseInt( properties.getProperty( "maxrefcount" ) );
        minIntervalLength = Integer.parseInt( properties.getProperty( "minintervallength" ) );
        if(properties.getProperty("zetak") != null) this.zetaK = Integer.parseInt(properties.getProperty("zetak"));

        InputBitStream offsetIbs = new InputBitStream(openFile(basename + OFFSETS_EXTENSION), STD_BUFFER_SIZE);

        if(pl != null) {
            pl.itemsName = "deltas";
            pl.start("Loading offsets...");
        }

        long graphLength = fs.getFileStatus(new Path(basename + GRAPH_EXTENSION)).getLen();
        offsets = new EliasFanoMonotoneLongBigList((long)(n + 1), graphLength * 8L + 1L, new HdfsBVGraph.OffsetsLongIterator(this, offsetIbs));

        if(pl != null) {
            pl.count = (long)(this.n + 1);
            pl.done();
            pl.logger().info("Pointer bits per node: " + Util.format((double)((EliasFanoMonotoneLongBigList)this.offsets).numBits() / ((double)this.n + 1.0D)));
        }

        offsetIbs.close();

        outdegreeIbs = new InputBitStream(graphStream(), 0);

        return this;
    }

    @Override
    public void writeOffsets( final OutputBitStream obs, final ProgressLogger pl ) throws IOException {
        final HdfsBVGraphNodeIterator nodeIterator = (HdfsBVGraphNodeIterator) nodeIterator( 0 );
        int n = numNodes();
        long lastOffset = 0;
        while( n-- != 0 ) {
            writeOffset( obs, nodeIterator.ibs.readBits() - lastOffset );
            lastOffset = nodeIterator.ibs.readBits();
            nodeIterator.nextInt();
            nodeIterator.outdegree();
            nodeIterator.successorArray();
            if ( pl != null ) pl.update();
        }
        writeOffset( obs, nodeIterator.ibs.readBits() - lastOffset );
    }

    private final static class OffsetsLongIterator extends AbstractLongIterator {
        private final InputBitStream offsetIbs;
        private final int n;
        private long off;
        private int i;
        private HdfsBVGraph g;

        private OffsetsLongIterator(HdfsBVGraph g, InputBitStream offsetIbs) {
            this.offsetIbs = offsetIbs;
            this.g = g;
            this.n = g.numNodes();
        }

        public boolean hasNext() {
            return this.i <= this.n;
        }

        public long nextLong() {
            ++this.i;

            try {
                return off += g.readOffset(offsetIbs);
            } catch (IOException var2) {
                throw new RuntimeException(var2);
            }
        }
    }

    private final static class ResidualIntIterator extends AbstractLazyIntIterator {
        /** The graph associated to this iterator. */
        private final HdfsBVGraph g;
        /** The input bit stream from which residuals will be read. */
        private final InputBitStream ibs;
        /** The last residual returned. */
        private int next;
        /** The number of remaining residuals. */
        private int remaining;

        private ResidualIntIterator( final HdfsBVGraph g, final InputBitStream ibs, final int residualCount, final int x ) {
            this.g = g;
            this.remaining = residualCount;
            this.ibs = ibs;
            try {
                this.next = (int)( x  + Fast.nat2int( g.readLongResidual( ibs ) ) );
            }
            catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }

        public int nextInt() {
            if ( remaining == 0 ) return -1;
            try {
                final int result = next;
                if ( --remaining != 0 ) next += g.readResidual( ibs ) + 1;
                return result;
            }
            catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }

        @Override
        public int skip( int n ) {
            if ( n >= remaining ) {
                n = remaining;
                remaining = 0;
                return n;
            }
            try {
                for( int i = n; i-- != 0; ) next += g.readResidual( ibs ) + 1;
                remaining -= n;
                return n;
            }
            catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }

    }
}
