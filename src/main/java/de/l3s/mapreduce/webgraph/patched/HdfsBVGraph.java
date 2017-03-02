package de.l3s.mapreduce.webgraph.patched;

import de.l3s.mapreduce.webgraph.io.HdfsRepositionableStream;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.FastMultiByteArrayInputStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;
import java.util.Properties;

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
        result.outdegreeIbs = offsetType <= 0 ? null : isMemory ? new InputBitStream( graphMemory ): new InputBitStream( isMapped ? mappedGraphStream.copy() : new FastMultiByteArrayInputStream( graphStream ), 0 );
        return result;
    }

    private FileSystem fs;

    protected HdfsBVGraph(FileSystem fs) {
        super();
        this.fs = fs;
    }

    private InputStream openFile(String filename) throws FileNotFoundException {
        try {
            return new HdfsRepositionableStream(fs.open(new Path(filename)));
        } catch ( FileNotFoundException e ) {
            throw e;
        } catch ( IOException e ) {
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

    public class HdfsBVGraphNodeIterator extends NodeIterator {
        @SuppressWarnings("hiding")
        final private int n = numNodes();
        final InputBitStream ibs;
        final private int cyclicBufferSize = windowSize + 1;
        final private int window[][] = new int[ cyclicBufferSize ][ INITIAL_SUCCESSOR_LIST_LENGTH ];
        final private int outd[] = new int[ cyclicBufferSize ];
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
            }
            else if ( from != 0 ) {
                if ( offsetType <= 0 ) throw new IllegalStateException( "You cannot iterate from a chosen node without offsets" );

                int pos;
                for( int i = 1; i < Math.min( from + 1, cyclicBufferSize ); i++ ) {
                    pos = ( from - i + cyclicBufferSize ) % cyclicBufferSize;
                    this.outd[ pos ] = HdfsBVGraph.this.outdegreeInternal( from - i );
                    System.arraycopy( HdfsBVGraph.this.successorArray( from - i ), 0, this.window[ pos ] = IntArrays.grow( this.window[ pos ], this.outd[ pos ], 0 ), 0, this.outd[ pos ] );
                }
                this.ibs.position( offsets.getLong( from ) );
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
            boolean hasNext = curr < Math.min( n - 1, upperBound - 1 );
            if (!hasNext) close();
            return hasNext;
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

        public void close() {
            try {
                ibs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        protected void finalize() throws Throwable {
            close();
            super.finalize();
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
            if ( offsetType == -1 )
                return new InputBitStream( openFile( basename + GRAPH_EXTENSION ), STD_BUFFER_SIZE );
            else
            if ( isMemory )
                return new InputBitStream( graphMemory );
            else
            if ( isMapped )
                return new InputBitStream( mappedGraphStream.copy() );
            else
                return new InputBitStream( new FastMultiByteArrayInputStream( graphStream ), 0 );
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

    public static BVGraph load(FileSystem fs, CharSequence basename, ProgressLogger pl) throws IOException {
        return new HdfsBVGraph(fs).loadInternal( basename, pl );
    }

    public static BVGraph load(FileSystem fs, CharSequence basename) throws IOException {
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

    private BVGraph loadInternal( final CharSequence basename, final ProgressLogger pl ) throws IOException {
        final InputStream propertyFile = openFile( basename + PROPERTIES_EXTENSION );
        final Properties properties = new Properties();
        properties.load( propertyFile );
        propertyFile.close();

        this.offsetType = -1;
        this.basename = new MutableString( basename );

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
        if ( properties.getProperty( "zetak" ) != null ) zetaK = Integer.parseInt( properties.getProperty( "zetak" ) );

        final InputBitStream offsetIbs = offsetType > 0 ? new InputBitStream( openFile( basename + OFFSETS_EXTENSION ), STD_BUFFER_SIZE ) : null;

        if ( offsetIbs != null ) offsetIbs.close();

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
}
