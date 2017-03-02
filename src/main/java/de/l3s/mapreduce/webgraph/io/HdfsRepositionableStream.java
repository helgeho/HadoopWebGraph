package de.l3s.mapreduce.webgraph.io;

import it.unimi.dsi.fastutil.io.RepositionableStream;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStream;

public class HdfsRepositionableStream extends InputStream implements RepositionableStream {
    private FSDataInputStream in = null;

    public HdfsRepositionableStream(FSDataInputStream in) {
        this.in = in;
    }

    @Override
    public void position(long l) throws IOException {
        in.seek(l);
    }

    @Override
    public long position() throws IOException {
        return in.getPos();
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return in.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return in.skip(n);
    }

    @Override
    public synchronized void mark(int readlimit) {
        in.mark(readlimit);
    }

    @Override
    public int available() throws IOException {
        return in.available();
    }

    @Override
    public synchronized void reset() throws IOException {
        in.reset();
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }
}
