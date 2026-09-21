package com.raydash.client;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/*
Talks to a single raydash server over a small pool of tcp connections SET/GET 
are binary safe & connections can be authenticated if the server requires it.

Binary safe means read will be exactly the number of bytes sent instead of just 
reading till '\r\n'.
For AUTH if it's setup then every pooled connection sends "AUTH token" as part 
of connecting and requires a "+OK" back before it's considered usable.

Earlier this design was a single shared socket instead of now a shared pool why
we changed it? One socket behind a lock meant that every single SET/GET from 
every thread in the whole spring app will be queued up behind one TCP connection.
That's not a real concurrency no matter how many threads call it. A pool of 
independent connections mean multiple threads can genuinely be mid request at 
the same time each on their own socket.

Implements AutoCloseable only for one reason that is so spring can call close()
automatically when the application context shut down. RaydashAutoConfiguration
has a @Bean(destroyMethod = "close") for this reason only.
*/
public class RaydashClient implements AutoCloseable {
    private static final Integer CONNECT_TIMEOUT_MS = 2000; // 2 seconds
    private static final Integer READ_TIMEOUT_MS = 3000;    // 3 seconds
    private static final Integer DEFAULT_POOL_SIZE = 8;
    private static final Integer MAX_CONNECT_ATTEMPTS = 5;
    private static final Long BORROW_TIMEOUT_MS = 5000L;    // 5 seconds

    private final String host;
    private final Integer port;
    private final Integer poolSize;
    private final String authToken;

    /*
    BlockingQueue is java's built-in thread safe queue interface where 
    multiple threads can push/pop from it at once with no extra lock
    on our part. LinkedBlockingQueue is one implementation of it backed 
    by a linked list.
    We need 2 operations only poll() & offer(). Think of this pool as a
    box of "ready to use" connections, a thread that needs one takes it
    out (poll), uses it and then puts it back (offer).
    */
    private final BlockingQueue<Connection> pool;

    /*
    3 constructors with different defaults filled in using this(...) chaining
    */
    public RaydashClient(String host, Integer port) {
        this(host, port, DEFAULT_POOL_SIZE, null);
    }

    public RaydashClient(String host, Integer port, Integer poolSize) {
        this(host, port, poolSize, null);
    }

    public RaydashClient(String host, Integer port, Integer poolSize, String authToken) {
        this.host = host;
        this.port = port;
        this.poolSize = poolSize;
        this.authToken = authToken;
        /*
        Setting a poolSize capacity means offer() would refuse a connection
        past that count but this situation never occurs in practice since
        we only ever create exactly poolSize Connection objects in total.
        */
        this.pool = new LinkedBlockingQueue<>(poolSize);
    }

    /*
    Eagerly establish every connection in the pool retrying each one with
    backoff before giving up.
    "Eagerly" means all poolSize connections get made right away up front,
    rather than lazily the first time someone needs one (you can see this
    in practice when you spin up a server it gives logs for the poolSize
    connections that they are ready to accept requests). This way a broken
    server is discovered immediately at startup instead of silently when
    a requests happen to need a connection first.
    pool.put(conn) instead of offer() is used here because put() blocks if
    the queue is full instead of failing, but since again we are filling
    an empty queue one connection at a time to its exact capacity size it
    will never come to that and would never have to block. so honestly 
    you can use offer() here if you would want.    
    */
    public void connect() throws Exception {
        for(Integer i = 0; i < poolSize; i++) {
            Connection conn = new Connection();
            connectWithRetry(conn);
            pool.put(conn);
        }
    }

    /*
    ===================================================================
    SET <key> <byte-length>\r\n
    <exactly byte-length raw bytes>\r\n
    ===================================================================

    Example: 
    '
     SET project 7\r\n
     raydash\r\n
    '
    1. Command goes through 'writer', a PrintWriter which is a text 
       oriented wrapper. It's always plain ASCII/UTF-8 text we control 
       ourselves.
    2. VALUE itself goes through 'rawOut', the underlying OutputStream
       directly as raw bytes and not through PrintWriter. This is what
       the point is for "binary safe", a value could contain literally
       any byte possible including \r or \n but because its a length 
       prefixed raw write it lets a server tell exactly where the value
       ends without ever having to scan the contents for a terminator.
    3. We manually add '\r' & '\n' after the payload purely so the wire
       format stays readable/consistence and the server consumes and 
       discard these 2 bytes to stay in sync for the next command. 
    */
    public String set(String key, String value) throws Exception {
        Connection conn = borrow();

        try {
            /*
            getBytes(UTF_8) turns the java spring (UTF-16 internally) into the 
            exact sequence of bytes we're about to send.
            */
            byte[] payload = value.getBytes(StandardCharsets.UTF_8);

            // payload.length -> BYTE count which the server needs
            conn.writer.print("SET " + key + " " + payload.length + "\r\n");
            conn.writer.flush(); // pushing the bytes onto the socket

            conn.rawOut.write(payload);
            conn.rawOut.write('\r');
            conn.rawOut.write('\n');
            conn.rawOut.flush();
            
            String response = readLine(conn.rawIn);
            release(conn);

            return response;
        } catch(IOException e) {
            /*
            Any failure here and we don't really know what state the connection
            is in anymore so rather than risking a possibly corrupted connection
            to the next caller we mark it broken so borrow() reconnects it fresh
            before anyone reuses it
            */
            conn.markBroken();
            release(conn);
            throw e;
        }
    }

    /*
    ===================================================================
    GET <key>\r\n
    ===================================================================
    
    Example: 
    '
     GET project\r\n
     7 raydash\r\n
    '
    Reading GET response is the mirror image of writing a SET's payload.
    The header line ("$7") comes back through our own readLine() helper,
    then we read EXACTLY that many raw bytes for the value with readNBytes().
    If response is "$-1" means no such key and that results in a null.
    */
    public String get(String key) throws Exception {
        Connection conn = borrow();

        try {
            conn.writer.print("GET " + key + "\r\n");
            conn.writer.flush();
            
            String header = readLine(conn.rawIn);
            
            String result;
            if(header == null || header.equals("$-1")) { result = null; }
            else if(header.startsWith("$")) {
                /*
                readNBytes(n) keeps reading until it has n bytes or the stream ends.
                It's a plain InputStream method no wrapping is needed and no line/
                character interpretation.
                */
                Integer length = Integer.parseInt(header.substring(1));
                byte[] payload = conn.rawIn.readNBytes(length);

                if(payload.length != length) { throw new IOException("[Raydash Client] GET, expected: " + length + " bytes, got: " + payload.length); }
                readLine(conn.rawIn); // throw away trailing \r\n
                result = new String(payload, StandardCharsets.UTF_8); 
            }
            else result = header; // defensive fallback nothing else just to avoid a silent swallowing of an unexpected response

            release(conn);
            return result;
        } catch(IOException e) {
            conn.markBroken();
            release(conn);
            throw e;
        }
    }

    /*
    ===================================================================
    DEL,EXPIRE,TTL,EXISTS,FLUSHALL all follow the same shape as SET & GET.
    1. Borrow a connection
    2. Write one command line
    3. Read line back
    4. Release connection
    5. Mark broken on IOException and that's it!
    No raw binary payload or anything crazy just simple text and some numbers. 
    ===================================================================
    */
    public String del(String key) throws Exception {
        Connection conn = borrow();

        try {
            conn.writer.print("DEL " + key + "\r\n");
            conn.writer.flush();
            
            String response = readLine(conn.rawIn);
            release(conn);

            return response;
        } catch(IOException e) {
            conn.markBroken();
            release(conn);
            throw e;
        }
    }

    public String expire(String key, Integer seconds) throws Exception {
        Connection conn = borrow();

        try {
            conn.writer.print("EXPIRE " + key + " " + seconds + "\r\n");
            conn.writer.flush();
            
            String response = readLine(conn.rawIn);
            release(conn);

            return response;
        } catch(IOException e) {
            conn.markBroken();
            release(conn);
            throw e;
        }
    }

    public Integer ttl(String key) throws Exception {
        Connection conn = borrow();

        try {
            conn.writer.print("TTL " + key + "\r\n");
            conn.writer.flush();
            
            String response = readLine(conn.rawIn);
            release(conn);

            // Server TTL replies with ":<seconds>" on success which means ":" denotes an Integer reply
            if(response != null && response.startsWith(":")) {
                return Integer.parseInt(response.substring(1));
            }
            
            return -2; // matches server's "key does not exist" sentinel value
        } catch(IOException e) {
            conn.markBroken();
            release(conn);
            throw e;
        }
    }

    public Integer exists(String key) throws Exception {
        Connection conn = borrow();

        try {
            conn.writer.print("EXISTS " + key + "\r\n");
            conn.writer.flush();
            
            String response = readLine(conn.rawIn);
            release(conn);
            
            return Integer.parseInt(response.substring(1)); // could be either 0/1
        } catch(IOException e) {
            conn.markBroken();
            release(conn);
            throw e;
        }
    }

    public String flushall() throws Exception {
        Connection conn = borrow();

        try {
            conn.writer.print("FLUSHALL\r\n");
            conn.writer.flush();

            String response = readLine(conn.rawIn);
            release(conn);

            return response;
        } catch(IOException e) {
            conn.markBroken();
            release(conn);
            throw e;
        }
    }

    /*
    ===================================================================
    INFO returns in the same:
    <length>\r\n<byte>\r\n
    shape as GET just carrying a small human readable text blob (
    key_count,
    uptime_seconds,
    ops_served,
    active_connections,
    max_connections,
    max_keys
    )
    Instead of a cached value. Deliberately returned here as a raw String.
    ===================================================================
    */
    public String info() throws Exception {
        Connection conn = borrow();

        try {
            conn.writer.print("INFO\r\n");
            conn.writer.flush();

            String header = readLine(conn.rawIn);
            if(header == null || !header.startsWith("$")) {
                throw new IOException("unexpected INFO response: " + header);
            }

            Integer length = Integer.parseInt(header.substring(1));
            byte[] payload = conn.rawIn.readNBytes(length);
            if(payload.length != length) {
                throw new IOException("[Raydash Client] INFO, expected: " + length + " bytes, got: " + payload.length);
            }

            readLine(conn.rawIn);

            String result = new String(payload, StandardCharsets.UTF_8);
            release(conn);

            return result;
        } catch (Exception e) {
            conn.markBroken();
            release(conn);
            throw e;
        }
    }

    /*
    Takes one connection out of the pool for exlusion use by the calling thread.
    thread.poll(timeout, unit) is the blocking-with-a-limit version of "gimme an item".
    If every connection is currently borrowed by other thread, this thread waits up
    to BORROW_TIMEOUT_MS for one to free up rather than waiting forever or failing
    immediately.
    isUsable() is to catch the cases where the connection we got back happens to be the
    one that was marked broken by some other request earlier we repair it here (connectWithRetry)
    rather than making the caller deal with a dead connection.
    */
    private Connection borrow() throws Exception {
        Connection conn = pool.poll(BORROW_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        if(conn == null) {
            throw new Exception("Timed out waiting for an available Raydash connection (pool size: " + poolSize + ")");
        }

        if(!conn.isUsable()) {
            try { connectWithRetry(conn); } 
            catch (Exception e) {
                // put it back anyway so the pool doesn't shrink permanently after a bad patch
                // the next borrower will just retry again
                pool.offer(conn);
                throw e;
            }
        }

        return conn;
    }

    // hands a connection back to the pool so another thread can borrow it
    private void release(Connection conn) { pool.offer(conn); }

    /*
    Tries to (re)connect to a single Connection retrying with exponential backoff 
    (250ms, 500ms, 1s, 2s, 4s, capped) instead of giving up after 1 failed attempt.
    This is what makes the whole client resilient to "raydash container isn't
    accepting connections yet" a real race on deployed to Cloud when the app container
    could start before raydash's does. Without retry that race would fail this bean's
    creation and crash the whole spring app.
    */
    private void connectWithRetry(Connection conn) throws Exception {
        Integer attempts = 0;
        Long backoffMs = 250L;
        Exception lastError = null;

        while(attempts < MAX_CONNECT_ATTEMPTS) {
            try {
                conn.connect(host, port, authToken);
                return;
            } catch(IOException e) {
                lastError = e;
                attempts++;
                Thread.sleep(backoffMs);
                backoffMs = Math.min(backoffMs * 2, 4000);
            }
        }

        throw new Exception("Failed to connect to Raydash at: " + host + ":" + port + " after " + attempts + " attempts", lastError);
    } 

    // called by spring on application shutdown
    // drains the pool and closes every underlying socket cleanly
    @Override 
    public void close() throws Exception {
        Connection conn;
        while((conn = pool.poll()) != null) { conn.close(); }
    }

    /*
    Reads one line, byte by byte, directly off a raw InputStream, stripping the
    trailing \r and \n. Returns null on immediate end-of-stream with nothing read
    yet.
    Earlier we used to have BufferedReader which wraps an InputStreamReader, which 
    DECODES bytes into character as it goes, and reads ahead into it's own internal
    buffer whenever you ask it for anything. Both of these are a problem the moment
    you also need to read a payload as EXACT RAW BYTES on the very same stream.
    The Reader might have already buffered some of those payload bytes internally as 
    part of an earlier read, or it might try to decode them as if they were text.
    Reading everything both the short text lines AND the exact-length payloads 
    through this one raw InputStream avoids that mismatch entirely, at the small
    cost of writing our own 3-line line reader.

    Basically developers often use BufferedReader & InputStreamReader to read lines
    of text in Java. However doing that creates 2 major issues if the stream contains 
    BOTH text and raw binary data like for example, a file upload with a text header
    followed by a raw image payload:
    1. BufferedReader is designed to be fast by reading ahead. If you ask it for one
       line of text it might secretly read 8KB of data into it's internal memory. If
       your raw binary payload starts right after that text line, BufferedReader will
       already have swallowed part of your binary data, making it impossible to read
       correctly from the main stream.
    2. InputStreamReader automatically converts bytes into characters using a text 
       encoding (like in our case UTF-8). If you try to force it to read a raw binary
       payload, it will try to "decode" those bytes as text, which can permanently
       corrupt the binary data.

    By reading directly from the raw InputStream one byte at a time this method ensures
    NOT A SINGLE EXTRA BYTE is consumed beyond text line. Stream is left exactly at the
    starting position of the raw binary payload.
    */
    private static String readLine(InputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        int c = in.read(); // InputStream.read() returns 1 byte as an int (0-255) or -1 at end of stream

        if(c == -1) return null;

        while(c != -1 && c != '\n') {
            if(c != '\r') { sb.append((char) c); } // discard carriage return

            c = in.read();
        }

        return sb.toString();
    }

    /*
    One pooled TCP connection to the raydash server, everything a single "line" to server
    needs: the socket, a writer for outgoing text commands & the raw streams for binary-safe
    payload reads/writes. Marked as "private static final class" because nothing outside of 
    RaydashClient ever needs to interact with a Connection directly and makes it much safer.
    Purely an internal implementation detail of the pool.
    */
    private static final class Connection {
        private Socket socket;
        private PrintWriter writer;
        private OutputStream rawOut;
        private InputStream rawIn;

        /*
        "volatile" keyword in Java is a field modifier used in multithreaded programming to
        ensure memory visibility and instructions ordering across threads. 
        When a variable is marked "volatile", the JVM and CPU are instructed to read and write
        it's value directly from and to the main memory, completely bypassing thread-local
        CPU caches.        
        1. Without volatile a thread might cache a shared variable's value in a CPU register or
           L1/L2 cache. If Thread A updates the variable, Thread B might keep reading the stale
           cached value. Marking it "volatile" ensures that any write is immediately visible to
           all other threads.
        2. The compiler and CPU often rearrange code instructions to optimize performance.
           "volatile" establishes a "happen-before" boundary. A write to "volatile" field
           happens-before every subsequent read of that same field, preventing hazardous 
           instruction reordering around that variable.

        A key note is that "volatile" does not provide atomicity nor does it act as a mutual
        exclusion lock, meaning it cannot make compound operations thread-safe.
        */
       /*
       "volatile" matters here specifically because a Connection object gets handed from thread
       to thread via the pool. Without it one thread calling markBroken() isn't guaranteed to be
       visible to the next thread that borrows this same object and checks isUsable(), since each
       CPU core can otherwise cache it's own stale copy of the field. "volatile" forces every
       read/write to go through main memory.
       */
        private volatile Boolean broken = false;

        /*
        Opens a fresh socket, wires up the streams and if an authToken was configured,
        immediately sends "AUTH token" and requires an "+OK" back before returning successfully.
        If AUTH fails this connection is marked broken right away so it's never handed 
        out as if it were usable.
        */
        void connect(String host, Integer port, String authToken) throws IOException {
            close(); // in case this Connection object is being reconnected, not created fresh

            socket = new Socket();

            // connect(address, timeoutMs) is what gives us a bounded connection attempt instead of OS default which could be a looong time
            socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
            // this applied to READS on this socket from here on any read() call would otherwise block forever now throws SocketTimeoutException (an IOException)
            socket.setSoTimeout(READ_TIMEOUT_MS);

            rawOut = socket.getOutputStream();
            rawIn = new BufferedInputStream(socket.getInputStream()); // buffered so our byte-at-a-time readLine() above isn't one syscall per byte
            /*
            the (OutputStream out, boolean autoFlush, Charset charset) constructor lets us pin
            the encoding explicitly to UTF-8. Without it PrintWriter falls back to the JVM's
            platform-default charset, which can silently differ between machines
            (Windows default is often NOT UTF-8)
            */
            writer = new PrintWriter(rawOut, true, StandardCharsets.UTF_8);
            broken = false;

            if(authToken != null && !authToken.isEmpty()) {
                writer.print("AUTH " + authToken + "\r\n");
                writer.flush();

                String response; 
                try {
                    response = readLine(rawIn);
                } catch(Exception e) {
                    broken = true;
                    throw new IOException("[Raydash Client] AUTH failed: " + e);
                }
                if(response == null || !response.startsWith("+OK")) {
                    broken = true;
                    throw new IOException("[Raydash Client] AUTH failed: " + response);
                }
            }
        }

        /*
        A connection is only safe to hand out if it hasn't been flagged broken AND
        the underlying socket still thinks it's open/connected.
        */
        boolean isUsable() {
            return !broken && socket != null && !socket.isClosed() && socket.isConnected();
        }

        void markBroken() { broken = true; }

        /*
        Best effort cleanup of every layer we opened each wrapped in it's own try/catch 
        so a failure closing one resource does not stop the others from being closed too.
        */
        void close() {
            try { if(rawIn != null) rawIn.close(); } catch (Exception ignored) {}
            try { if(writer != null) writer.close(); } catch (Exception ignored) {}
            try { if(socket != null) socket.close(); } catch (Exception ignored) {}
        }
    }
}
