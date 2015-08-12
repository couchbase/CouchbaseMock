package org.couchbase.mock.httpio;

import org.apache.http.*;
import org.apache.http.impl.DefaultBHttpServerConnectionFactory;
import org.apache.http.protocol.*;
import org.apache.http.util.VersionInfo;
import org.couchbase.mock.Info;

import java.io.IOException;
import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.util.HashSet;
import java.util.Set;

public class HttpServer extends Thread {
    /**
     * Subclass of HttpService which adds some additional hooks to all responses
     */
    static class MyHttpService extends HttpService {
        MyHttpService(HttpProcessor proc, UriHttpRequestHandlerMapper registry) {
            super(proc, registry);
        }

        @Override
        protected void doService(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
            response.addHeader(HttpHeaders.CACHE_CONTROL, "must-revalidate"); // Disable caching
            // Normalize the URI
            super.doService(request, response, context);
        }
    }

    /**
     * Subclass of UriHttpRequestHandlerMapper which normalizes a URI and matches only its path.
     * This is because we ignore the host part (we don't use the {@code Host} header)
     */
    static class MyRequestHandlerMapper extends UriHttpRequestHandlerMapper {
        @Override
        protected String getRequestPath(final HttpRequest request) {
            String s = request.getRequestLine().getUri();
            try {
                URI uri = new URI(s);
                return uri.getPath();
            } catch (URISyntaxException ex) {
                return s;
            } catch (IllegalArgumentException ex) {
                return s;
            }
        }
    }

    private volatile boolean shouldRun = true;
    private final DefaultBHttpServerConnectionFactory connectionFactory;
    private final HttpService httpService;
    private final UriHttpRequestHandlerMapper registry;
    private final Set<Worker> allWorkers = new HashSet<Worker>();
    private static final String serverString = String.format("CouchbaseMock/%s (mcd; views) httpcomponents/%s",
            Info.getVersion(), VersionInfo.loadVersionInfo("org.apache.http", null).getRelease());

    private ServerSocketChannel listener;

    final public static String CX_SOCKET = "couchbase.mock.http.socket";
    final public static String CX_AUTH = "couchbase.mock.http.auth";

    /**
     * Creates a new server. To make the server respond to requests, invoke
     * the {@link #bind(java.net.InetSocketAddress)} method to make it use a socket,
     * and then invoke the {@link #start()} method to start it up in the background.
     *
     * Use {@link #register(String, org.apache.http.protocol.HttpRequestHandler)} to add
     * handlers which respond to various URL paths
     */
    public HttpServer() {
        this.connectionFactory = new DefaultBHttpServerConnectionFactory();
        this.registry = new MyRequestHandlerMapper();

        HttpProcessor httpProcessor = HttpProcessorBuilder.create()
                .add(new ResponseServer(serverString))
                .add(new ResponseContent())
                .add(new ResponseConnControl())
                .build();

        this.httpService = new MyHttpService(httpProcessor, registry);
        // Register the unknown handler
        register("*", new HttpRequestHandler() {
            @Override
            public void handle(HttpRequest request, HttpResponse response, HttpContext context) throws HttpException, IOException {
                response.setStatusCode(HttpStatus.SC_NOT_FOUND);
            }
        });
    }

    /**
     * Set the server's listening address
     * @param address The address the server should listen on
     * @throws IOException if a new socket could not be created
     * @see {@link #bind(java.nio.channels.ServerSocketChannel)}
     */
    public void bind(InetSocketAddress address) throws IOException {
        if (listener != null) {
            listener.close();
            listener = null;
        }
        listener = ServerSocketChannel.open();
        listener.socket().bind(address);
    }

    /**
     * Set the server's listening socket.
     * @param newSock An existing listening socket.
     * @see {@link #bind(java.net.InetSocketAddress)}
     */
    public void bind(ServerSocketChannel newSock) {
        listener = newSock;
    }

    /**
     * Register a path with a handler
     * @param pattern The path to register
     * @param handler The handler to handle the path
     * @see {@link org.apache.http.protocol.UriHttpRequestHandlerMapper}
     */
    public void register(String pattern, HttpRequestHandler handler) {
        registry.register(pattern, handler);
        registry.register(pattern + "/", handler);
    }

    /**
     * Unregister a given path. Further requests to paths matching the specified
     * pattern will result in a 404 being delivered to the client
     * @param pattern The pattern to unregister. Must have previously been registered
     *                via {@link #register(String, org.apache.http.protocol.HttpRequestHandler)}
     */
    public void unregister(String pattern) {
        registry.unregister(pattern);
        registry.unregister(pattern + "/");
    }


    class Worker extends Thread {
        final HttpServerConnection htConn;
        final Socket rawSocket;
        private volatile boolean closeRequested = false;

        Worker(HttpServerConnection htConn, Socket rawSocket) {
            this.htConn = htConn;
            this.rawSocket = rawSocket;
            setName("Mock Http Worker: " + rawSocket.getRemoteSocketAddress());
        }

        void stopSocket() {
            closeRequested = true;
            try {
                this.rawSocket.close();
            } catch (IOException ex) {
                //
            }
        }

        private void bail() {
            this.stopSocket();
        }

        public void doReadLoop() {
            HttpContext context = new BasicHttpContext();
            context.setAttribute(CX_SOCKET, rawSocket);

            while (!Thread.interrupted() && this.htConn.isOpen() && HttpServer.this.shouldRun) {
                // Clear the context from any auth settings; since this is done
                // anew on each connection..
                context.removeAttribute(CX_AUTH);

                try {
                    HttpServer.this.httpService.handleRequest(htConn, context);
                } catch (ConnectionClosedException ex_closed) {
                    break;
                } catch (IOException ex) {
                    if (!closeRequested) {
                        ex.printStackTrace();
                    }
                    break;
                } catch (HttpException ex) {
                    ex.printStackTrace();
                    break;
                } catch (ResponseHandledException ex) {
                    break;
                }
            }
            bail();
        }

        @Override
        public void run() {
            try {
                doReadLoop();
            } finally {
                synchronized (HttpServer.this.allWorkers) {
                    HttpServer.this.allWorkers.remove(this);
                }
                bail();
            }
        }
    }

    @Override
    public void run() {
        setName("Mock HTTP Listener: "+listener.socket().getInetAddress());
        while (shouldRun) {
            Socket incoming;
            try {
                incoming = listener.accept().socket();
                HttpServerConnection conn = connectionFactory.createConnection(incoming);
                Worker worker = new Worker(conn, incoming);

                synchronized (allWorkers) {
                    allWorkers.add(worker);
                }
                worker.start();

            } catch (IOException ex) {
                if (shouldRun) {
                    ex.printStackTrace();
                }
            }
        }
    }

    /**
     * Shut down the HTTP server and all its workers, and close the listener socket.
     */
    public void stopServer() {
        shouldRun = false;
        try {
            listener.close();
        } catch (IOException ex) {
            // Don't care
        }
        while (true) {
            synchronized (allWorkers) {
                if (allWorkers.isEmpty()) {
                    break;
                }
                for (Worker w : allWorkers) {
                    w.stopSocket();
                    w.interrupt();
                }
            }
        }

        try {
            listener.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
