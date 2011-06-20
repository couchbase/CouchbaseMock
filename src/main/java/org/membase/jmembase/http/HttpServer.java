/**
 *     Copyright 2011 Membase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.membase.jmembase.http;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a <u>super-scaled-down</u> implementation of a HTTP server.
 * You may get the impression of that I'm suffering from the
 * "not invented here", but I don't want to add a huge list of dependencies
 * for stuff I only need a basic functionality from...
 *
 * This HTTP server doesn't implement more functionality than the bare
 * minimum I needed in order to provide the REST interface from membase ;-)
 *
 * @author Trond Norbye
 * @version 1.0
 */
public class HttpServer {

    private final ServerSocketChannel server;
    private final Selector selector;

    public HttpServer(int port) throws IOException {
        server = ServerSocketChannel.open();
        server.configureBlocking(false);
        server.socket().bind(new InetSocketAddress(port));
        selector = Selector.open();
        server.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void close() {
        try {
            selector.close();
        } catch (IOException ex) {
            Logger.getLogger(HttpServer.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            server.close();
        } catch (IOException ex) {
            Logger.getLogger(HttpServer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void serve(HttpRequestHandler requestHandler) {
        while (!Thread.currentThread().isInterrupted() && selector.isOpen()) {
            try {
                // @todo this should be blocking and we should just interrupt
                //       the selector when one of the client behaviors change!
                //       and we should not have to iterate all of them, just
                //       reevaluate the ones that changed...
                selector.select(1000);
                Set<SelectionKey> readyKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = readyKeys.iterator();
                while (iterator.hasNext()) {
                    try {
                        SelectionKey key = iterator.next();
                        iterator.remove();

                        HttpClient client = (HttpClient) key.attachment();
                        if (client == null) {
                            assert key.isAcceptable();
                            SocketChannel cc = server.accept();
                            cc.configureBlocking(false);
                            cc.register(selector, SelectionKey.OP_READ, new HttpClient(requestHandler));
                        } else {
                            assert key.isAcceptable() == false;
                            SocketChannel channel = (SocketChannel) key.channel();
                            boolean shouldClose = false;
                            if (key.isReadable() && channel.read(client.getInputBuffer()) == -1) {
                                shouldClose = true;
                            }
                            if (key.isWritable()) {
                                channel.write(client.getOutputBuffer());
                            }

                            if (shouldClose || !client.driveMachine()) {
                                channel.close();
                            } else {
                                channel.register(selector, client.getSelectionKey(), client);
                            }
                        }
                    } catch (ClosedChannelException exp) {
                        // just ditch this client..
                    } catch (ClosedSelectorException e) {
                        // TODO: This should be logged somewhere
                        System.err.println("Selector closed while polling");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        HttpServer app = null;
        try {
            app = new HttpServer(8080);
            app.serve(new HttpRequestHandler() {

                @Override
                public void handleHttpRequest(HttpRequest request) {
                    request.setReasonCode(HttpReasonCode.Not_Implemented);
                }
            });
        } catch (IOException exp) {
            exp.printStackTrace();
        }
    }
}
