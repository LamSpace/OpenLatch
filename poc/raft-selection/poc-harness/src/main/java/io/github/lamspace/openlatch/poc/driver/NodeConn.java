package io.github.lamspace.openlatch.poc.driver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * 节点行协议连接（driver 侧）。命令-响应严格一行对一行。
 */
public final class NodeConn implements AutoCloseable {

    private final String host;
    private final int port;
    private Socket socket;
    private BufferedReader in;
    private Writer out;

    /**
     * 建立连接。
     *
     * @param host 节点地址
     * @param port 行协议端口
     */
    public NodeConn(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        reconnect();
    }

    /** 断开并重建连接（NOT_LEADER 重定向路径复用）。 */
    public void reconnect() throws IOException {
        close();
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), 5_000);
        socket.setTcpNoDelay(true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        out = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
    }

    /** 发送一行命令并读取一行响应。 */
    public synchronized String request(String line) throws IOException {
        out.write(line);
        out.write('\n');
        out.flush();
        String r = in.readLine();
        if (r == null) {
            throw new IOException("conn closed by node " + host + ":" + port);
        }
        return r;
    }

    /** 仅发送（pipeline 模式，响应由 {@link #readLine()} 回收）。 */
    public synchronized void send(String line) throws IOException {
        out.write(line);
        out.write('\n');
        out.flush();
    }

    /** 读取一行响应。 */
    public String readLine() throws IOException {
        return in.readLine();
    }

    @Override
    public void close() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // best effort
        }
    }
}
