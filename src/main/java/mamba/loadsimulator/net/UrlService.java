package mamba.loadsimulator.net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by sanbing on 10/10/16.
 */
public class UrlService {
    public static final int CONNECT_TIMEOUT = 20000;
    public static final int READ_TIMEOUT = 20000;
    private final String address;
    private HttpURLConnection conn;

    private UrlService(String address) {
        this.address = address;
    }

    /**
     * Returns a new UrlService connected to specified address.
     *
     * @param address
     * @return
     * @throws java.io.IOException
     */
    public static UrlService newConnection(String address) throws IOException {
        UrlService svc = new UrlService(address);
        svc.connect();

        return svc;
    }

    public HttpURLConnection connect() throws IOException {
        URL url = new URL(address);
        conn = (HttpURLConnection) url.openConnection();

        //TODO: make timeouts configurable
        conn.setConnectTimeout(CONNECT_TIMEOUT);
        conn.setReadTimeout(READ_TIMEOUT);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "*/*");

        return conn;
    }

    public String send(String payload) throws IOException {
        if (conn == null)
            throw new IllegalStateException("Cannot use unconnected UrlService");
        write(payload);

        return read();
    }

    private String read() throws IOException {
        StringBuilder response = new StringBuilder();

        BufferedReader br = new BufferedReader(new InputStreamReader(
                conn.getInputStream()));
        String line = null;
        while ((line = br.readLine()) != null) {
            response.append(line);
        }
        br.close();

        return response.toString();
    }

    private void write(String payload) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream(),
                "UTF-8");
        writer.write(payload);
        writer.close();
    }

    public void disconnect() {
        conn.disconnect();
    }
}
