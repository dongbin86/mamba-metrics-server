package mamba.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by sanbing on 10/10/16.
 */
public class Servers {


    private Servers() {
    }


    public static List<InetSocketAddress> parse(String specs, int defaultPort) {
        List<InetSocketAddress> result = new ArrayList<InetSocketAddress>();
        if (specs == null) {
            result.add(new InetSocketAddress("localhost", defaultPort));
        } else {
            String[] specStrings = specs.split("[ ,]+");
            for (String specString : specStrings) {
                result.add(createSocketAddr(specString, defaultPort));
            }
        }
        return result;
    }

    private static InetSocketAddress createSocketAddr(String target, int defaultPort) {
        String helpText = "";
        if (target == null) {
            throw new IllegalArgumentException("Target address cannot be null." + helpText);
        }
        boolean hasScheme = target.contains("://");
        URI uri = null;
        try {
            uri = hasScheme ? URI.create(target) : URI.create("dummyscheme://" + target);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Does not contain a valid host:port authority: " + target + helpText);
        }

        String host = uri.getHost();
        int port = uri.getPort();
        if (port == -1) {
            port = defaultPort;
        }
        String path = uri.getPath();

        if ((host == null) || (port < 0) || (!hasScheme && path != null && !path.isEmpty())) {
            throw new IllegalArgumentException("Does not contain a valid host:port authority: " + target + helpText);
        }
        return createSocketAddrForHost(host, port);
    }


    private static InetSocketAddress createSocketAddrForHost(String host, int port) {
        InetSocketAddress addr;
        try {
            InetAddress iaddr = InetAddress.getByName(host);
            iaddr = InetAddress.getByAddress(host, iaddr.getAddress());
            addr = new InetSocketAddress(iaddr, port);
        } catch (UnknownHostException e) {
            addr = InetSocketAddress.createUnresolved(host, port);
        }
        return addr;
    }
}
