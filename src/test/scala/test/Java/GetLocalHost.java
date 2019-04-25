package test.Java;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class GetLocalHost {

    public static void main (String[] ags) throws UnknownHostException {
        InetAddress address = InetAddress.getLocalHost();
        System.out.println(address.getHostAddress());
        System.out.println(address.getAddress());
        System.out.println(address.getCanonicalHostName());
        System.out.println(address.getHostName());
    }

}
