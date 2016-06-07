package net.deelam.vertx;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.function.Consumer;

import com.hazelcast.config.Config;
import com.hazelcast.config.TcpIpConfig;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StartVertx {
  public static String guessMyIPv4Address(String ipPrefix) {
    try {
      for (Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces(); e.hasMoreElements();) {
        for (Enumeration<InetAddress> ee = e.nextElement().getInetAddresses(); ee.hasMoreElements();) {
          InetAddress i = (InetAddress) ee.nextElement();
          if (i instanceof Inet4Address) { // I only know how to deal with IPv4 
            int lastOctet = i.getAddress()[3] & 0xFF; // convert to unsigned int
            log.debug("ip={} [3]={}", i, lastOctet);
            if (lastOctet > 1) { // if last byte is 0 or 1, don't choose it
              if(ipPrefix==null || i.getHostAddress().startsWith(ipPrefix));
                return i.getHostAddress();
            }
          }
        }
      }
    } catch (SocketException se) {
    }
    return null;
  }

  public static void create(VertxOptions options, boolean isServer, String serverIp, int serverPort,
      Consumer<Vertx> vertxCons) {
    if (options.isClustered()) {
      System.setProperty("hazelcast.phone.home.enabled", "false");
      Config cfg = new Config(); //new XmlConfigBuilder().build(); //new Config();

      { /// configure Hazelcast
        // turn off the multicast
        cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        // turn on the tcp/ip join
        TcpIpConfig tcpconfig = cfg.getNetworkConfig().getJoin().getTcpIpConfig();
        tcpconfig.setEnabled(true).getMembers().clear();

        // declare the interface server and clients should bind to
        if (serverIp == null)
          serverIp = guessMyIPv4Address(null);
        if (serverIp == null)
          throw new RuntimeException("Cannot guess IP; provide the IP address of the server");
        String ipPrefix = serverIp.substring(0, serverIp.lastIndexOf(".")+1);
        String clientSubnet = ipPrefix + "*"; // TODO: 6: dont assume this subnet
        log.info("Using serverIP={} and clientSubnet={}", serverIp, clientSubnet);

        if(options.getClusterHost()==null || options.getClusterHost().equals("localhost")){
          String myIP = guessMyIPv4Address(ipPrefix);
          options.setClusterHost(myIP);
        }
        log.info("This vertx's clusterHost={} (required for cross-host eventbus)", options.getClusterHost());
        
        cfg.getNetworkConfig().getInterfaces().setEnabled(true).clear().addInterface(clientSubnet);
        if (isServer) {
          log.info("Starting hazelcast instance as server");
          //              cfg.setInstanceName( "my-server" );
          cfg.getNetworkConfig().setPort(serverPort);
          cfg.getNetworkConfig().setPortAutoIncrement(false);
          //cfg.getNetworkConfig().getInterfaces().addInterface(serverIp+":"+serverPort);
        } else {
          log.info("Starting hazelcast instance as client");
          //            lsMembers.add(serverIp+":"+port);
          tcpconfig.setRequiredMember(serverIp + ":" + serverPort);
          cfg.getNetworkConfig().setPort(serverPort + 1);
        }
      }
      options.setClusterManager(new HazelcastClusterManager(cfg));
      Vertx.clusteredVertx(options, res -> {
        if (res.succeeded()) {
          Vertx vertx = res.result();
          vertxCons.accept(vertx);
        } else {
          log.error("Could not initialize Vertx", res.cause());
          //          res.cause().printStackTrace();
        }
      });
    } else {
      Vertx vertx = Vertx.vertx();
      vertxCons.accept(vertx);
    }
    
    Runtime.getRuntime().addShutdownHook(new Thread(()->{
      System.out.println("Shutting down... (please wait)");
      try {
        int sec=6;
        Context vertx = Vertx.currentContext();
        if(vertx!=null){
          sec=vertx.getInstanceCount();
        }
        log.info("Sleeping {} seconds to allow Vertx's shutdown hook to finish completely...", sec);
        Thread.sleep(sec*1000);
        log.info("Done waiting for Vertx's shutdown hook");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "vertx-shutdown-hook"));
    
  }
}

