package net.deelam.vertx;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.ignite.configuration.IgniteConfiguration;

import com.google.common.base.Stopwatch;
import com.hazelcast.config.Config;
import com.hazelcast.config.TcpIpConfig;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemException;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;
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
              if(ipPrefix==null || i.getHostAddress().startsWith(ipPrefix))
                return i.getHostAddress();
            }
          }
        }
      }
    } catch (SocketException se) {
    }
    return null;
  }
  
  public enum CLUSTER_MANAGER { HAZELCAST, IGNITE };

  public static void create(VertxOptions options, boolean isServer, String serverIp, int serverPort,
      Consumer<Vertx> vertxCons) {
    create(CLUSTER_MANAGER.IGNITE, options, isServer, serverIp, serverPort, vertxCons);
  }
  public static void create(CLUSTER_MANAGER cm, VertxOptions options, boolean isServer, String serverIp, int serverPort,
      Consumer<Vertx> vertxCons) {
    if (options.isClustered()) {
      Stopwatch sw= Stopwatch.createStarted();
      ClusterManager clusterManager;
      switch(cm){
        case HAZELCAST:
          clusterManager = createHazelcastClusterManager(options, isServer, serverIp, serverPort);
          break;
        case IGNITE:
          clusterManager = createIgniteClusterManager(options, isServer, serverIp, serverPort);
          break;
        default:
          throw new IllegalArgumentException("Unknown: "+cm);
      }
      clusterManagers.add(clusterManager);
      options.setClusterManager(clusterManager);
      Vertx.clusteredVertx(options, res -> {
        if (res.succeeded()) {
          log.info("Clustered Vertx instance created in {}", sw);
          Vertx vertx = res.result();
          vertxList.add(vertx);
          vertxCons.accept(vertx);
        } else {
          log.error("Could not initialize Vertx", res.cause());
          //          res.cause().printStackTrace();
        }
      });
    } else {
      Vertx vertx = Vertx.vertx();
      vertxList.add(vertx);
      vertxCons.accept(vertx);
    }
    
//    Runtime.getRuntime().addShutdownHook(new Thread(()->{
//      shutdown()
//    }, "vertx-shutdown-hook"));
    
    // OSGi workaround to make sure this class is loaded before shutdown() is called when the Vertx bundle may be already removed
    new FileSystemException("");
  }

  private static ClusterManager createIgniteClusterManager(VertxOptions options, boolean isServer, String serverIp,
      int serverPort) {
    IgniteConfiguration cfg=new IgniteConfiguration();
    IgniteClusterManager clusterManager = new IgniteClusterManager(cfg);
    return clusterManager;
  }
  private static HazelcastClusterManager createHazelcastClusterManager(VertxOptions options, boolean isServer,
      String serverIp, int serverPort) {
    //System.setProperty("hazelcast.phone.home.enabled", "false");
    //System.setProperty("hazelcast.local.localAddress", "10.14.120.129");
    Config cfg = new Config(); //new XmlConfigBuilder().build(); //new Config();

    { /// configure Hazelcast
      // turn off the multicast
      cfg.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
      cfg.getNetworkConfig().getJoin().getAwsConfig().setEnabled(false);

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
        log.info("Starting hazelcast instance as client to server={}", serverIp);
        //            lsMembers.add(serverIp+":"+port);
        tcpconfig.setRequiredMember(serverIp + ":" + serverPort);
        cfg.getNetworkConfig().setPort(serverPort + 1); // start client on next port number
        cfg.getNetworkConfig().setPortAutoIncrement(true);
      }
    }
    log.debug("Hazelcast config={}", cfg);
    
    HazelcastClusterManager clusterManager = new HazelcastClusterManager(cfg);
    return clusterManager;
  }
  
  private final static List<Vertx> vertxList = new ArrayList<>();
  private final static List<ClusterManager> clusterManagers = new ArrayList<>();
  
  public static void shutdown() {
    System.out.println("Shutting down Vertx... (please wait)");
    try {
      final CountDownLatch latch=new CountDownLatch(vertxList.size());
      
      Thread closer=new Thread(()->{
        for(ClusterManager m:clusterManagers){
          log.info("Closing clusterManager={}", m);
          //m.getHazelcastInstance().shutdown();
          m.leave(result ->{
            if(result.failed())
              log.error("Could not leave ClusterManager={}", m);
            
            for(Vertx v:vertxList){
              log.info("Closing Vertx={}", v);
              v.close(res->{
                log.info("Vertx closed: {}", v);
                latch.countDown();
              });
            }
          });
        }
        
      }, "shutdown-closing-vertx");
      closer.start();
      
      log.info("Waiting to allow Vertx instances to finish closing...");
      latch.await(3, TimeUnit.SECONDS);
      log.info("Done waiting for Vertx to shutdown");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

