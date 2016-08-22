package net.deelam.vertx;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import com.google.common.base.Stopwatch;
import com.hazelcast.config.Config;
import com.hazelcast.config.TcpIpConfig;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemException;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;
import lombok.Builder;
import lombok.ToString;
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

  public static void create(VertxOptions options, boolean isServer, Consumer<Vertx> vertxCons) {
    create(CLUSTER_MANAGER.IGNITE, options, isServer, null, 0, vertxCons);
  }
  
  public static void create(VertxOptions options, boolean isServer, String serverIp, int serverPort,
      Consumer<Vertx> vertxCons) {
    create(CLUSTER_MANAGER.IGNITE, options, isServer, serverIp, serverPort, vertxCons);
  }
  public static void create(CLUSTER_MANAGER cm, VertxOptions options, boolean isServer, String serverIp, int serverPort,
      Consumer<Vertx> vertxCons) {
    if (options.isClustered()) {
      Stopwatch sw= Stopwatch.createStarted();
      // declare the interface server and clients should bind to
      IpInfo ipInfo=IpInfo.builder().isServer(isServer)
          .serverIp(serverIp).serverPort(serverPort).build().infer();
      
      ClusterManager clusterManager;
      switch(cm){
        case HAZELCAST:
          clusterManager = createHazelcastClusterManager(options, ipInfo);
          break;
        case IGNITE:
          clusterManager = createIgniteClusterManager(options, ipInfo);
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
  
  @Builder
  @ToString
  static class IpInfo{
    String myIp;
    String serverIp;
    int serverPort=54321;
    int portRangeSize=100;
    String ipPrefix;
    boolean isServer;
    
    IpInfo infer(){
      // set defaults
      if(serverPort<=0)
        serverPort=54321;
      if(portRangeSize==0)
        portRangeSize=100;
        
      if(isServer){
        if (serverIp == null)
          serverIp = guessMyIPv4Address(null);
        if (serverIp == null)
          throw new RuntimeException("Cannot guess IP; provide the IP address of the Vertx cluster server");
        myIp=serverIp;
      } else {
        if(serverIp==null){
          myIp = guessMyIPv4Address(null);
        } else {
          String ipPrefix = serverIp.substring(0, serverIp.lastIndexOf(".")+1);
          myIp = guessMyIPv4Address(ipPrefix);
        }
      }      
      ipPrefix = myIp.substring(0, myIp.lastIndexOf(".")+1);
      return this;
    }

    public int getEndServerPort() {
      return Math.min(serverPort+portRangeSize, 65535);
    }

  }

  private static ClusterManager createIgniteClusterManager(VertxOptions options, IpInfo ipInfo) {
    IgniteConfiguration cfg=new IgniteConfiguration();
    TcpDiscoverySpi spi=new TcpDiscoverySpi();
    TcpDiscoveryMulticastIpFinder ipFinder=new TcpDiscoveryMulticastIpFinder();
    //ipFinder.setMulticastGroup(?); // http://www.tcpipguide.com/free/t_IPMulticastAddressing.htm
    
    if(ipInfo.isServer){
      cfg.setLocalHost(ipInfo.myIp);
    }
    
    String clientSubnet = ipInfo.ipPrefix + "0..255"; // TODO: 6: dont assume this subnet
    Collection<String> initIpAddr=Arrays.asList(clientSubnet+":"+ipInfo.serverPort+".."+ipInfo.getEndServerPort());
    log.info("Using {} and initIpAddr={}", ipInfo, initIpAddr);
    ipFinder.setAddresses(initIpAddr);
    spi.setIpFinder(ipFinder);
    cfg.setDiscoverySpi(spi);
    cfg.setClientMode(!ipInfo.isServer);
    IgniteClusterManager clusterManager = new IgniteClusterManager(cfg);
    return clusterManager;
  }
  private static HazelcastClusterManager createHazelcastClusterManager(VertxOptions options, IpInfo ipInfo) {
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

      String clientSubnet = ipInfo.ipPrefix + "*"; // TODO: 6: dont assume this subnet
      log.info("Using {} and clientSubnet={}", ipInfo, clientSubnet);

      if(options.getClusterHost()==null || options.getClusterHost().equals("localhost")){
        options.setClusterHost(ipInfo.myIp);
      }
      log.info("This vertx's clusterHost={} (required for cross-host eventbus)", options.getClusterHost());
      
      cfg.getNetworkConfig().getInterfaces().setEnabled(true).clear().addInterface(clientSubnet);
      if (ipInfo.isServer) {
        log.info("Starting hazelcast instance as server");
        //              cfg.setInstanceName( "my-server" );
        cfg.getNetworkConfig().setPort(ipInfo.serverPort);
        cfg.getNetworkConfig().setPortAutoIncrement(false);
        //cfg.getNetworkConfig().getInterfaces().addInterface(serverIp+":"+serverPort);
      } else {
        log.info("Starting hazelcast instance as client to server={}", ipInfo.serverIp);
        //            lsMembers.add(serverIp+":"+port);
        tcpconfig.setRequiredMember(ipInfo.serverIp + ":" + ipInfo.serverPort);
        cfg.getNetworkConfig().setPort(ipInfo.serverPort + 1); // start client on next port number
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

