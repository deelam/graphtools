package net.deelam.vertx.pool;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.FileUtils;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.deelam.vertx.VerticleUtils;
import net.deelam.vertx.jobmarket.JobMarket;

/**
 * Distributed resource pool consist of ponds (one for each host).
 * A resource is identified by a URI with syntax scheme://host/path[&version=2]
 * A pond broadcasts to other ponds for a resource (without the host part).
 * [Add an option to register resource to all ponds.]
 * A pond will compare version numbers as integers.
 * 
 * A pond's registrationTable: scheme:///path&version=2 -> serializedMasterLocation(REST), 
 *  sourceHost, originalHost (may be same as sourceHost), localSerializedLocation, resourceTable: {
 *      resourceUri -> fromSerializedMasterLocation, createdDate, checkoutTime, usedBy, usedCount, isBeingUsed
 *    } 
 *    Also add entry: scheme:///path -> same as latest version
 * 
 * A pond verticle is used by an app.  The app REQUESTs scheme:///path[&version=2] via a Vertx message.
 * If pond has an available copy of the resource in resourceTable, it will return the resourceUri.
 * Else if none available, it will deserialize(replicate) a copy and return the resourceUri.
 * Else if pond has it in the registrationTable, it first GETs serialized resource using serializedMasterLocation, 
 *    then creates a deserialized copy and returns the resourceUri.
 *    All these messages will have the original request to cause the chain of actions and return the resourceUri to the requester.
 * Else if pond doesn't know about URI, it first publishes/broadcasts a FIND to all ponds,
 *    which will REGISTER a resource (If multiple respond, take the first response).
 *    [Add option for any pond responds vs. only the original holding pond]
 *    All these messages will have the original request to cause the chain of actions and return the resourceUri to the requester.
 * 
 * 
 * ResourceCreator can ADD a resource to a local pond,
 * which will update the registrationTable and use a ResourceSerializer to create a file at serializedMasterLocation.
 * 
 * For each scheme:
 * - ResourceSerializer uses URI to archive a resource for distribution to other ponds.
 * - ResourceDeserializer unarchives a resource and adds the URI to its resourceTable. 
 * 
 * The pond on the sourceHost will serve up the resource at serializedMasterLocation via REST
 * (https://github.com/vert-x/vertx-examples/blob/master/src/raw/java/sendfile/SendFileExample.java)
 * 
 * app -> CHECKOUT -> requesterPond
 * requesterPond -> FIND -> all ponds
 * srcPond -> REGISTER(serializedMasterLocation) -> requesterPond
 * requesterPond -> GET(via REST) -> srcPond
 * 
 * app -> CHECKIN -> requesterPond
 * 
 * 
 * @author dd
 */
@Slf4j
public class PondVerticle extends AbstractVerticle {

  private static final String SERIALIZED_FILE_REST_URL = "url";
  private static final String EVENTBUS_PREFIX = "net.deelam.pool.";
  private static final String REQUESTER_ADDR = "requesterAddr"; // addr of another pondVerticle
  public static final String CLIENT_ADDR = "clientAddr";
  public static final String RESOURCE_URI = "resourceUri";

  public static void main(String[] args) throws UnknownHostException {
    URI uri = URI.create("schem://hostname/path123?version=2");
    System.out.println("authority=" + uri.getAuthority());
    System.out.println(removeAuthority(uri));

    URI uri2 = URI.create("schem:///path123?version=2");
    System.out.println(removeAuthority(uri2));

    ResourceId ri = new ResourceId(uri);
    ResourceId ri2 = new ResourceId(uri2);
    System.out.println(ri.equals(ri2) + " " + ri.hashCode() + "=?" + ri2.hashCode() + " " + ri);

    System.out.println(InetAddress.getLocalHost().getHostName());
  }

  private static String removeAuthority(URI uri) {
    return uri.getScheme() + "://" + uri.getPath() + "?" + uri.getQuery();
  }

  @Getter
  private final String serviceType;
  private final String pondDir;

  private String host;
  private final int port;

  @Getter
  private String addressBase;

  public PondVerticle(String vrtxServiceName, String pondFolder) {
    this(vrtxServiceName, pondFolder, nextAvailablePort());
  }

  private static int nextAvailablePort() {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    } catch (IOException e) {
      log.error("Cannot find available port", e);
      return 9999;
    }
  }

  public PondVerticle(String vrtxServiceName, String pondFolder, int restPort) {
    this.serviceType = vrtxServiceName;
    pondDir = pondFolder; // "tmp/pond-" + vrtxServiceName;
    File pondDirFile = new File(pondDir);
    if (pondDirFile.exists()) {
      boolean loadPrev=true;
      if(loadPrev){
        loadState();
      }else try {
        log.info("Deleting directory: {}", pondDirFile);
        FileUtils.deleteDirectory(pondDirFile.getAbsoluteFile());
      } catch (IOException e) {
        log.warn("Could not delete directory: " + pondDirFile, e);
      }
    }
    
    if (!pondDirFile.exists() && !pondDirFile.mkdirs())
      log.error("Could not create directory: {}", pondDir);
    //pondDirFile.deleteOnExit(); // only deletes dir if empty
    

    this.port = restPort;
    try {
      host = InetAddress.getLocalHost().getHostName();
      addressBase = host + "." + restPort;
      log.info(this + " is providing serialized resource files {} at {}:{} ", pondDirFile.getAbsolutePath(), host, restPort);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  public enum ADDR {
    ADD, CHECKOUT, CHECKIN, // from client
    FIND, REGISTER, // among ponds
    //RESOURCE_READY // to client
  };

  private Map<ResourceId, ResourceLocation> regisTable = new Hashtable<>();

  private static final String POND_STATE_SER_FILENAME = "pondState.ser";
  private void saveState() {
    log.debug("Saving pond state to {}", pondDir);
    File pondDirFile = new File(pondDir, POND_STATE_SER_FILENAME);
    try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(pondDirFile))){
      oos.writeObject(regisTable);
    } catch (IOException e) {
      log.error("", e);
    }
  }

  private void loadState() {
    log.info("Loading pond state from {}", pondDir);
    File pondDirFile = new File(pondDir, POND_STATE_SER_FILENAME);
    if(pondDirFile.exists()) 
      try(ObjectInputStream objectinputstream = new ObjectInputStream(new FileInputStream(pondDirFile))){
        regisTable = (Hashtable) objectinputstream.readObject();
        log.info("Loaded {} pond resources", regisTable.size());
      } catch (IOException | ClassNotFoundException e) {
        log.error("Resetting pond resource registry", e);
      }
  }
  
  @Override
  public void stop() throws Exception {
    super.stop();
    saveState();
  }

  private ExecutorService threadPool = Executors.newCachedThreadPool();

  @Override
  public void start() throws Exception {
    addressBase = VerticleUtils.getConfig(this, addressBase, JobMarket.EVENT_BUS_PREFIX, deploymentID());

    EventBus eb = vertx.eventBus();
    eb.consumer(addressBase + ADDR.ADD, (Message<String> msg) -> {
      URI origResourceURI = URI.create(msg.body());
      ResourceId resourceId = new ResourceId(origResourceURI);

      if (serializers.get(resourceId.scheme) == null) {
        log.error("Unknown scheme: {}", resourceId.scheme);
      }

      ResourceLocation rsrcLoc = regisTable.get(resourceId);
      boolean notifyOtherPonds = false;
      if (rsrcLoc != null) {
        log.info("Overwriting existing entry: {}", rsrcLoc);
        notifyOtherPonds = true;
        if(rsrcLoc.localSerializedPath!=null){
          File existingFile = new File(rsrcLoc.localSerializedPath);
          if(existingFile.exists()){
            log.info("Moving {} to .old", rsrcLoc.localSerializedPath);
            existingFile.renameTo(new File(rsrcLoc.localSerializedPath + ".old"));
          }
        }
      }

      rsrcLoc = new ResourceLocation(origResourceURI, resourceId, host, host);
      log.info(serviceType + ": Adding resource: {} -> {}", resourceId, rsrcLoc);
      regisTable.put(resourceId, rsrcLoc);

      if (notifyOtherPonds) { // notify other ponds
        asyncInvalidateAtOtherPonds(rsrcLoc);
      }
    });
    eb.consumer(addressBase + ADDR.CHECKOUT, (Message<JsonObject> msg) -> {
      String resrcUriStr = msg.body().getString(RESOURCE_URI);
      checkNotNull(resrcUriStr, msg.body());
      URI rsrcUri = URI.create(resrcUriStr);
      ResourceId rid = new ResourceId(rsrcUri);
      ResourceLocation rsrcLoc = regisTable.get(rid);
      String clientAddr = msg.body().getString(CLIENT_ADDR);
      if (rsrcLoc == null) {//query all ponds
        JsonObject uriWithAppsRequestMsg = new JsonObject()
            .put(CLIENT_ADDR, clientAddr)
            .put(RESOURCE_URI, resrcUriStr)
            .put(REQUESTER_ADDR, addressBase + ADDR.REGISTER);
        log.debug("Broadcast a find for: {}", uriWithAppsRequestMsg);
        eb.publish(EVENTBUS_PREFIX + ADDR.FIND, uriWithAppsRequestMsg);
      } else {
        ResourceLocation.Resource rsrc = getAvailableResource(rsrcLoc);
        if (rsrc == null) {
          log.debug("No available resource, replicating: {}", rsrcLoc);
          if(false)
            replicateResourceAndNotify(rsrcLoc, clientAddr); // potentially takes too long and Vertx complains
          else
            threadPool.execute(() -> replicateResourceAndNotify(rsrcLoc, clientAddr)); 
          // multiple of these threads may clash; added synchronized block in deserialize()
        } else {
          log.debug("Found available resource: {}", rsrc);
          asyncSendResourceReady(clientAddr, rsrc, rsrcLoc);
        }
      }
    });
    eb.consumer(addressBase + ADDR.CHECKIN, (Message<JsonObject> msg) -> {
      String rsrcUriStr = msg.body().getString(RESOURCE_URI);
      checkNotNull(rsrcUriStr, msg.body());
      ResourceLocation rLoc = checkouts.get(rsrcUriStr);
      if(rLoc==null && rsrcUriStr.endsWith("/")){// try removing '/'
        rsrcUriStr=rsrcUriStr.substring(0, rsrcUriStr.length()-1);
        log.warn("Stripping final '/' to get resourceUri={}", rsrcUriStr);
        rLoc = checkouts.get(rsrcUriStr);
      }
      checkNotNull(rLoc, "Could not find resource=" + rsrcUriStr + " in checkouts=" + checkouts);
      URI rsrcUri = URI.create(rsrcUriStr);
      ResourceLocation.Resource rsrc = rLoc.resources.get(rsrcUri);
      if (rsrc == null) {
        log.error("Cannot find resource: {} listed in {}", rsrcUri, rLoc.resources);
      } else {
        rsrc.checkin();
        checkouts.remove(rsrcUriStr);
      }
    });
    eb.consumer(EVENTBUS_PREFIX + ADDR.FIND, (Message<JsonObject> msg) -> {
      String rsrcUriStr = msg.body().getString(RESOURCE_URI);
      checkNotNull(rsrcUriStr, msg.body());
      URI rsrcUri = URI.create(rsrcUriStr);
      ResourceId rid = new ResourceId(rsrcUri);
      ResourceLocation rsrcLoc = regisTable.get(rid);
      if (rsrcLoc != null) { // since have the resource, register at requesterAddr
        String requesterAddr = msg.body().getString(REQUESTER_ADDR);
        asyncSerializeResourceAndRegister(rsrcLoc, requesterAddr, msg.body().getString(CLIENT_ADDR));
      }
    });
    eb.consumer(addressBase + ADDR.REGISTER, (Message<JsonObject> msg) -> {
      String origRsrcUriStr = msg.body().getString(ORIGINAL_URI);
      checkNotNull(origRsrcUriStr, msg.body());
      URI rsrcUri = URI.create(origRsrcUriStr);
      ResourceId resourceId = new ResourceId(rsrcUri);
      log.debug("Registering {}: {}", resourceId, msg.body());
      Boolean invalidateResource = msg.body().getBoolean(INVALIDATE_RESOURCE, false);
      if (regisTable.containsKey(resourceId)) {
        if(invalidateResource){
          log.info(serviceType + ": Invalidating old resource: {}", msg.body());
          regisTable.remove(resourceId);
        }else{
          log.warn(serviceType + ": ResourceId already registered; ignoring msg: {}", msg.body());
        }
      } else {
        if(invalidateResource){
          // don't do anything since it's not in regisTable
        } else {
          ResourceLocation rsrcLoc = new ResourceLocation(URI.create(origRsrcUriStr),
              resourceId, msg.body().getString(SOURCE_HOST), msg.body().getString(ORIGINAL_HOST));
          rsrcLoc.serializedMasterLocationREST = URI.create(msg.body().getString(SERIALIZED_FILE_REST_URL));
  
          log.info(serviceType + ": Adding resource: {} -> {}", resourceId, rsrcLoc);
          regisTable.put(resourceId, rsrcLoc);
  
          String clientAddr = msg.body().getString(CLIENT_ADDR);
          if (clientAddr != null) { // if originally caused by a request from a client, then retrieve a copy for app
            asyncTriggerReponseToClient(rsrcLoc, clientAddr);
          }
        }
      }
    });
    vertx.createHttpServer().requestHandler((HttpServerRequest req) -> {
      //String webroot = pondDir;
      File fileRequested = new File(req.path());
      if (fileRequested.exists()) {
        log.debug("Sending response, file={}", Paths.get(req.path()).normalize().toAbsolutePath());
        req.response().sendFile(fileRequested.getAbsolutePath());
      } else {
        log.warn("File doesn't exist: {}", fileRequested.getAbsolutePath());
        req.response().close();
      }
    }).listen(port);

    log.info("Ready: " + this + " addressPrefix=" + addressBase);
    VerticleUtils.announceServiceType(vertx, serviceType, addressBase);
  }

  private void asyncTriggerReponseToClient(ResourceLocation rsrcLoc, String clientAddr) {
    // GET resource from serializedMasterLocationREST
    HttpClient httpClient = vertx.createHttpClient();
    log.info("Retrieving resource={} from {}", rsrcLoc.resourceId, rsrcLoc.serializedMasterLocationREST);
    httpClient.getNow(rsrcLoc.serializedMasterLocationREST.getPort(),
        rsrcLoc.serializedMasterLocationREST.getHost(),
        rsrcLoc.serializedMasterLocationREST.getPath(), (HttpClientResponse response) -> {

/*      StringBuilder sb = new StringBuilder();
      response.headers().forEach(e -> sb.append("|").append(e.getKey()).append("=").append(e.getValue()));
      log.debug("Response received: headers={}", sb);*/

      if (rsrcLoc.localSerializedPath == null){
        Path localSerializedPath = Paths.get(pondDir,
            rsrcLoc.serializedMasterLocationREST.getHost() + "_" +
                rsrcLoc.serializedMasterLocationREST.getPath().replace('/', '_') +
                "-" + System.currentTimeMillis());
        rsrcLoc.localSerializedPath=localSerializedPath.toString();
      }
      File serFile = new File(rsrcLoc.localSerializedPath);
      if (serFile.exists()) {
        log.warn("Overwriting existing file: {}", serFile);
        if (!serFile.delete())
          throw new RuntimeException("Could not delete existing file=" + serFile);
      }

      response.handler((Buffer buffer) -> {
        byte[] bytes = buffer.getBytes();
        try (FileOutputStream fos = new FileOutputStream(serFile, true)) {
          log.debug("Writing buffer of size={} to file={}", buffer.length(), serFile);
          fos.write(bytes);
        } catch (Exception e) {
          log.error("Could not write to file", e);
          return;
        }
      });
      response.endHandler((Void v) -> {
        if (serFile.exists()) {
          log.debug("File ready: {}", serFile.getAbsolutePath());
          // create a deserialized copy and return the resourceUri.
          replicateResourceAndNotify(rsrcLoc, clientAddr);
        } else {
          log.error("File not retrieved from pond's REST API: {}", rsrcLoc.serializedMasterLocationREST);
        }
      });
    });
  }

  private void asyncSerializeResourceAndRegister(ResourceLocation rsrcLoc, String requesterAddr, String clientAddr) {
    if (rsrcLoc.serializedMasterLocationREST == null) { // not yet serialized
      threadPool.execute(() -> {
        try {
          serializeIfNeeded(rsrcLoc);

          asyncRegisterWithOtherPond(rsrcLoc, requesterAddr, clientAddr);
        } catch (IOException e) {
          log.error("Could not serialize " + rsrcLoc.originalUri, e);
        }
      });
    } else {
      asyncRegisterWithOtherPond(rsrcLoc, requesterAddr, clientAddr);
    }
  }

  public static final String ORIGINAL_URI = "originalUri";
  private static final String SOURCE_HOST = "sourceHost";
  private static final String ORIGINAL_HOST = "originalHost";

  private Set<String> requesterPondsRegisterAddr = new HashSet<>();

  private void asyncRegisterWithOtherPond(ResourceLocation rsrcLoc, String pondRegisterAddr, String clientAddr) {
    JsonObject iHaveResourceMsg = new JsonObject()
        .put(ORIGINAL_URI, rsrcLoc.originalUri.toString())
        .put(SOURCE_HOST, addressBase)
        .put(ORIGINAL_HOST, addressBase)
        .put(SERIALIZED_FILE_REST_URL, rsrcLoc.serializedMasterLocationREST.toString());
    if (clientAddr != null)
      iHaveResourceMsg.put(CLIENT_ADDR, clientAddr);
    requesterPondsRegisterAddr.add(pondRegisterAddr);
    vertx.eventBus().send(pondRegisterAddr, iHaveResourceMsg);
  }

  private static final String INVALIDATE_RESOURCE="invalidateResource";
  private void asyncInvalidateAtOtherPonds(ResourceLocation rsrcLoc) {
    JsonObject invalidateResourceMsg = new JsonObject()
        .put(ORIGINAL_URI, rsrcLoc.originalUri.toString())
        .put(SOURCE_HOST, addressBase)
        .put(ORIGINAL_HOST, addressBase)
        .put(INVALIDATE_RESOURCE, true);
    requesterPondsRegisterAddr.forEach(registerAddr -> {
      vertx.eventBus().send(registerAddr, invalidateResourceMsg);
    });
  }

  private ResourceLocation.Resource getAvailableResource(ResourceLocation rsrcLoc) {
    for (Entry<URI, ResourceLocation.Resource> e : rsrcLoc.resources.entrySet()) {
      if (!e.getValue().isBeingUsed)
        return e.getValue();
    };
    return null;
  }

  private void replicateResourceAndNotify(ResourceLocation rsrcLoc, String clientAddr) {
    log.debug("replicateResourceAndNotify: {}", rsrcLoc);
    try {
      serializeIfNeeded(rsrcLoc);
      ResourceLocation.Resource rsrc = deserialize(rsrcLoc);
      if (rsrcLoc.resources.containsKey(rsrc.uri)) {
        log.error("Should not happen!  Resource URI already exists: {}", rsrc.uri);
      } else
        rsrcLoc.resources.put(rsrc.uri, rsrc);

      asyncSendResourceReady(clientAddr, rsrc, rsrcLoc);
    } catch (IOException e) {
      log.error("Could not serialize", e);
    }
  }

  private Map<String, Serializer> serializers = new HashMap<>();
  private Map<String, Deserializer> deserializers = new HashMap<>();

  @FunctionalInterface
  public static interface Serializer {
    Path apply(URI origUri, String localPondDir);
  }
  @FunctionalInterface
  public static interface Deserializer {
    URI apply(URI origUri, Path localSerializedFile, String localPondDir);
  }

  public void register(String scheme, Serializer serializer, Deserializer deserializer) {
    serializers.put(scheme, serializer);
    deserializers.put(scheme, deserializer);
  }

  private void serializeIfNeeded(ResourceLocation rsrcLoc) throws IOException {
    synchronized (rsrcLoc) {
      if (rsrcLoc.localSerializedPath == null) {

        //    File serializedFile = new File(localPondDir, rsrcLoc.getSerializedFilename());
        //    if (serializedFile.exists()) {
        //      log.warn("Overwriting existing file: {}", serializedFile);
        //      if (!serializedFile.delete())
        //        throw new RuntimeException("Could not delete existing file=" + serializedFile);
        //    }

        Serializer ser = serializers.get(rsrcLoc.resourceId.scheme);
        checkNotNull(ser, "Serializer for " + rsrcLoc.resourceId.scheme + " not registered");
        rsrcLoc.localSerializedPath = ser.apply(rsrcLoc.originalUri, pondDir).toString();
        rsrcLoc.serializedMasterLocationREST =
            URI.create("http://" + host + ":" + port + "/" + rsrcLoc.localSerializedPath);
        
        log.debug("Serialized uri={} to path={} and url={}", rsrcLoc.originalUri, rsrcLoc.localSerializedPath, rsrcLoc.serializedMasterLocationREST);
      }
    }
  }

  private ResourceLocation.Resource deserialize(ResourceLocation rsrcLoc) {
    synchronized(rsrcLoc){ // in case multiple copies are created of the same resource, take turns
      log.debug("Deserializing from file={}", rsrcLoc.localSerializedPath);
      //File newFile = new File("resourceCopy" + (++resourceCounter)+"-"+System.currentTimeMillis());
      // rsrcLoc.localSerializedLocation -> newURI
      Deserializer deser = deserializers.get(rsrcLoc.resourceId.scheme);
      checkNotNull(deser, "Deserializer for " + rsrcLoc.resourceId.scheme + " not registered");
      URI newUri = deser.apply(rsrcLoc.originalUri, Paths.get(rsrcLoc.localSerializedPath), pondDir);
      log.debug("Deserialized path={} to uri={}", rsrcLoc.localSerializedPath, newUri);
      ResourceLocation.Resource rsrc = new ResourceLocation.Resource(newUri);
      return rsrc;
    }
  }

  private void asyncSendResourceReady(String clientAddr, ResourceLocation.Resource rsrc, ResourceLocation rLoc) {
    log.debug("Sending resource to {}: {}", clientAddr, rsrc);
    String uriStr = rsrc.uri.toString();
    rsrc.checkout(clientAddr);
    checkouts.put(uriStr, rLoc);
    DeliveryOptions delivOps=new DeliveryOptions().addHeader(ORIGINAL_URI, rLoc.originalUri.toString());
    vertx.eventBus().send(clientAddr, uriStr, delivOps);
  }

  private Map<String, ResourceLocation> checkouts = new HashMap<>();

  @EqualsAndHashCode(/*exclude = {"uri"}*/)
  @ToString
  private static class ResourceId implements Serializable {
    private static final long serialVersionUID = 201606080845L;
    
    String scheme, path;
    Integer version;

    public ResourceId(URI uri) {
      scheme = uri.getScheme();
      path = uri.getPath();
      version = getVersion(uri);
    }
  }

  public final static String VERSION_PARAM = "version";

  public static Integer getVersion(URI url) {
    if (url.getQuery() == null)
      return null;

    final String[] pairs = url.getQuery().split("&");
    for (String pair : pairs) {
      final int idx = pair.indexOf('=');
      try {
        String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
        if (key.equals(VERSION_PARAM)) {
          final String value =
              idx > 0 && pair.length() > idx + 1 ? URLDecoder.decode(pair.substring(idx + 1), "UTF-8") : null;
          if (value == null)
            return null;
          return Integer.valueOf(value);
        }
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
    return null;
  }


  @RequiredArgsConstructor
  @ToString
  private static class ResourceLocation implements Serializable {
    private static final long serialVersionUID = 201606080846L;
    
    final URI originalUri;
    final ResourceId resourceId;
    final String sourceHost;
    final String originalHost; //(may be same as sourceHost)

    URI serializedMasterLocationREST;
    String localSerializedPath;

    Map<URI, Resource> resources = new Hashtable<>();

    @RequiredArgsConstructor
    @ToString
    static class Resource implements Serializable {
      private static final long serialVersionUID = 201606080847L;
      
      //String fromSerializedMasterLocation;
      final URI uri;
      Date checkoutTime;
      String usedBy;
      Date createdDate = new Date();
      int usedCount = 0;
      boolean isBeingUsed = false;

      public void checkout(String clientAddr) {
        log.debug("Checking out: {}", this);
        isBeingUsed = true;
        usedBy = clientAddr;
        ++usedCount;
        checkoutTime = new Date();
      }

      public void checkin() {
        log.debug("Checking in: {}", this);
        isBeingUsed = false;
        checkoutTime = null;
      }
    }
  }

}
