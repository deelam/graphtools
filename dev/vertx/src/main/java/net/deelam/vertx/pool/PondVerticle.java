package net.deelam.vertx.pool;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.buffer.Buffer;
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
 * requesterPond -> RESOURCE_READY(URI) -> app
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
  public static final String CLIENT_ADDR = "appAddr";
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

  Map<ResourceId, ResourceLocation> regisTable = new Hashtable<>();

  private final String serviceType;
  private final String pondDir;

  private String host;
  private final int port;

  @Getter
  private String addressBase;

  public PondVerticle(String serviceType) {
    this(serviceType, nextAvailablePort());
  }

  private static int nextAvailablePort() {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    } catch (IOException e) {
      log.error("Cannot find available port", e);
      return 9999;
    }
  }

  public PondVerticle(String serviceType, int restPort) {
    this.serviceType = serviceType;
    pondDir = "pond-" + serviceType;
    File pondDirFile = new File(pondDir);
    if (!pondDirFile.exists() && !pondDirFile.mkdir())
      log.error("Could not create directory: {}", pondDir);
    pondDirFile.deleteOnExit();

    this.port = restPort;
    try {
      host = InetAddress.getLocalHost().getHostName();
      addressBase = host + "." + restPort;
      log.info(this + " is providing serialized resource files at {}:{} ", host, restPort);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  public enum ADDR {
    ADD, CHECKOUT, CHECKIN, // from app
    FIND, REGISTER, // among ponds
    RESOURCE_READY // to app
  };

  @Override
  public void start() throws Exception {
    addressBase = VerticleUtils.getConfig(this, addressBase, JobMarket.EVENT_BUS_PREFIX, deploymentID());
    VerticleUtils.announceServiceType(vertx, serviceType, addressBase);

    EventBus eb = vertx.eventBus();
    eb.consumer(addressBase + ADDR.ADD, (Message<String> msg) -> {
      URI origResourceURI = URI.create(msg.body());
      ResourceId resourceId = new ResourceId(origResourceURI);

      if (serializers.get(resourceId.scheme) == null) {
        log.error("Unknown scheme: {}", resourceId.scheme);
      }

      ResourceLocation rsrcLoc = regisTable.get(resourceId);
      if (rsrcLoc != null)
        log.warn("Overwriting existing entry: {}", rsrcLoc);

      rsrcLoc = new ResourceLocation(origResourceURI, resourceId, host, host);
      log.info(serviceType + ": Adding resource: {} -> {}", resourceId, rsrcLoc);
      regisTable.put(resourceId, rsrcLoc);
    });
    eb.consumer(addressBase + ADDR.CHECKOUT, (Message<JsonObject> msg) -> {
      URI rsrcUri = URI.create(msg.body().getString(RESOURCE_URI));
      ResourceId rid = new ResourceId(rsrcUri);
      ResourceLocation rsrcLoc = regisTable.get(rid);
      String appAddr = msg.body().getString(CLIENT_ADDR);
      if (rsrcLoc == null) {//query all ponds
        JsonObject uriWithAppsRequestMsg = new JsonObject()
            .put(CLIENT_ADDR, appAddr)
            .put(RESOURCE_URI, msg.body().getString(RESOURCE_URI))
            .put(REQUESTER_ADDR, addressBase + ADDR.REGISTER);
        log.info("Broadcast a find for: {}", uriWithAppsRequestMsg);
        eb.publish(EVENTBUS_PREFIX + ADDR.FIND, uriWithAppsRequestMsg);
      } else {
        ResourceLocation.Resource rsrc = getAvailableResource(rsrcLoc);
        if (rsrc == null) {
          log.info("No available resource, replicating: {}", rsrcLoc);
          new Thread(() -> replicateResourceAndNotify(rsrcLoc, appAddr)).start();
        } else {
          log.info("Found available resource: {}", rsrc);
          sendResourceReady(appAddr, rsrc, rsrcLoc.originalUri);
        }
      }
    });
    eb.consumer(addressBase + ADDR.CHECKIN, (Message<JsonObject> msg) -> {
      String origUriStr = msg.body().getString(ORIGINAL_URI);
      String rsrcUriStr = msg.body().getString(RESOURCE_URI);
      URI origUri;
      if (origUriStr == null) {
        origUri = checkouts.get(rsrcUriStr);
      } else {
        origUri = URI.create(origUriStr);
      }

      ResourceId rid = new ResourceId(origUri);
      ResourceLocation rLoc = regisTable.get(rid);
      checkNotNull(rLoc);
      URI rsrcUri = URI.create(rsrcUriStr);
      ResourceLocation.Resource rsrc = rLoc.resources.get(rsrcUri);
      if (rsrc == null) {
        log.error("Cannot find resource: {} listed in {}", rsrcUri, rLoc.resources);
      } else {
        rsrc.checkin();
      }
    });
    eb.consumer(EVENTBUS_PREFIX + ADDR.FIND, (Message<JsonObject> msg) -> {
      URI rsrcUri = URI.create(msg.body().getString(RESOURCE_URI));
      ResourceId rid = new ResourceId(rsrcUri);
      ResourceLocation rsrcLoc = regisTable.get(rid);
      if (rsrcLoc != null) { // since have the resource, register at requesterAddr
        String requesterAddr = msg.body().getString(REQUESTER_ADDR);
        asyncSerializeResourceAndRegister(msg.body().getString(RESOURCE_URI), rsrcLoc, requesterAddr,
            msg.body().getString(CLIENT_ADDR));
      }
    });
    eb.consumer(addressBase + ADDR.REGISTER, (Message<JsonObject> msg) -> {
      URI rsrcUri = URI.create(msg.body().getString(RESOURCE_URI));
      ResourceId resourceId = new ResourceId(rsrcUri);
      log.info("Registering {}: {}", resourceId, msg.body());
      if (regisTable.containsKey(resourceId)) {
        log.info(serviceType + ": ResourceId already registered; ignoring REGISTER msg: {}", msg.body());
      } else {
        ResourceLocation rsrcLoc = new ResourceLocation(URI.create(msg.body().getString(ORIGINAL_URI)),
            resourceId, msg.body().getString(SOURCE_HOST), msg.body().getString(ORIGINAL_HOST));
        rsrcLoc.serializedMasterLocationREST = URI.create(msg.body().getString(SERIALIZED_FILE_REST_URL));

        log.info(serviceType + ": Adding resource: {} -> {}", resourceId, rsrcLoc);
        regisTable.put(resourceId, rsrcLoc);

        String appAddr = msg.body().getString(CLIENT_ADDR);
        if (appAddr != null) { // if originally caused by a request from an app, then retrieve a copy for app
          // GET resource from serializedMasterLocationREST
          HttpClient httpClient = vertx.createHttpClient();
          log.info("Retrieving resource={} from {}", resourceId, rsrcLoc.serializedMasterLocationREST);
          httpClient.getNow(rsrcLoc.serializedMasterLocationREST.getPort(),
              rsrcLoc.serializedMasterLocationREST.getHost(),
              rsrcLoc.serializedMasterLocationREST.getPath(), (HttpClientResponse response) -> {

            StringBuilder sb = new StringBuilder();
            response.headers().forEach(e -> sb.append("|").append(e.getKey()).append("=").append(e.getValue()));
            log.info("Response received: headers={}", sb);

            if (rsrcLoc.localSerializedPath == null)
              rsrcLoc.localSerializedPath = Paths.get(pondDir,
                  rsrcLoc.serializedMasterLocationREST.getHost() + "_" +
                      rsrcLoc.serializedMasterLocationREST.getPath().replace('/', '_') +
                      "-" + System.currentTimeMillis());
            File serFile = rsrcLoc.localSerializedPath.toFile();
            if (serFile.exists()) {
              log.warn("Overwriting existing file: {}", serFile);
              if (!serFile.delete())
                throw new RuntimeException("Could not delete existing file=" + serFile);
            }

            response.handler((Buffer buffer) -> {
              log.info("Response (" + buffer.length() + ")");
              byte[] bytes = buffer.getBytes();
              try (FileOutputStream fos = new FileOutputStream(serFile, true)) {
                log.info("Writing to file={}", serFile.getAbsolutePath());
                fos.write(bytes);
              } catch (Exception e) {
                log.error("Could not write to file", e);
                return;
              }
            });
            response.endHandler((Void v) -> {
              if (serFile.exists()) {
                log.info("File ready: {}", serFile.getAbsolutePath());
                // create a deserialized copy and return the resourceUri.
                replicateResourceAndNotify(rsrcLoc, appAddr);
              } else {
                log.error("File not retrieved from pond's REST API: {}", rsrcLoc.serializedMasterLocationREST);
              }
            });
          });
        }
      }
    });
    vertx.createHttpServer().requestHandler((HttpServerRequest req) -> {
      String webroot = "./";
      if (new File(webroot + req.path()).exists()) {
        log.info("Sending response, file={}", Paths.get(webroot, req.path()).normalize().toAbsolutePath());
        req.response().sendFile(webroot + req.path());
      } else {
        log.warn("File doesn't exist: {}", webroot + req.path());
        req.response().close();
      }
    }).listen(port);

    log.info("Ready: " + this + " addressPrefix=" + addressBase);
  }

  private void asyncSerializeResourceAndRegister(String rsrcUri, ResourceLocation rsrcLoc,
      String requesterAddr, String appAddr) {
    if (rsrcLoc.serializedMasterLocationREST == null) { // not yet serialized
      new Thread(() -> {
        try {
          serializeIfNeeded(rsrcLoc);
          rsrcLoc.serializedMasterLocationREST =
              URI.create("http://" + host + ":" + port + "/" + rsrcLoc.localSerializedPath);

          sendRestGetUrl(rsrcUri, rsrcLoc, requesterAddr, appAddr);
        } catch (IOException e) {
          log.error("Could not serialize " + rsrcLoc.originalUri, e);
        }
      }).start();
    } else {
      sendRestGetUrl(rsrcUri, rsrcLoc, requesterAddr, appAddr);
    }
  }

  public static final String ORIGINAL_URI = "originalUri";
  private static final String SOURCE_HOST = "sourceHost";
  private static final String ORIGINAL_HOST = "originalHost";

  private void sendRestGetUrl(String rsrcUri, ResourceLocation rsrcLoc, String requesterAddr, String appAddr) {
    JsonObject iHaveResourceMsg = new JsonObject()
        .put(ORIGINAL_URI, rsrcLoc.originalUri.toString())
        .put(RESOURCE_URI, rsrcUri)
        .put(SOURCE_HOST, addressBase)
        .put(ORIGINAL_HOST, addressBase)
        .put(SERIALIZED_FILE_REST_URL, rsrcLoc.serializedMasterLocationREST.toString())
        .put(CLIENT_ADDR, appAddr);
    vertx.eventBus().send(requesterAddr, iHaveResourceMsg);
  }

  private ResourceLocation.Resource getAvailableResource(ResourceLocation rsrcLoc) {
    for (Entry<URI, ResourceLocation.Resource> e : rsrcLoc.resources.entrySet()) {
      if (!e.getValue().isBeingUsed)
        return e.getValue();
    };
    return null;
  }

  private void replicateResourceAndNotify(ResourceLocation rsrcLoc, String appAddr) {
    log.info("replicateResourceAndNotify: {}", rsrcLoc);
    try {
      serializeIfNeeded(rsrcLoc);
      ResourceLocation.Resource rsrc = deserialize(rsrcLoc);
      if (rsrcLoc.resources.containsKey(rsrc.uri)) {
        log.error("Should not happen!  Resource URI already exists: {}", rsrc.uri);
      } else
        rsrcLoc.resources.put(rsrc.uri, rsrc);

      sendResourceReady(appAddr, rsrc, rsrcLoc.originalUri);
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
        rsrcLoc.localSerializedPath = ser.apply(rsrcLoc.originalUri, pondDir);
        log.info("Serialized uri={} to path={}", rsrcLoc.originalUri, rsrcLoc.localSerializedPath);
      }
    }
  }

  private ResourceLocation.Resource deserialize(ResourceLocation rsrcLoc) {
    log.info("Deserialize from file={}", rsrcLoc.localSerializedPath);
    //File newFile = new File("resourceCopy" + (++resourceCounter)+"-"+System.currentTimeMillis());
    // rsrcLoc.localSerializedLocation -> newURI
    Deserializer deser = deserializers.get(rsrcLoc.resourceId.scheme);
    checkNotNull(deser, "Deserializer for " + rsrcLoc.resourceId.scheme + " not registered");
    URI newUri = deser.apply(rsrcLoc.originalUri, rsrcLoc.localSerializedPath, pondDir);
    log.info("Deserialized path={} to uri={}", rsrcLoc.localSerializedPath, newUri);
    ResourceLocation.Resource rsrc = new ResourceLocation.Resource(newUri);
    return rsrc;
  }

  private void sendResourceReady(String appAddr, ResourceLocation.Resource rsrc, URI originalUri) {
    log.info("Sending resource to {}: {}", appAddr, rsrc);
    String uriStr = rsrc.uri.toString();
    rsrc.checkout(appAddr);
    checkouts.put(uriStr, originalUri);
    vertx.eventBus().send(appAddr, uriStr);
  }

  private Map<String, URI> checkouts = new HashMap<>();

  @EqualsAndHashCode(/*exclude = {"uri"}*/)
  @ToString
  private static class ResourceId {
    //URI uri;
    String scheme, path;
    Integer version;

    public ResourceId(URI uri) {
      scheme = uri.getScheme();
      path = uri.getPath();
      version = getVersion(uri);
    }

    private static Integer getVersion(URI url) {
      if (url.getQuery() == null)
        return null;

      final String[] pairs = url.getQuery().split("&");
      for (String pair : pairs) {
        final int idx = pair.indexOf("=");
        try {
          String key = idx > 0 ? URLDecoder.decode(pair.substring(0, idx), "UTF-8") : pair;
          if (key.equals("version")) {
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
  }

  @RequiredArgsConstructor
  @ToString
  private static class ResourceLocation {
    final URI originalUri;
    final ResourceId resourceId;
    final String sourceHost;
    final String originalHost; //(may be same as sourceHost)

    URI serializedMasterLocationREST;
    Path localSerializedPath;

    Map<URI, Resource> resources = new Hashtable<>();

    @RequiredArgsConstructor
    @ToString
    static class Resource {
      //String fromSerializedMasterLocation;
      final URI uri;
      Date checkoutTime;
      String usedBy;
      Date createdDate = new Date();
      int usedCount = 0;
      boolean isBeingUsed = false;

      public void checkout(String appAddr) {
        log.info("Checking out: {}", this);
        isBeingUsed = true;
        usedBy = appAddr;
        ++usedCount;
        checkoutTime = new Date();
      }

      public void checkin() {
        log.info("Checking in: {}", this);
        isBeingUsed = false;
        checkoutTime = null;
      }
    }
  }

}
