package net.deelam.vertx;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.hazelcast.config.Config;

import io.vertx.core.*;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
/*
VertxOptions options = new VertxOptions(); //.setClustered(true);

Vertx vertx = VerticleUtils.initVertx(options, runner, jc, prod, cons);
try {
  log.info("Allowing time for completion...");
  Thread.sleep(3000);
} catch (InterruptedException e) {
  e.printStackTrace();
}
log.info("Closing Vertx");
vertx.close();
*/
public final class VerticleUtils {

  public static Vertx initVertx(VertxOptions options, Consumer<Vertx> deployer) {
    if (options.isClustered()) {
      Config hazelcastConfig = new Config();
      // Now set some stuff on the config (omitted)
      ClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);
      options.setClusterManager(mgr);

      Vertx.clusteredVertx(options, res -> {
        if (res.succeeded()) {
          Vertx vertx = res.result();
          deployer.accept(vertx);
        } else {
          res.cause().printStackTrace();
        }
      });
      return null;
    } else {
      Vertx vertx = Vertx.vertx(options);
      deployer.accept(vertx);
      return vertx;
    }
  }

  public static Vertx initVertx(VertxOptions options, Verticle verticle,
      DeploymentOptions deploymentOptions, Handler<AsyncResult<String>> completionHandler) {
    Consumer<Vertx> deployer = vertx -> {
      try {
        if (deploymentOptions != null) {
          vertx.deployVerticle(verticle, deploymentOptions, completionHandler);
        } else {
          vertx.deployVerticle(verticle, completionHandler);
        }
      } catch (Throwable t) {
        t.printStackTrace();
        throw t;
      }
    };
    return initVertx(options, deployer);
  }

  public static Vertx initVertx(VertxOptions options, Consumer<Vertx> runner,  Verticle... verticles) {
    Consumer<Vertx> deployer = vertx -> {
      try {
        final AtomicInteger count=new AtomicInteger(verticles.length);
        for (Verticle v : verticles) {
          vertx.deployVerticle(v, res -> {
            if(count.decrementAndGet()==0)
              runner.accept(vertx);
          });
        }
      } catch (Throwable t) {
        t.printStackTrace();
        throw t;
      }
    };
    return initVertx(options, deployer);
  }

}

