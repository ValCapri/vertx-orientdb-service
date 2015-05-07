package eu.tradegrid.tinkerpop.persistor;

import io.vertx.core.AbstractVerticle;
import io.vertx.serviceproxy.ProxyHelper;

/**
 * Created by lionelschinckus on 7/05/15.
 */
public class TinkerpopServiceVerticle extends AbstractVerticle {
    private TinkerpopService service;

    @Override
    public void start() {

        // Create the service object
        service = TinkerpopService.create(vertx, config());

        // And register it on the event bus against the configured address
        final String address = config().getString("address");
        if (address == null) {
            throw new IllegalStateException("address field must be specified in config for service verticle");
        }

        ProxyHelper.registerService(TinkerpopService.class, vertx, service, address);

        // Start it
        service.start();
    }

    @Override
    public void stop() {
        if (service != null) {
            service.stop();
        }
    }
}
