package company.vk.edu.distrib.compute.rybolovlevalexey.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.rybolovlevalexey.urlshortener.RybolovlevAlexeyPersistentDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serial;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RybAlexeyKv implements KVService {
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String LINK_ENTITY_V0 = "/v0/entity";
    private static final String LINK_STATUS_V0 = "/v0/status";
    private static final String LINK_ENTITY_REPLICA_V0 = "/v0/entity/replica";

    private static final int MAX_BODY_SIZE = 1024 * 1024;
    private static final int READ_BUFFER_SIZE = 8192;

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final ExecutorService executor;
    private static final Logger log = LoggerFactory.getLogger(RybAlexeyKv.class);
    private final RybAlexeyRendezvousHash<Integer> hash = new RybAlexeyRendezvousHash<>();
    private final int port;
    private final HttpClient client;
    private boolean hashInitialized;

    public RybAlexeyKv(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.port = port;
        this.client = HttpClient.newHttpClient();

        final var threadPoolSize = 8;
        this.executor = Executors.newFixedThreadPool(threadPoolSize);
        this.server.setExecutor(this.executor);

        final Path dataDir = Path.of(System.getProperty("java.io.tmpdir"), "rybolovlevalexey-kv-data");
        this.dao = RybolovlevAlexeyPersistentDao.bytesBased(dataDir.resolve("entity-" + port + ".properties"));

        server.createContext(LINK_STATUS_V0, new ErrorHandler(statusHandler()));
        server.createContext(LINK_ENTITY_V0, new ErrorHandler(entityHandler()));
        server.createContext(LINK_ENTITY_REPLICA_V0, new ErrorHandler(replicaEntityHandler()));
    }

    private HttpHandler statusHandler() {
        log.info("Creating of statusHandler");
        return httpExchange -> {
            log.info("Received request /v0/status");
            final var requestMethod = httpExchange.getRequestMethod();
            if (METHOD_GET.equals(requestMethod)) {
                httpExchange.sendResponseHeaders(200, -1);
            } else {
                httpExchange.sendResponseHeaders(405, -1);
            }
            httpExchange.close();
        };
    }

    private HttpHandler entityHandler() {
        log.info("Creating of entityHandler");
        return httpExchange -> {
            final var clusterPorts = clusterPorts();

            final var path = httpExchange.getRequestURI().getPath();
            final String uriStr = httpExchange.getRequestURI().toString();
            var entityParams = parseEntityHandlerParams(uriStr, clusterPorts.size());
            if (entityParams.badRequest) {
                httpExchange.sendResponseHeaders(400, -1);
                httpExchange.close();
                return;
            }

            var entityID = entityParams.entityID;
            var ack = entityParams.ack;
            var method = httpExchange.getRequestMethod();
            log.info("Received request at {} with method {} and params: entityID {}, ack {}",
                    path, method, entityID, ack);

            switch (method) {
                case METHOD_GET:
                    getEntityHandler(httpExchange, entityID, ack);
                    break;
                case METHOD_PUT:
                    try {
                        byte[] body = readBodyWithLimit(httpExchange);
                        putEntityHandler(httpExchange, entityID, body, ack);
                    } catch (BodyTooLargeException e) {
                        httpExchange.sendResponseHeaders(400, -1);
                        log.info("PUT for entityID {} rejected: body too large", entityID);
                        httpExchange.close();
                    }
                    break;
                case METHOD_DELETE:
                    deleteEntityHandler(httpExchange, entityID, ack);
                    break;
                default:
                    httpExchange.sendResponseHeaders(405, -1);
                    httpExchange.close();
                    break;
            }
        };
    }

    private EntityParams parseEntityHandlerParams(String uriStr, int clusterNodesCount) {
        String entityID = "";
        String ackStr = "";

        try {
            var pathParams = uriStr.split("\\?")[1].split("&");
            for (String pathParam : pathParams) {
                var keyValue = pathParam.split("=");
                if (!(keyValue.length == 2 || keyValue.length == 1 && "ack".equals(keyValue[0]))) {
                    return new EntityParams("", -1, true);
                }

                switch (keyValue[0]) {
                    case "id":
                        entityID = keyValue[1];
                        break;
                    case "ack":
                        ackStr = keyValue[1];
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            return new EntityParams("", -1, true);
        }

        if (entityID.isEmpty()) {
            return new EntityParams("", -1, true);
        }

        int ack = 1;
        if (ackStr.isEmpty()){
            return new EntityParams(entityID, ack, false);
        }
        try {
            ack = Integer.parseInt(ackStr);
        } catch (NumberFormatException e) {
            return new EntityParams("", -1, true);
        }
        if (ack < 1 || ack > clusterNodesCount) {
            return new EntityParams("", -1, true);
        }
        return new EntityParams(entityID, ack, false);
    }

    private record EntityParams(String entityID, int ack, boolean badRequest) {}

    private byte[] readBodyWithLimit(HttpExchange httpExchange) throws IOException {
        final var in = httpExchange.getRequestBody();
        final var out = new ByteArrayOutputStream();
        final byte[] buffer = new byte[READ_BUFFER_SIZE];
        int read;
        while ((read = in.read(buffer)) != -1) {
            if (out.size() + read > MAX_BODY_SIZE) {
                throw new BodyTooLargeException();
            }
            out.write(buffer, 0, read);
        }
        return out.toByteArray();
    }

    private List<Integer> clusterPorts() {
        String clusterProperty = System.getProperty("CLUSTER");
        if (clusterProperty == null || clusterProperty.isEmpty()) {
            return List.of(this.port);
        }
        List<Integer> result = new ArrayList<>();
        for (String p : clusterProperty.split(",")) {
            result.add(Integer.parseInt(p));
        }
        return result;
    }

    private HttpHandler replicaEntityHandler() {
        return httpExchange -> {
            final var method = httpExchange.getRequestMethod();
            final var uriStr = httpExchange.getRequestURI().toString();
            String entityID;
            try {
                entityID = uriStr.split("\\?id=")[1];
            } catch (Exception e) {
                httpExchange.sendResponseHeaders(400, -1);
                httpExchange.close();
                return;
            }
            if (entityID.isEmpty()) {
                httpExchange.sendResponseHeaders(400, -1);
                httpExchange.close();
                return;
            }

            switch (method) {
                case METHOD_GET:
                    try {
                        final byte[] value = dao.get(entityID);
                        httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
                        httpExchange.sendResponseHeaders(200, value.length);
                        httpExchange.getResponseBody().write(value);
                    } catch (NoSuchElementException e) {
                        httpExchange.sendResponseHeaders(404, -1);
                    } catch (IllegalArgumentException e) {
                        httpExchange.sendResponseHeaders(422, -1);
                    }
                    break;
                case METHOD_PUT:
                    try {
                        byte[] body = readBodyWithLimit(httpExchange);
                        dao.upsert(entityID, body);
                        httpExchange.sendResponseHeaders(201, -1);
                    } catch (BodyTooLargeException e) {
                        httpExchange.sendResponseHeaders(400, -1);
                    } catch (IllegalArgumentException e) {
                        httpExchange.sendResponseHeaders(422, -1);
                    }
                    break;
                case METHOD_DELETE:
                    try {
                        dao.delete(entityID);
                        httpExchange.sendResponseHeaders(202, -1);
                    } catch (IllegalArgumentException e) {
                        httpExchange.sendResponseHeaders(422, -1);
                    }
                    break;
                default:
                    httpExchange.sendResponseHeaders(405, -1);
                    break;
            }

            httpExchange.close();
        };
    }

    private void getEntityHandler(HttpExchange httpExchange, String entityID, int ack) throws IOException {
        log.info("Using getEntityHandler with entityID {} and ack {}", entityID, ack);
        this.ensureHashInitialized();
        var replicas = hash.replicas(entityID, hash.clusterNodesListSize());
        int answered = 0;
        byte[] found = null;

        for (Integer nodePort : replicas) {
            if (nodePort.equals(this.port)) {
                try {
                    found = dao.get(entityID);
                    answered += 1;
                } catch (NoSuchElementException e) {
                    answered += 1;
                } catch (Exception e) {
                    log.warn("Failed to get info from local port");
                }
            } else {
                try {
                    var resp = client.send(HttpRequest.newBuilder(URI.create(
                            "http://localhost:" + nodePort + "/v0/entity/replica?id="
                                    + entityID)).GET().build(),
                            HttpResponse.BodyHandlers.ofByteArray());
                    if (resp.statusCode() == 200) {
                        found = resp.body();
                        answered += 1;
                    } else if (resp.statusCode() == 404) {
                        answered += 1;
                    }
                } catch (Exception e) {
                    log.warn("Failed to get info from port {}", nodePort);
                }
            }
        }

        log.info("ack {} answered {}", ack, answered);
        if (found != null && answered >= ack) {
            httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            httpExchange.sendResponseHeaders(200, found.length);
            httpExchange.getResponseBody().write(found);
            log.info("getEntityHandler for entityID {} 200 and body {}", entityID, found);
        } else if (answered >= ack) {
            httpExchange.sendResponseHeaders(404, -1);
        } else {
            httpExchange.sendResponseHeaders(503, -1);
        }

        httpExchange.close();
    }

    private void putEntityHandler(HttpExchange httpExchange, String entityID, byte[] body, int ack) throws IOException {
        log.info("Using putEntityHandler with entityID {}, body {} and ack {}", entityID, body, ack);
        this.ensureHashInitialized();
        var replicas = hash.replicas(entityID, hash.clusterNodesListSize());
        int answered = 0;

        for (Integer nodePort : replicas) {
            if (nodePort.equals(this.port)) {
                try {
                    dao.upsert(entityID, body);
                    answered += 1;
                } catch (IllegalArgumentException e) {
                    log.info("putEntityHandler for entityID {} 422", entityID);
                    httpExchange.sendResponseHeaders(422, -1);
                    httpExchange.close();
                }
            } else {
                try {
                    var postBody = HttpRequest.BodyPublishers.ofByteArray(body);
                    var resp = client.send(HttpRequest.newBuilder(URI.create(
                                    "http://localhost:" + nodePort + "/v0/entity/replica?id="
                                            + entityID)).PUT(postBody).build(),
                            HttpResponse.BodyHandlers.ofByteArray());
                    if (resp.statusCode() == 201) {
                        answered += 1;
                    }
                } catch (Exception e) {
                    log.warn("Failed to put info into port {}", nodePort);
                }
            }
        }

        if (answered >= ack) {
            httpExchange.sendResponseHeaders(201, -1);
        } else {
            httpExchange.sendResponseHeaders(503, -1);
        }
        httpExchange.close();
    }

    private void deleteEntityHandler(HttpExchange httpExchange, String entityID, int ack) throws IOException {
        log.info("Using deleteEntityHandler with entityID {} and ack {}", entityID, ack);
        int answered = 0;
        var ports = clusterPorts();

        for (Integer nodePort : ports) {
            if (nodePort.equals(this.port)) {
                try {
                    dao.delete(entityID);
                    answered += 1;
                } catch (IllegalArgumentException e) {
                    httpExchange.sendResponseHeaders(422, -1);
                    log.info("deleteEntityHandler for entityID {} 422", entityID);
                    httpExchange.close();
                }
            } else {
                try {
                    var resp = client.send(HttpRequest.newBuilder(URI.create(
                                    "http://localhost:" + nodePort + "/v0/entity/replica?id="
                                            + entityID)).DELETE().build(),
                            HttpResponse.BodyHandlers.ofByteArray());
                    if (resp.statusCode() == 202) {
                        answered += 1;
                    }
                } catch (Exception e) {
                    log.warn("Failed to delete info into port {}", nodePort);
                }
            }
        }

        if (answered >= ack) {
            httpExchange.sendResponseHeaders(202, -1);
            log.info("deleteEntityHandler for entityID {} 202", entityID);
        } else {
            httpExchange.sendResponseHeaders(503, -1);
            log.info("deleteEntityHandler for entityID {} 503", entityID);
        }
        httpExchange.close();
    }

    private synchronized void ensureHashInitialized() {
        if (!hashInitialized) {
            for (Integer port : clusterPorts()) {
                hash.add(port);
            }
            hashInitialized = true;
        }
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        this.server.stop(1);
        this.executor.shutdown();
    }

    private static final class BodyTooLargeException extends IOException {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    private record ErrorHandler(HttpHandler delegate) implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException e) {
                final var message = e.getMessage();
                final var body = message.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(422, body.length);
                exchange.getResponseBody().write(body);
            } catch (Exception e) {
                final var message = e.getMessage();
                final var body = message.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(500, body.length);
                exchange.getResponseBody().write(body);
            }
            exchange.close();
        }
    }
}
