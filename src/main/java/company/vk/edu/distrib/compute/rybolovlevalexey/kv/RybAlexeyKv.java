package company.vk.edu.distrib.compute.rybolovlevalexey.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.KVService;
import company.vk.edu.distrib.compute.rybolovlevalexey.urlshortener.RybolovlevAlexeyPersistentDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.NoSuchElementException;

public class RybAlexeyKv implements KVService {
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String LINK_ENTITY_V0 = "/v0/entity";
    private static final String LINK_STATUS_V0 = "/v0/status";

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private static final Logger log = LoggerFactory.getLogger(RybAlexeyKv.class);

    public RybAlexeyKv(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        final Path dataDir = Path.of(System.getProperty("java.io.tmpdir"), "rybolovlevalexey-kv-data");
        this.dao = RybolovlevAlexeyPersistentDao.bytesBased(dataDir.resolve("entity-" + port + ".properties"));

        server.createContext(LINK_STATUS_V0, new ErrorHandler(statusHandler()));
        server.createContext(LINK_ENTITY_V0, new ErrorHandler(entityHandler()));
    }

    private HttpHandler statusHandler() {
        log.info("Creating of statusHandler");
        return httpExchange -> {
            log.info("Received request /v0/status");
            final var requestMethod = httpExchange.getRequestMethod();
            if (METHOD_GET.equals(requestMethod)) {
                httpExchange.sendResponseHeaders(200, 0);
            } else {
                httpExchange.sendResponseHeaders(405, 0);
            }
            httpExchange.close();
        };
    }

    private HttpHandler entityHandler() {
        final var uriMinPartsCount = 2;
        log.info("Creating of entityHandler");
        return httpExchange -> {
            var method = httpExchange.getRequestMethod();
            final var path = httpExchange.getRequestURI().getPath();
            final String uriStr = httpExchange.getRequestURI().toString();
            if (uriStr.split("id=").length < uriMinPartsCount) {
                httpExchange.sendResponseHeaders(400, 0);
                httpExchange.close();
                return;
            }
            final var entityID = uriStr.split("id=")[1];
            log.info("Received request at {} with method {} and entityID {}", path, method, entityID);

            if (entityID.isEmpty()) {
                httpExchange.sendResponseHeaders(400, 0);
                httpExchange.close();
                return;
            }

            switch (method) {
                case METHOD_GET:
                    getEntityHandler(httpExchange, entityID);
                    break;
                case METHOD_PUT:
                    byte[] body = httpExchange.getRequestBody().readAllBytes();
                    putEntityHandler(httpExchange, entityID, body);
                    break;
                case METHOD_DELETE:
                    deleteEntityHandler(httpExchange, entityID);
                    break;
                default:
                    httpExchange.sendResponseHeaders(405, 0);
                    httpExchange.close();
                    break;
            }
        };
    }

    private void getEntityHandler(HttpExchange httpExchange, String entityID) throws IOException {
        log.info("Using getEntityHandler with entityID {}", entityID);
        try {
            final byte[] entityResult = dao.get(entityID);

            httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            httpExchange.sendResponseHeaders(200, entityResult.length);
            httpExchange.getResponseBody().write(entityResult);

            log.info("getEntityHandler for entityID {} 200 and body {}", entityID, entityResult);
        } catch (IllegalArgumentException e) {
            httpExchange.sendResponseHeaders(422, 0);
            log.info("getEntityHandler for entityID {} 422", entityID);
        } catch (NoSuchElementException e) {
            httpExchange.sendResponseHeaders(404, 0);
            log.info("getEntityHandler for entityID {} 404", entityID);
        }
        httpExchange.close();
    }

    private void putEntityHandler(HttpExchange httpExchange, String entityID, byte[] body) throws IOException {
        log.info("Using putEntityHandler with entityID {} and body {}", entityID, body);
        try {
            dao.upsert(entityID, body);
            httpExchange.sendResponseHeaders(201, 0);
            log.info("putEntityHandler for entityID {} 201", entityID);
            httpExchange.close();
        } catch (IllegalArgumentException e) {
            httpExchange.sendResponseHeaders(422, 0);
            log.info("putEntityHandler for entityID {} 422", entityID);
            httpExchange.close();
        }
    }

    private void deleteEntityHandler(HttpExchange httpExchange, String entityID) throws IOException {
        log.info("Using deleteEntityHandler with entityID {}", entityID);
        try {
            dao.delete(entityID);
            httpExchange.sendResponseHeaders(202, 0);
            log.info("deleteEntityHandler for entityID {} 202", entityID);
            httpExchange.close();
        } catch (IllegalArgumentException e) {
            httpExchange.sendResponseHeaders(422, 0);
            log.info("deleteEntityHandler for entityID {} 422", entityID);
            httpExchange.close();
        }
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        this.server.stop(1);
    }

    private record ErrorHandler(HttpHandler delegate) implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException e) {
                final var message = e.getMessage();
                exchange.sendResponseHeaders(422, message.length());
                exchange.getResponseBody().write(message.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                final var message = e.getMessage();
                exchange.sendResponseHeaders(500, message.length());
                exchange.getResponseBody().write(message.getBytes(StandardCharsets.UTF_8));
            }
            exchange.close();
        }
    }
}
