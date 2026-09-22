package company.vk.edu.distrib.compute.rybolovlevalexey.urlshortener;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.urlshortener.UrlShortenerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;

public class RybolovlevAlexeyUrlShortenerService implements UrlShortenerService {

    private final int port;
    private final HttpServer server;
    private static final Logger log = LoggerFactory.getLogger(RybolovlevAlexeyUrlShortenerServiceFactory.class);
    private final Dao<String> dao = new RybolovlevAlexeyDao();
    private final ShortLinkIDGenerator shortLinkGenerator = new ShortLinkIDGenerator(10);
    private final RybolovlevAlexeyUrlShortenerUtils serviceUtils = new RybolovlevAlexeyUrlShortenerUtils();


    public RybolovlevAlexeyUrlShortenerService(int port) throws IOException {
        this.port = port;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext(
            "/v0/status",
            new ErrorHandler(new HttpHandler() {
                @Override
                public void handle(HttpExchange httpExchange) throws IOException {
                    log.info("Received request /v0/status");
                    var requestMethod = httpExchange.getRequestMethod();
                    if (requestMethod.equals("GET")) {
                        httpExchange.sendResponseHeaders(200, 0);
                    } else {
                        httpExchange.sendResponseHeaders(405, 0);
                    }
                    httpExchange.close();
                }
            }
        ));

        server.createContext("/v0/links", new ErrorHandler(new HttpHandler() {
            @Override
            public void handle(HttpExchange httpExchange) throws IOException {
                var requestMethod = httpExchange.getRequestMethod();
                log.info("Request to /v0/links with method {}", requestMethod);

                switch (requestMethod) {
                    case "GET":
                        getV0LinksHandler(httpExchange);
                        break;
                    case "POST":
                        postV0LinksHandler(httpExchange);
                        break;
                    case "PUT":
                        putV0LinksHandler(httpExchange);
                        break;
                    case "DELETE":
                        deleteV0LinksHandler(httpExchange);
                        break;
                    default:
                        httpExchange.sendResponseHeaders(405, 0);
                        httpExchange.close();
                }
            }
        }));

        server.createContext("/", new ErrorHandler(new HttpHandler() {
            @Override
            public void handle(HttpExchange httpExchange) throws IOException {
                final var requestMethod = httpExchange.getRequestMethod();
                final var path = httpExchange.getRequestURI().getPath();
                log.info("Received request for redirect with method {} and ID {}", requestMethod, path);
                final var linkID = path.substring(1);

                if (!Objects.equals(requestMethod, "GET")){
                    httpExchange.sendResponseHeaders(405, 0);
                    return;
                }
                try{
                    serviceUtils.validateLinkID(linkID);
                    final var longLink = dao.get(linkID);

                    httpExchange.getResponseHeaders().add("Location", longLink);
                    httpExchange.sendResponseHeaders(301,  -1);
                } catch (IllegalArgumentException e){
                    httpExchange.sendResponseHeaders(422,  0);
                } catch (NoSuchElementException e){
                    httpExchange.sendResponseHeaders(404,  0);
                } finally {
                    httpExchange.close();
                }
            }
        }));
    }
    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        this.server.stop(1);
    }

    private void getV0LinksHandler(HttpExchange httpExchange) throws IOException {
        final var path = httpExchange.getRequestURI().getPath();
        final var linkID = path.substring("/v0/links/".length());
        log.info("Received request to GET /v0/links with ID {}", linkID);

        try {
            serviceUtils.validateLinkID(linkID);
            final var valueLongLink = this.dao.get(linkID);
            httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            httpExchange.sendResponseHeaders(200, valueLongLink.getBytes(StandardCharsets.UTF_8).length);
            httpExchange.getResponseBody().write(valueLongLink.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchElementException e) {
            httpExchange.sendResponseHeaders(404, 0);
        } catch (IllegalArgumentException e){
            httpExchange.sendResponseHeaders(422, 0);
        }
        httpExchange.close();
    }

    private void postV0LinksHandler(HttpExchange httpExchange) throws IOException{
        final var body = new String(httpExchange.getRequestBody().readAllBytes());
        log.info("Received request to POST /v0/links with body {}", body);
        serviceUtils.validateLink(body);
        final var linkID = shortLinkGenerator.generateID();
        dao.upsert(linkID, body);
        final var resultBody = "http://localhost:%d/%s".formatted(port, linkID);
        log.info("Sending response {}", resultBody);

        httpExchange.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
        httpExchange.sendResponseHeaders(201, resultBody.getBytes(StandardCharsets.UTF_8).length);
        httpExchange.getResponseBody().write(resultBody.getBytes(StandardCharsets.UTF_8));
        httpExchange.close();
    }

    private void putV0LinksHandler(HttpExchange httpExchange) throws IOException{
        final var body = new String(httpExchange.getRequestBody().readAllBytes());
        final var path = httpExchange.getRequestURI().getPath();
        final var linkID = path.substring("/v0/links/".length());
        log.info("Received request to PUT /v0/links with body {} and link ID {}", body, linkID);

        try{
            serviceUtils.validateLink(body);
            final var value = dao.get(linkID);
            dao.delete(linkID);
            dao.upsert(linkID, body);
            httpExchange.sendResponseHeaders(200, 0);
        } catch (IllegalArgumentException e){
            throw e;
        } catch (NoSuchElementException e){
            httpExchange.sendResponseHeaders(404, 0);
        }
        httpExchange.close();
    }

    private void deleteV0LinksHandler(HttpExchange httpExchange) throws IOException{
        final var path = httpExchange.getRequestURI().getPath();
        final var linkID = path.substring("/v0/links/".length());
        log.info("Received request to DELETE /v0/links with link ID {}", linkID);

        try{
            dao.delete(linkID);
            httpExchange.sendResponseHeaders(202, 0);
        } catch (NoSuchElementException e){
            httpExchange.sendResponseHeaders(404, 0);
        }
        httpExchange.close();
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

    public static class ShortLinkIDGenerator {
        private static final Random RANDOM = new Random();
        private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        private static int idLength;

        public ShortLinkIDGenerator(int idLength){
            ShortLinkIDGenerator.idLength = idLength;
        }

        public String generateID(){
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < idLength; i++){
                int index = RANDOM.nextInt(CHARS.length());
                result.append(CHARS.charAt(index));
            }
            return result.toString();
        }
    }
}
