package company.vk.edu.distrib.compute.rybolovlevalexey.urlshortener;

import java.io.IOException;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;

public class RybolovlevAlexeyUrlShortenerServiceFactory
    extends AbstractHttpServiceFactory<RybolovlevAlexeyUrlShortenerService> {
    @Override
    protected RybolovlevAlexeyUrlShortenerService doCreate(int port) throws IOException {
        return new RybolovlevAlexeyUrlShortenerService(port);
    }
}
