package company.vk.edu.distrib.compute.rybolovlevalexey.urlshortener;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;

import java.io.IOException;

public class RybolovlevAlexeyUrlShortenerServiceFactory extends AbstractHttpServiceFactory<RybolovlevAlexeyUrlShortenerService> {
    @Override
    protected RybolovlevAlexeyUrlShortenerService doCreate(int port) throws IOException {
        return new RybolovlevAlexeyUrlShortenerService(port);
    }
}
