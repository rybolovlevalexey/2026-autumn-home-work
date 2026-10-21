package company.vk.edu.distrib.compute.rybolovlevalexey.kv;

import company.vk.edu.distrib.compute.AbstractHttpServiceFactory;
import company.vk.edu.distrib.compute.kv.KVServiceTest;

import java.io.IOException;

@KVServiceTest
public class RybAlexeyKvServiceFactory extends AbstractHttpServiceFactory<RybAlexeyKv> {
    @Override
    protected RybAlexeyKv doCreate(int port) throws IOException {
        return new RybAlexeyKv(port);
    }
}
