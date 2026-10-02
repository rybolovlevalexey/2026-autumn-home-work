package company.vk.edu.distrib.compute.rybolovlevalexey.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

import java.io.IOException;
import java.net.http.HttpClient;

@RemoteDaoFactoryTest
public class RybAlexeyRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao<String> create(int... ports) throws IOException {
        final var port = ports[0];
        final var kvServer = new RybAlexeyKv(port);
        kvServer.start();
        return new RybAlexeyRemoteDao(HttpClient.newHttpClient(), port);
    }
}
