package company.vk.edu.distrib.compute.rybolovlevalexey.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

import java.io.IOException;
import java.net.http.HttpClient;

@ClusterDaoFactoryTest
public class RybAlexeyClusterRemoteDaoFactory implements RemoteDaoFactory<String> {
    @Override
    public Dao create(int... ports) throws IOException {
        return new RybAlexeyClusterRemoteDao(HttpClient.newHttpClient(), ports);
    }
}
