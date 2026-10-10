package company.vk.edu.distrib.compute.test.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.ClusterDaoFactoryTest;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;

import java.io.IOException;

@ClusterDaoFactoryTest
public class RybAlexeyClusterRemoteDaoFactory implements RemoteDaoFactory {
    @Override
    public Dao create(int... ports) throws IOException {
        return null;
    }
}
