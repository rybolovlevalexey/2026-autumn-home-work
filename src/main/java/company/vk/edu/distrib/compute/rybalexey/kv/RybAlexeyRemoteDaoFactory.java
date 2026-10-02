package company.vk.edu.distrib.compute.rybalexey.kv;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactory;
import company.vk.edu.distrib.compute.kv.RemoteDaoFactoryTest;

import java.io.IOException;
import java.util.Objects;

@RemoteDaoFactoryTest
public class RybAlexeyRemoteDaoFactory implements RemoteDaoFactory {
    @Override
    public Dao create(int... ports) throws IOException {
        return new RybAlexeyRemoteDao(ports);
    }
}
