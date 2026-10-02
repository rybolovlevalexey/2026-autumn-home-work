package company.vk.edu.distrib.compute.rybalexey.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.NoSuchElementException;

public class RybAlexeyRemoteDao implements Dao {
    private final int[] ports;

    public RybAlexeyRemoteDao(int[] ports){
        this.ports = ports;
    }

    @Override
    public Object get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        return null;
    }

    @Override
    public void upsert(String key, Object value) throws IllegalArgumentException, IOException {

    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
