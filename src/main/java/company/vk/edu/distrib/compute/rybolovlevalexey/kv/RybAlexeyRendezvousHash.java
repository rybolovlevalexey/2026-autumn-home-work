package company.vk.edu.distrib.compute.rybolovlevalexey.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RybAlexeyRendezvousHash<N> {
    private final Set<N> nodes = new LinkedHashSet<>();

    public void add(N node) {
        nodes.add(node);
    }

    public N get(String key) {
        N best = null;
        long bestScore = Long.MIN_VALUE;
        for (N node : nodes) {
            long score = score(key, node);
            if (best == null || score > bestScore) {
                best = node;
                bestScore = score;
            }
        }
        return best;
    }

    public List<N> replicas(String key, int count) {
        List<N> ring = new ArrayList<>(nodes);
        ring.sort(Comparator.comparing(Object::toString));
        final N owner = get(key);
        final int start = ring.indexOf(owner);
        final List<N> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(ring.get((start + i) % ring.size()));
        }
        return result;
    }

    public int clusterNodesListSize() {
        return nodes.size();
    }

    private long score(String key, N node) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest((key + '|' + node).getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(digest).getLong(); // первые 8 байт как long
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

}
