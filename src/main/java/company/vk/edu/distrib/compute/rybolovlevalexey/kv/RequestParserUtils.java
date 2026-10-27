package company.vk.edu.distrib.compute.rybolovlevalexey.kv;

public final class RequestParserUtils {
    private RequestParserUtils() {
    }

    public record EntityParams(String entityID, int ack, boolean badRequest, String ackStr) {}

    public static EntityParams parseEntityHandlerParams(String uriStr, int clusterNodesCount) {
        var createParamsRes = createParams(uriStr);
        String entityID = createParamsRes.entityID;
        String ackStr = createParamsRes.ackStr;

        if (entityID.isEmpty()) {
            return new EntityParams("", -1, true, "");
        }

        int ack = 1;
        if (ackStr.isEmpty()) {
            return new EntityParams(entityID, ack, false, "");
        }
        try {
            ack = Integer.parseInt(ackStr);
        } catch (NumberFormatException e) {
            return new EntityParams("", -1, true, "");
        }
        if (ack < 1 || ack > clusterNodesCount) {
            return new EntityParams("", -1, true, "");
        }
        return new EntityParams(entityID, ack, false, "");
    }

    private static EntityParams createParams(String uriStr) {
        String entityID = "";
        String ackStr = "";

        try {
            var pathParams = uriStr.split("\\?")[1].split("&");
            for (String pathParam : pathParams) {
                var keyValue = pathParam.split("=");
                if (!(keyValue.length == 2 || keyValue.length == 1 && "ack".equals(keyValue[0]))) {
                    return new EntityParams("", -1, true, "");
                }

                switch (keyValue[0]) {
                    case "id" -> entityID = keyValue[1];
                    case "ack" -> ackStr = keyValue[1];
                    default -> {
                    }
                }
            }
        } catch (Exception e) {
            return new EntityParams("", -1, true, "");
        }
        return new EntityParams(entityID, -1, false, ackStr);
    }
}
