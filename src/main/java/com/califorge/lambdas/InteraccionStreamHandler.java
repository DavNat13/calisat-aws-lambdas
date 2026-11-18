package com.califorge.lambdas;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.apigatewaymanagementapi.ApiGatewayManagementApiClient;
import software.amazon.awssdk.services.apigatewaymanagementapi.model.PostToConnectionRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Disparada por DynamoDB Streams de UserInteractions. Agrega el lote de cambios (eventos por accion y
 * productos vistos) y lo difunde a todos los dashboards conectados por API Gateway WebSocket.
 * Variables de entorno: WEBSOCKET_API_ENDPOINT (https://{api-id}.execute-api.us-east-1.amazonaws.com/{stage})
 * y CONNECTIONS_TABLE (por defecto ConexionesWebSocket).
 */
public class InteraccionStreamHandler implements RequestHandler<DynamodbEvent, Void> {

    private final ObjectMapper mapper = new ObjectMapper();
    private final DynamoDbClient dynamoDb = DynamoDbClient.create();
    private final String connectionsTable = System.getenv().getOrDefault("CONNECTIONS_TABLE", "ConexionesWebSocket");

    @Override
    public Void handleRequest(DynamodbEvent event, Context context) {
        if (event == null || event.getRecords() == null || event.getRecords().isEmpty()) {
            return null;
        }
        Map<String, Integer> porAccion = new LinkedHashMap<>();
        Map<String, Integer> vistas = new LinkedHashMap<>();
        int total = 0;
        for (DynamodbEvent.DynamodbStreamRecord r : event.getRecords()) {
            if (!"INSERT".equals(r.getEventName())) {
                continue;
            }
            Map<String, AttributeValue> img = r.getDynamodb().getNewImage();
            if (img == null || img.get("action") == null) {
                continue;
            }
            total++;
            String accion = img.get("action").getS();
            porAccion.merge(accion, 1, Integer::sum);
            if ("view".equals(accion) && img.get("itemId") != null) {
                vistas.merge(img.get("itemId").getS(), 1, Integer::sum);
            }
        }
        if (total == 0) {
            return null;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("eventosEnLote", total);
        payload.put("eventosPorAccion", porAccion);
        payload.put("productosVistos", vistas);
        broadcast(payload, context);
        return null;
    }

    private void broadcast(Map<String, Object> payload, Context context) {
        String endpoint = System.getenv("WEBSOCKET_API_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            context.getLogger().log("WEBSOCKET_API_ENDPOINT no configurada");
            return;
        }
        try {
            String json = mapper.writeValueAsString(payload);
            ApiGatewayManagementApiClient client = ApiGatewayManagementApiClient.builder()
                    .endpointOverride(URI.create(endpoint)).region(Region.US_EAST_1).build();
            for (String connectionId : conexionesActivas()) {
                try {
                    client.postToConnection(PostToConnectionRequest.builder()
                            .connectionId(connectionId).data(SdkBytes.fromUtf8String(json)).build());
                } catch (Exception e) {
                    context.getLogger().log("Conexion " + connectionId + " no disponible: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            context.getLogger().log("Error en broadcast: " + e.getMessage());
        }
    }

    private List<String> conexionesActivas() {
        List<String> ids = new ArrayList<>();
        dynamoDb.scanPaginator(ScanRequest.builder().tableName(connectionsTable).build())
                .items().forEach(item -> ids.add(item.get("connectionId").s()));
        return ids;
    }
}
