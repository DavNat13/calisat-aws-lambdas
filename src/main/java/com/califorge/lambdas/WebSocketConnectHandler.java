package com.califorge.lambdas;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;

/** Ruta $connect del API Gateway WebSocket: registra el connectionId en ConexionesWebSocket. */
public class WebSocketConnectHandler
        implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {

    private final DynamoDbClient dynamoDb = DynamoDbClient.create();
    private final String tableName = System.getenv().getOrDefault("CONNECTIONS_TABLE", "ConexionesWebSocket");

    @Override
    public APIGatewayV2WebSocketResponse handleRequest(APIGatewayV2WebSocketEvent event, Context context) {
        APIGatewayV2WebSocketResponse response = new APIGatewayV2WebSocketResponse();
        try {
            Map<String, String> query = event.getQueryStringParameters();
            String clientId = query != null && query.get("clientId") != null ? query.get("clientId") : "dashboard";
            String connectionId = event.getRequestContext().getConnectionId();
            dynamoDb.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(Map.of(
                            "connectionId", AttributeValue.builder().s(connectionId).build(),
                            "clientId", AttributeValue.builder().s(clientId).build()))
                    .build());
            context.getLogger().log("Cliente conectado: " + connectionId);
            response.setStatusCode(200);
            response.setBody("Conectado");
        } catch (Exception e) {
            context.getLogger().log("Error guardando conexion: " + e.getMessage());
            response.setStatusCode(500);
            response.setBody("Error interno");
        }
        return response;
    }
}
