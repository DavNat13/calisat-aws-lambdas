package com.califorge.lambdas;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2WebSocketResponse;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;

import java.util.Map;

/** Ruta $disconnect: elimina el connectionId de ConexionesWebSocket. */
public class WebSocketDisconnectHandler
        implements RequestHandler<APIGatewayV2WebSocketEvent, APIGatewayV2WebSocketResponse> {

    private final DynamoDbClient dynamoDb = DynamoDbClient.create();
    private final String tableName = System.getenv().getOrDefault("CONNECTIONS_TABLE", "ConexionesWebSocket");

    @Override
    public APIGatewayV2WebSocketResponse handleRequest(APIGatewayV2WebSocketEvent event, Context context) {
        APIGatewayV2WebSocketResponse response = new APIGatewayV2WebSocketResponse();
        try {
            dynamoDb.deleteItem(DeleteItemRequest.builder()
                    .tableName(tableName)
                    .key(Map.of("connectionId",
                            AttributeValue.builder().s(event.getRequestContext().getConnectionId()).build()))
                    .build());
            response.setStatusCode(200);
        } catch (Exception e) {
            context.getLogger().log("Error eliminando conexion: " + e.getMessage());
            response.setStatusCode(500);
        }
        return response;
    }
}
