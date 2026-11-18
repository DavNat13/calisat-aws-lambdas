# calisat-aws-lambdas

Lambdas Java 21 para el **dashboard en tiempo real** (WebSocket + DynamoDB Streams). Basado en la guía
"Telemetría a tiempo real con frontend". Compilar: `./mvnw clean package` → `target/calisat-aws-lambdas-1.0.0.jar`.

| Handler | Ruta/Trigger | Función |
|---|---|---|
| `com.califorge.lambdas.WebSocketConnectHandler::handleRequest` | WebSocket `$connect` | Guarda `connectionId` en `ConexionesWebSocket` |
| `com.califorge.lambdas.WebSocketDisconnectHandler::handleRequest` | WebSocket `$disconnect` | Elimina el `connectionId` |
| `com.califorge.lambdas.InteraccionStreamHandler::handleRequest` | DynamoDB Stream de `UserInteractions` | Agrega el lote y hace *broadcast* a todos los dashboards |

## Despliegue (AWS Academy, `us-east-1`, rol `LabRole`)
1. **DynamoDB**
   - `UserInteractions`: PK `userId` (S), SK `sk` (S). Habilitar **DynamoDB Streams** (*New image*).
   - `ConexionesWebSocket`: PK `connectionId` (S).
2. **Lambdas** (Java 21, rol `LabRole`, subir el mismo JAR en ambas) con los handlers de la tabla.
   Variables de entorno de `InteraccionStreamHandler`: `WEBSOCKET_API_ENDPOINT=https://<api-id>.execute-api.us-east-1.amazonaws.com/<stage>`.
3. **API Gateway → WebSocket API** `calisat_ws` (expresión de ruta `request.body.action`): integrar `$connect` y `$disconnect` con las Lambdas de conexión.
4. **Trigger**: en `InteraccionStreamHandler` agregar el stream de `UserInteractions` (batch 10, *Latest*).
5. **ms-analytics con DynamoDB (+2 pts)**: arrancar con `SPRING_PROFILES_ACTIVE=dynamodb` (tabla `UserInteractions`, credenciales vía `LabInstanceProfile`).

## Dos canales de tiempo real
- **Principal (implementado y probado)**: `calisat-ms-analytics` expone `wss://<host>/ws/metricas?token=<JWT>` (solo ADMINISTRADOR) con 4 métricas.
- **Serverless (este repo)**: mismo patrón con DynamoDB Streams + API Gateway WebSocket para cuando el historial vive en DynamoDB.
  El frontend (`/admin/metricas`) acepta cualquiera de las dos URL vía `VITE_METRICAS_WS_URL`.
