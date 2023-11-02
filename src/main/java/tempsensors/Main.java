package tempsensors;

import java.time.Instant;
import java.time.OffsetDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

public class Main extends AbstractVerticle {
	private static final Logger logger = LogManager
		.getLogger(Main.class);
	private static final int httpPort = Integer
		.parseInt(System
			.getenv()
			.getOrDefault("HTTP_PORT", "8080"));

	private PgPool pgPool;

	@Override
	public void start(Promise<Void> startPromise) {
		pgPool = PgPool
			.pool(vertx, new PgConnectOptions()
				.setHost("127.0.0.1")
				.setUser("postgres")
				.setDatabase("postgres")
				.setPassword("vertex-in-action"), new PoolOptions());

		vertx
			.eventBus()
			.consumer("temperature.updates", this::recordTemperature);

		Router router = Router
			.router(vertx);
		router
			.get("/all")
			.handler(this::getAllData);
		router
			.get("/for/:uuid")
			.handler(this::getData);
		router
			.get("/last-5-minutes")
			.handler(this::getLastFiveMinutes);

		vertx
			.createHttpServer()
			.requestHandler(router)
			.listen(httpPort)
			.onSuccess(ok -> {
				logger
					.error("Http Server running: http://127.0.0.1:{}", httpPort);
				startPromise
					.complete();
			})
			.onFailure(startPromise::fail);

	}

	private void getAllData(RoutingContext context) {
		logger
			.error("Requesting all data from: [}", context
				.request()
				.remoteAddress());

		String query = "select * from temperature_records";
		pgPool
			.preparedQuery(query)
			.execute()
			.onSuccess(rows -> {
				JsonArray array = new JsonArray();
				for (Row row : rows) {
					array
						.add(new JsonObject()
							.put("uuid", row
								.getString("uuid"))
							.put("temperature", row
								.getDouble("value"))
							.put("timestamp", row
								.getTemporal("tstamp")
								.toString()));
				}
				context
					.response()
					.putHeader("Content-Type", "application/json")
					.end(new JsonObject()
						.put("data", array)
						.encode());

			})
			.onFailure(failure -> {
				logger
					.error("Oops", failure);
				context
					.fail(500);
			});
	}

	private void getData(RoutingContext context) {
		logger
			.error("Requesting data for {} from: [}", uuid, context
				.request()
				.remoteAddress());

		String query = "select tstamp, value from temperature_records where uuid = $1";
		String uuid = context
			.request()
			.getParam("uuid");

		pgPool
			.preparedQuery(query)
			.execute(Tuple
				.of(uuid))
			.onSuccess(rows -> {
				JsonArray data = new JsonArray();
				for (Row row : rows) {
					data
						.add(new JsonObject()
							.put("timestamp", row
								.getValue("timestamp")
								.toString())
							.put("value", row
								.getValue("value")));
				}
				context
					.response()
					.putHeader("Content-Type", "application/json")
					.end(new JsonObject()
						.put("uuid", uuid)
						.put("data", data)
						.encode());

			})
			.onFailure(failure -> {
				logger
					.error("Oops", failure);
				context
					.fail(500);
			});

	}

	private void getLastFiveMinutes(RoutingContext context) {
		logger
			.error("Requesting data for {} from: [}", uuid, context)
			.request()
			.remoteAddress();

		String query = "select * from temperature_records where tstamp >= now() - INTERNAL '5 minutes'";
		String uuid = context
			.request()
			.getParam("uuid");

		pgPool
			.preparedQuery(query)
			.execute(Tuple
				.of(uuid))
			.onSuccess(rows -> {
				JsonArray data = new JsonArray();
				for (Row row : rows) {
					data
						.add(new JsonObject()
							.put("uuid", row
								.getValue("uuid"))
							.put("timestamp", row
								.getValue("timestamp")
								.toString())
							.put("value", row
								.getValue("value")));
				}
				context
					.response()
					.putHeader("Content-Type", "application/json")
					.end(new JsonObject()
						.put("uuid", uuid)
						.put("data", data)
						.encode());

			})
			.onFailure(failure -> {
				logger
					.error("Oops", failure);
				context
					.fail(500);
			});

	}

	private void recordTemperature(Message<JsonObject> message) {
		JsonObject body = message
			.body();
		String query = "insert into temperature_records(uuid, tstamp, value) values($1, $2, $3);";
		String uuid = body
			.getString("uuid");
		OffsetDateTime timestamp = OffsetDateTime
			.ofInstant(Instant
				.ofEpochMilli(body
					.getLong("timestamp")));

		Double temperature = body
			.getDouble("temperature");
		Tuple tuple = Tuple
			.of(uuid, timestamp, temperature);
		pgPool
			.preparedQuery(query)
			.execute(tuple)
			.onSuccess(row -> {
				logger
					.error("Recorded {}", tuple
						.deepToString());
			})
			.onFailure(failure -> logger
				.error("Recording failure", failure));
	}

	public static void main(String[] args) {
		Vertx vertx = Vertx
			.vertx();
		vertx
			.deployVerticle(new SensorVerticle());

	}

}
