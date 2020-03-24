package io.dgraph;

import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import io.opencensus.common.Scope;
import io.opencensus.exporter.trace.jaeger.JaegerTraceExporter;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.config.TraceParams;
import io.opencensus.trace.samplers.Samplers;
import java.util.concurrent.*;
import org.testng.annotations.Test;

public class OpencensusJaegerTest extends DgraphIntegrationTest {
  private static final String JAEGER_COLLECTOR = "http://localhost:14268/api/traces";
  private static final ExecutorService SHUTDOWN_EXECUTER = Executors.newFixedThreadPool(1);

  private static void runTransactions() {
    // change schema
    DgraphProto.Operation op =
        DgraphProto.Operation.newBuilder()
            .setSchema("name: string @index(fulltext) @upsert .")
            .build();
    dgraphClient.alter(op);

    // Add data
    JsonObject jsonData = new JsonObject();
    jsonData.addProperty("name", "Alice");

    DgraphProto.Mutation mu =
        DgraphProto.Mutation.newBuilder()
            .setCommitNow(false)
            .setSetJson(ByteString.copyFromUtf8(jsonData.toString()))
            .build();
    Transaction txn = dgraphClient.newTransaction();
    txn.mutate(mu);
    txn.commit();

    String query = "{\n q(func: eq(name, \"Alice\")) {\n name\n uid\n}\n}";
    dgraphClient.newTransaction().query(query);
  }

  @Test
  public void testOpencensusJaeger() {
    // 1. configure the jaeger exporter
    JaegerTraceExporter.createAndRegister(JAEGER_COLLECTOR, "my-service");

    // 2. Configure 100% sample rate, otherwise, few traces will be sampled.
    TraceConfig traceConfig = Tracing.getTraceConfig();
    TraceParams activeTraceParams = traceConfig.getActiveTraceParams();
    traceConfig.updateActiveTraceParams(
        activeTraceParams.toBuilder().setSampler(Samplers.alwaysSample()).build());

    // 3. Get the global singleton Tracer object.
    Tracer tracer = Tracing.getTracer();

    // 4. Create a scoped span, a scoped span will automatically end when closed.
    // It implements AutoClosable, so it'll be closed when the try block ends.
    try (Scope ignored = tracer.spanBuilder("test-span").startScopedSpan()) {
      tracer.getCurrentSpan().addAnnotation("test annotation");
      runTransactions();
    }

    // 5. Gracefully shutdown the exporter, so that it'll flush queued traces to Jaeger.
    Future<?> shutdownFuture =
        SHUTDOWN_EXECUTER.submit(() -> Tracing.getExportComponent().shutdown());

    try {
      shutdownFuture.get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // ignore
      System.out.println("Tracking export component shutdown got interrupted");
    } catch (ExecutionException e) {
      // ignore
      System.out.println("Tracking export component shutdown encountered an exception");
    } catch (TimeoutException e) {
      // ignore
      System.out.println("Tracking export component shutdown timed out after 10s");
    }
  }
}
