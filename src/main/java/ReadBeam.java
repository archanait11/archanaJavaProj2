import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

import java.util.Arrays;


public class ReadBeam {
    public static void main(String[] args) {

//        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
//        options.setJobName("archana-ganipineni-bq-java");
//        options.setProject("york-cdf-start");
//        options.setRegion("us-central1");
//        options.setRunner(DataflowRunner.class);
//        options.setTempLocation("gs://archana-python-bucket/temp/");

        DataflowPipelineOptions dataflowOptions =
                PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        dataflowOptions.setProject("york-cdf-start");
        dataflowOptions.setRegion("us-central1");
        dataflowOptions.setJobName("archana-java-read-1");
        dataflowOptions.setRunner(DataflowRunner.class);
        dataflowOptions.setGcpTempLocation("gs://archana-python-bucket/temp/");
        dataflowOptions.setTempLocation("gs://archana-python-bucket/temp/");

        Pipeline p = Pipeline.create(dataflowOptions);

        //Pipeline pipeline = Pipeline.create(dataflowOptions);

        String tableSpec = "york-cdf-start:aganipineni_proj_1.usd_customers";
        String out_tableSpec = "york-cdf-start:aganipineni_proj_1.java_output";
        TableSchema out_schema = new TableSchema()
                .setFields(Arrays.asList(
                        new TableFieldSchema().setName("first_name").setType("STRING").setMode("REQUIRED"),
                        new TableFieldSchema().setName("last_name").setType("STRING").setMode("REQUIRED")
                ));

        TableSchema schema = new TableSchema()
                .setFields(Arrays.asList(
                        new TableFieldSchema().setName("CUSTOMER_ID").setType("STRING").setMode("REQUIRED")
                ));


        PCollection<TableRow> rows =
                p.apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .fromQuery("SELECT first_name,last_name FROM `york-cdf-start.aganipineni_proj_1.usd_customers` ")
                                .usingStandardSql());

        rows.apply(
                "Write to BigQuery",
                BigQueryIO.writeTableRows()
                        .to("york-cdf-start:aganipineni_proj_1.java_output")
                        .withSchema(out_schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));


//        PCollection<TableRow> rows =
//                pipeline.apply(
//                        "Read from BigQuery query",
//                        BigQueryIO.readTableRows().from(tableSpec));

//        rows.apply("Write rows to bq",
//                BigQueryIO.writeTableRows()
//                        .to(String.format("york-cdf-start:aganipineni_proj_1.java_output")).withSchema(out_schema)
//                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
//                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
//        );

        p.run().waitUntilFinish();

    }
}
