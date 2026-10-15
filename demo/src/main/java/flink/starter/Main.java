package flink.starter;

import java.time.Duration;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.api.common.functions.MapFunction;

// import org.apache.flink.table.api.DataTypes;
// import org.apache.flink.table.api.EnvironmentSettings;
// import static org.apache.flink.table.api.Expressions.$;
// import org.apache.flink.table.api.Schema;
// import org.apache.flink.table.api.Table;
// import org.apache.flink.table.api.TableDescriptor;
// import org.apache.flink.table.api.TableEnvironment;
// import org.apache.flink.table.expressions.TimeIntervalUnit;
public class Main {

    static final String BROKERS = "localhost:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("Environment created");
        KafkaSource<Weather> source = KafkaSource.<Weather>builder()
                .setBootstrapServers(BROKERS)
                .setProperty("partition.discovery.interval.ms", "1000")
                .setTopics("weather")
                .setGroupId("groupdId-919292")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new WeatherDeserializationSchema())
                .build();

        DataStreamSource<Weather> kafka = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka");

        System.out.println("Kafka source created");

        DataStream<Tuple2<MyAverage, Double>> averageTemperatureStream = kafka.keyBy(myEvent -> myEvent.city)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(60)))
                .aggregate(new AverageAggregator());

        DataStream<Tuple2<String, Double>> cityAndValueStream = averageTemperatureStream
                .map(new MapFunction<Tuple2<MyAverage, Double>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple2<MyAverage, Double> input) throws Exception {
                        return new Tuple2<>(input.f0.city, input.f1);
                    }
                });

        System.out.println("Aggregation created");

        cityAndValueStream.sinkTo(
                JdbcSink.<Tuple2<String, Double>>builder()
                        .withQueryStatement(
                                "insert into weather (city, average_temperature) values (?, ?)",
                                (statement, event) -> {
                                    statement.setString(1, event.f0); // event.f0 là City (String)
                                    statement.setDouble(2, event.f1); // event.f1 là Average Temperature (Double)
                                })
                        .withExecutionOptions(JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build())
                        .buildAtLeastOnce(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:postgresql://localhost:5432/spring_starter")
                                .withDriverName("org.postgresql.Driver")
                                .withUsername("postgres")
                                .withPassword("postgres")
                                .build())
        );

        env.execute("Kafka-flink-postgres");
    }

    /**
     * Aggregation function for average.
     */
    public static class AverageAggregator implements AggregateFunction<Weather, MyAverage, Tuple2<MyAverage, Double>> {

        @Override
        public MyAverage createAccumulator() {
            return new MyAverage();
        }

        @Override
        public MyAverage add(Weather weather, MyAverage myAverage) {
            //logger.debug("add({},{})", myAverage.city, myEvent);
            myAverage.city = weather.city;
            myAverage.count = myAverage.count + 1;
            myAverage.sum = myAverage.sum + weather.temperature;
            return myAverage;
        }

        @Override
        public Tuple2<MyAverage, Double> getResult(MyAverage myAverage) {
            return new Tuple2<>(myAverage, myAverage.sum / myAverage.count);
        }

        @Override
        public MyAverage merge(MyAverage myAverage, MyAverage acc1) {
            myAverage.sum = myAverage.sum + acc1.sum;
            myAverage.count = myAverage.count + acc1.count;
            return myAverage;
        }
    }

    public static class MyAverage {

        public String city;
        public Integer count = 0;
        public Double sum = 0d;

        @Override
        public String toString() {
            return "MyAverage{"
                    + "city='" + city + '\''
                    + ", count=" + count
                    + ", sum=" + sum
                    + '}';
        }
    }

    // public static void main(String[] args) throws Exception {
    //     EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
    //     TableEnvironment tEnv = TableEnvironment.create(settings);
    //     tEnv.createTemporaryTable("transactions",
    //         TableDescriptor.forConnector("datagen")
    //             .schema(Schema.newBuilder()
    //                 .column("accountId", DataTypes.BIGINT())
    //                 .column("amount", DataTypes.BIGINT())
    //                 .column("transactionTime", DataTypes.TIMESTAMP(3))
    //                 .build())
    //             .option("rows-per-second", "100")
    //             .option("fields.accountId.min", "1")
    //             .option("fields.accountId.max", "5")
    //             .option("fields.amount.min", "1")
    //             .option("fields.amount.max", "1000")
    //             .build());
    //     Table transactions = tEnv.from("transactions");
    //     Table result = report(transactions);
    //     result.execute().print();
    // }
    // public static Table report(Table transactions) {
    //     return transactions
    //         .select(
    //             $("accountId"),
    //             $("transactionTime").floor(TimeIntervalUnit.HOUR).as("logTs"),
    //             $("amount"))
    //         .groupBy($("accountId"), $("logTs"))
    //         .select(
    //             $("accountId"),
    //             $("logTs"),
    //             $("amount").sum().as("amount"));
    // }
}
