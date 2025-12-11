package com.opendota;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class OpenDotaProcessorSimpleTest {

    private static SparkSession spark;
    private OpenDotaProcessor processor;

    @BeforeAll
    static void setupSpark() {
        spark = SparkSession.builder()
                .appName("OpenDotaSimpleTest")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @BeforeEach
    void setup() {
        processor = new OpenDotaProcessor(spark);
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @DisplayName("Тест: процессор создается без ошибок")
    void testProcessorCreation() {
        assertNotNull(processor, "Процессор должен создаваться");
        assertNotNull(spark, "Spark сессия должна быть создана");
    }

    @Test
    @DisplayName("Тест: таблица создается без ошибок")
    void testTableCreation() {
        assertDoesNotThrow(() -> {
            processor.createTableIfNotExists();
        }, "Создание таблицы не должно падать");
    }
}