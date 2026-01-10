package com.opendota;

import com.opendota.cli.CommandLineInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class App {
    private static final Logger LOGGER = LogManager.getLogger(App.class);

    public static void main(String[] args) {
        try (SparkSession spark = SparkSession.builder()
                .appName("OpenDotaAnalytics")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .config("spark.executor.extraJavaOptions",
                        "--add-opens java.base/java.lang=ALL-UNNAMED")
                .config("spark.driver.extraJavaOptions",
                        "--add-opens java.base/java.lang=ALL-UNNAMED")
                .getOrCreate()) {

            spark.sparkContext().setLogLevel("ERROR");
            System.setProperty("hadoop.home.dir", "C:\\hadoop");
            System.setProperty("io.netty.tryReflectionSetAccessible", "false");
            System.setProperty("sun.reflect.noInflation", "true");
            System.setProperty("sun.reflect.inflationThreshold", "0");
            System.setProperty("spark.ui.enabled", "false");
            System.setProperty("spark.ui.showConsoleProgress", "false");
            System.setProperty("log4j.logger.org.apache.hadoop.util.NativeCodeLoader", "ERROR");
            System.setProperty("spark.driver.userClassPathFirst", "false");
            System.setProperty("spark.executor.userClassPathFirst", "false");
            System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
            LOGGER.info("Запуск OpenDota Analytics...");

            if (args.length == 0) {
                CommandLineInterface.showHelp();
                return;
            }

            String command = args[0];
            OpenDotaProcessor processor = new OpenDotaProcessor(spark);

            switch (command) {
                case "--help":
                case "-h":
                    CommandLineInterface.showHelp();
                    break;

                case "--download":
                    downloadHeroes(processor);
                    break;

                case "--list":
                    listHeroes(processor, args);
                    break;

                case "--find":
                    findHeroes(processor, args);
                    break;

                case "--view":
                    viewHero(processor, args);
                    break;

                case "--add":
                    addHero(processor, args);
                    break;

                case "--update":
                    updateHero(processor, args);
                    break;

                case "--delete":
                    deleteHero(processor, args);
                    break;

                case "--stats":
                    showStats(processor);
                    break;

                case "--query":
                    executeQuery(spark, args);
                    break;

                default:
                    LOGGER.error("Неизвестная команда: {}", command);
                    CommandLineInterface.showHelp();
            }

        } catch (Exception e) {
            LOGGER.error("Ошибка приложения: {}", e.getMessage(), e);
        }
    }

    private static void downloadHeroes(OpenDotaProcessor processor) {
        LOGGER.info("Загружаем героев...");
        processor.processHeroData();
        LOGGER.info("Загрузка завершена");
    }

    private static void listHeroes(OpenDotaProcessor processor, String[] args) {
        int limit = args.length > 1 ? Integer.parseInt(args[1]) : 10;

        Dataset<Row> heroes = processor.getAllHeroes(limit);
        heroes.show(limit, false);
    }

    private static void findHeroes(OpenDotaProcessor processor, String[] args) {
        if (args.length < 2) {
            LOGGER.error("Использьзуйте --find <имя_героя>");
            return;
        }

        String name = args[1];
        Dataset<Row> heroes = processor.findHeroesByName(name);

        heroes.show(20, false);
    }

    private static void viewHero(OpenDotaProcessor processor, String[] args) {
        if (args.length < 2) {
            LOGGER.error("Используйте --view <id_героя>");
            return;
        }

        try {
            int id = Integer.parseInt(args[1]);
            Dataset<Row> hero = processor.getHeroById(id);

            if (hero.count() > 0) {
                hero.show(false);
            } else {
                LOGGER.info("Герой с ID {} не найден", id);
            }
        } catch (NumberFormatException e) {
            LOGGER.error("Неверный ID героя");
        }
    }

    private static void addHero(OpenDotaProcessor processor, String[] args) {
        if (args.length < 2) {
            LOGGER.error("Используйте --add <json_данные>");
            return;
        }

        String json = String.join(" ", Arrays.copyOfRange(args, 1, args.length));

        if (processor.addHero(json)) {
            LOGGER.info("Герой успешно добавлен");
        } else {
            LOGGER.error("Не удалось добавить героя");
        }
    }

    private static void updateHero(OpenDotaProcessor processor, String[] args) {
        if (args.length < 3) {
            LOGGER.error("Используйте --update <id_героя> <json_данные>");
            return;
        }

        try {
            int id = Integer.parseInt(args[1]);
            String json = String.join(" ", Arrays.copyOfRange(args, 2, args.length));

            if (processor.updateHero(id, json)) {
                LOGGER.info("Герой успешно обновлен");
            } else {
                LOGGER.error("Не удалось обновить героя");
            }
        } catch (Exception e) {
            LOGGER.error("Ошибка обновления: {}", e.getMessage());
        }
    }

    private static void deleteHero(OpenDotaProcessor processor, String[] args) {
        if (args.length < 2) {
            LOGGER.error("Используйте --delete <id_героя>");
            return;
        }

        try {
            int id = Integer.parseInt(args[1]);

            if (processor.deleteHero(id)) {
                LOGGER.info("Герой успешно удален");
            } else {
                LOGGER.error("Не удалось удалить героя");
            }
        } catch (NumberFormatException e) {
            LOGGER.error("Неверный ID");
        }
    }

    private static void showStats(OpenDotaProcessor processor) {
        LOGGER.info("\nСтатистика по атрибутам:");
        processor.getAttributeStatistics().show(false);
    }

    private static void executeQuery(SparkSession spark, String[] args) {
        if (args.length < 2) {
            LOGGER.error("Использование: --query <sql_запрос>");
            return;
        }

        String sql = String.join(" ", Arrays.copyOfRange(args, 1, args.length));

        try {
            spark.sql(sql).show(50, false);
        } catch (Exception e) {
            LOGGER.error("Ошибка выполнения запроса: {}", e.getMessage());
        }
    }
}