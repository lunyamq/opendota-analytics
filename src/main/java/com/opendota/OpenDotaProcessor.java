package com.opendota;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opendota.constants.AppConstants;
import com.opendota.model.HeroStats;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class OpenDotaProcessor {
    private static final Logger logger = LogManager.getLogger(OpenDotaProcessor.class);
    private final Properties properties;
    private final ObjectMapper objectMapper;
    private final SparkSession spark;

    public OpenDotaProcessor(SparkSession spark) {
        this.spark = spark;
        this.properties = new Properties();
        this.objectMapper = new ObjectMapper();
        loadProperties();
    }

    private void loadProperties() {
        try (InputStream input = getClass().getClassLoader()
                .getResourceAsStream(AppConstants.PROPERTIES_FILE)) {
            if (input != null) {
                properties.load(input);
                logger.info("Свойства загружены успешно");
            } else {
                logger.error("Файл свойств не найден");
            }
        } catch (Exception e) {
            logger.error("Ошибка загрузки свойств: {}", e.getMessage(), e);
        }
    }

    public void createTableIfNotExists() {
        try {
            String dbUrl = properties.getProperty(AppConstants.DB_URL, "jdbc:mysql://localhost:3306/opendota");
            String dbUser = properties.getProperty(AppConstants.DB_USER, "root");
            String dbPassword = properties.getProperty(AppConstants.DB_PASSWORD, "");

            try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
                 PreparedStatement stmt = conn.prepareStatement(AppConstants.CREATE_TABLE_SQL)) {
                stmt.execute();
                logger.info("Таблица создана или уже существует");
            }
        } catch (Exception e) {
            logger.error("Ошибка создания таблицы: {}", e.getMessage(), e);
        }
    }

    private void loadDataToSpark() {
        try {
            String dbUrl = properties.getProperty(AppConstants.DB_URL, "jdbc:mysql://localhost:3306/opendota");
            String dbUser = properties.getProperty(AppConstants.DB_USER, "root");
            String dbPassword = properties.getProperty(AppConstants.DB_PASSWORD, "");
            String dbTable = properties.getProperty(AppConstants.DB_TABLE, "hero_stats");

            Dataset<Row> heroesDF = spark.read()
                    .format("jdbc")
                    .option("url", dbUrl)
                    .option("dbtable", dbTable)
                    .option("user", dbUser)
                    .option("password", dbPassword)
                    .load();

            heroesDF.createOrReplaceTempView("heroes");
            logger.info("Загружено {} героев в Spark", heroesDF.count());

        } catch (Exception e) {
            logger.error("Ошибка загрузки данных в Spark: {}", e.getMessage(), e);
        }
    }

    private void saveToMySQL(Dataset<Row> df) {
        try {
            String dbUrl = properties.getProperty(AppConstants.DB_URL, "jdbc:mysql://localhost:3306/opendota");
            String dbUser = properties.getProperty(AppConstants.DB_USER, "root");
            String dbPassword = properties.getProperty(AppConstants.DB_PASSWORD, "");
            String dbTable = properties.getProperty(AppConstants.DB_TABLE, "hero_stats");

            df.write()
                    .mode(SaveMode.Overwrite)
                    .format("jdbc")
                    .option("url", dbUrl)
                    .option("dbtable", dbTable)
                    .option("user", dbUser)
                    .option("password", dbPassword)
                    .save();

            logger.info("Данные сохранены в MySQL");
            loadDataToSpark();

        } catch (Exception e) {
            logger.error("Ошибка сохранения в MySQL: {}", e.getMessage(), e);
        }
    }

    public boolean addHero(String jsonData) {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonData);
            createTableIfNotExists();
            loadDataToSpark();
            Dataset<Row> currentHeroes = spark.sql("SELECT * FROM heroes");

            List<Row> newRow = new ArrayList<>();
            newRow.add(RowFactory.create(
                    null,
                    jsonNode.has("id") ? jsonNode.get("id").asInt() : 0,
                    jsonNode.has("localized_name") ? jsonNode.get("localized_name").asText() : "",
                    jsonNode.has("base_health") ? jsonNode.get("base_health").asInt() : 0,
                    jsonNode.has("base_mana") ? jsonNode.get("base_mana").asInt() : 0,
                    jsonNode.has("base_attack_min") ? jsonNode.get("base_attack_min").asInt() : 0,
                    jsonNode.has("base_attack_max") ? jsonNode.get("base_attack_max").asInt() : 0,
                    jsonNode.has("move_speed") ? jsonNode.get("move_speed").asInt() : 0,
                    jsonNode.has("primary_attr") ? jsonNode.get("primary_attr").asText() : "",
                    jsonNode.has("roles") ? jsonNode.get("roles").asText() : "",
                    new java.sql.Timestamp(System.currentTimeMillis())
            ));

            org.apache.spark.sql.types.StructType schema = currentHeroes.schema();
            Dataset<Row> newHeroDF = spark.createDataFrame(newRow, schema);

            Dataset<Row> updatedHeroes = currentHeroes.union(newHeroDF);

            saveToMySQL(updatedHeroes);

            logger.info("Герой успешно добавлен");
            return true;

        } catch (Exception e) {
            logger.error("Ошибка добавления героя: {}", e.getMessage(), e);
            return false;
        }
    }

    public Dataset<Row> getAllHeroes(int limit) {
        try {
            loadDataToSpark();
            return spark.sql("SELECT hero_id, hero_name, base_health, base_mana, " +
                    "base_attack_min, base_attack_max, move_speed, primary_attribute " +
                    "FROM heroes ORDER BY hero_name LIMIT " + limit);
        } catch (Exception e) {
            logger.error("Ошибка получения героев: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }

    public Dataset<Row> getHeroById(int id) {
        try {
            loadDataToSpark();
            return spark.sql("SELECT * FROM heroes WHERE hero_id = " + id);
        } catch (Exception e) {
            logger.error("Ошибка поиска героя по ID: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }

    public Dataset<Row> findHeroesByName(String name) {
        try {
            loadDataToSpark();
            return spark.sql("SELECT hero_id, hero_name, base_health, move_speed, primary_attribute " +
                    "FROM heroes WHERE hero_name LIKE '%" + name + "%' ORDER BY hero_name");
        } catch (Exception e) {
            logger.error("Ошибка поиска героев по имени: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }

    public boolean updateHero(int heroId, String jsonData) {
        Connection conn = null;
        PreparedStatement checkStmt = null;
        PreparedStatement updateStmt = null;
        ResultSet rs = null;

        try {
            logger.info("Обновление героя {}", heroId);

            JsonNode jsonNode = objectMapper.readTree(jsonData);

            String dbUrl = properties.getProperty(AppConstants.DB_URL);
            String dbUser = properties.getProperty(AppConstants.DB_USER);
            String dbPassword = properties.getProperty(AppConstants.DB_PASSWORD);

            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);

            String checkSql = "SELECT * FROM hero_stats WHERE hero_id = ?";
            checkStmt = conn.prepareStatement(checkSql);
            checkStmt.setInt(1, heroId);
            rs = checkStmt.executeQuery();

            if (!rs.next()) {
                logger.error("Герой {} не найден", heroId);
                return false;
            }

            String currentName = rs.getString("hero_name");
            int currentHealth = rs.getInt("base_health");
            int currentMana = rs.getInt("base_mana");
            int currentAttackMin = rs.getInt("base_attack_min");
            int currentAttackMax = rs.getInt("base_attack_max");
            int currentMoveSpeed = rs.getInt("move_speed");
            String currentPrimary = rs.getString("primary_attribute");
            String currentRoles = rs.getString("roles");

            String newName = jsonNode.has("localized_name") ?
                    jsonNode.get("localized_name").asText() : currentName;
            int newHealth = jsonNode.has("base_health") ?
                    jsonNode.get("base_health").asInt() : currentHealth;
            int newMana = jsonNode.has("base_mana") ?
                    jsonNode.get("base_mana").asInt() : currentMana;
            int newAttackMin = jsonNode.has("base_attack_min") ?
                    jsonNode.get("base_attack_min").asInt() : currentAttackMin;
            int newAttackMax = jsonNode.has("base_attack_max") ?
                    jsonNode.get("base_attack_max").asInt() : currentAttackMax;
            int newMoveSpeed = jsonNode.has("move_speed") ?
                    jsonNode.get("move_speed").asInt() : currentMoveSpeed;
            String newPrimary = jsonNode.has("primary_attr") ?
                    jsonNode.get("primary_attr").asText() : currentPrimary;
            String newRoles = jsonNode.has("roles") ?
                    jsonNode.get("roles").toString() : currentRoles;

            String updateSql = "UPDATE hero_stats SET " +
                    "hero_name = ?, " +
                    "base_health = ?, " +
                    "base_mana = ?, " +
                    "base_attack_min = ?, " +
                    "base_attack_max = ?, " +
                    "move_speed = ?, " +
                    "primary_attribute = ?, " +
                    "roles = ? " +
                    "WHERE hero_id = ?";

            updateStmt = conn.prepareStatement(updateSql);
            updateStmt.setString(1, newName);
            updateStmt.setInt(2, newHealth);
            updateStmt.setInt(3, newMana);
            updateStmt.setInt(4, newAttackMin);
            updateStmt.setInt(5, newAttackMax);
            updateStmt.setInt(6, newMoveSpeed);
            updateStmt.setString(7, newPrimary);
            updateStmt.setString(8, newRoles);
            updateStmt.setInt(9, heroId);

            int rowsUpdated = updateStmt.executeUpdate();

            if (rowsUpdated > 0) {
                logger.info("Герой {} успешно обновлён. Обновлено {} строк", heroId, rowsUpdated);
                return true;
            } else {
                logger.error("Герой {} не был обновлён", heroId);
                return false;
            }

        } catch (Exception e) {
            logger.error("Ошибка обновления героя {}: {}", heroId, e.getMessage(), e);
            return false;
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
            try { if (checkStmt != null) checkStmt.close(); } catch (Exception ignored) {}
            try { if (updateStmt != null) updateStmt.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }
    }

    public boolean deleteHero(int heroId) {
        Connection conn = null;
        PreparedStatement checkStmt = null;
        PreparedStatement deleteStmt = null;
        ResultSet rs = null;

        try {
            logger.info("Удаление героя {}", heroId);

            String dbUrl = properties.getProperty(AppConstants.DB_URL);
            String dbUser = properties.getProperty(AppConstants.DB_USER);
            String dbPassword = properties.getProperty(AppConstants.DB_PASSWORD);

            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);

            String checkSql = "SELECT COUNT(*) as count FROM hero_stats WHERE hero_id = ?";
            checkStmt = conn.prepareStatement(checkSql);
            checkStmt.setInt(1, heroId);
            rs = checkStmt.executeQuery();

            rs.next();
            int count = rs.getInt("count");

            if (count == 0) {
                logger.warn("Герой {} не найден", heroId);
                return false;
            }

            String deleteSql = "DELETE FROM hero_stats WHERE hero_id = ?";
            deleteStmt = conn.prepareStatement(deleteSql);
            deleteStmt.setInt(1, heroId);

            int rowsDeleted = deleteStmt.executeUpdate();

            if (rowsDeleted > 0) {
                logger.info("Герой {} успешно удалён. Удалено {} строк", heroId, rowsDeleted);
                return true;
            } else {
                logger.error("Герой {} не был удалён", heroId);
                return false;
            }

        } catch (Exception e) {
            logger.error("Ошибка удаления героя {}: {}", heroId, e.getMessage(), e);
            return false;
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
            try { if (checkStmt != null) checkStmt.close(); } catch (Exception ignored) {}
            try { if (deleteStmt != null) deleteStmt.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }
    }

    public Dataset<Row> getAttributeStatistics() {
        try {
            loadDataToSpark();
            return spark.sql("SELECT primary_attribute, " +
                    "COUNT(*) as hero_count, " +
                    "AVG(base_health) as avg_health, " +
                    "AVG(base_mana) as avg_mana, " +
                    "AVG(move_speed) as avg_speed " +
                    "FROM heroes GROUP BY primary_attribute ORDER BY hero_count DESC");
        } catch (Exception e) {
            logger.error("Ошибка получения статистики: {}", e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }


    public void processHeroData() {
        try {
            logger.info("Начинаем обработку данных...");

            logger.info("Загружаем данные с OpenDota API...");
            String jsonData = downloadHeroData();

            if (jsonData == null || jsonData.isEmpty()) {
                logger.error("Не удалось загрузить данные");
                return;
            }

            logger.info("Парсим JSON данные...");
            List<HeroStats> heroStatsList = parseHeroData(jsonData);

            if (heroStatsList.isEmpty()) {
                logger.error("Нет данных для обработки");
                return;
            }

            logger.info("Создаем DataFrame...");
            Dataset<Row> heroDF = spark.createDataFrame(heroStatsList, HeroStats.class);

            logger.info("Пример данных:");
            heroDF.select("id", "localizedName", "baseHealth", "baseAttackMin",
                    "baseAttackMax", "moveSpeed", "primaryAttr").show(5, false);

            logger.info("Записываем в MySQL...");
            writeToMySQL(heroDF);

            logger.info("Обработка данных успешно завершена!");

        } catch (Exception e) {
            logger.error("Ошибка обработки данных: {}", e.getMessage(), e);
        }
    }

    private String downloadHeroData() {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(AppConstants.HERO_STATS_URL);
            request.addHeader("User-Agent", AppConstants.USER_AGENT);

            logger.info("Отправляем запрос: {}", AppConstants.HERO_STATS_URL);
            HttpResponse response = httpClient.execute(request);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                logger.info("Успешно загружено {} символов", responseBody.length());
                return responseBody;
            } else {
                logger.error("Ошибка HTTP: {}", statusCode);
                return null;
            }
        } catch (Exception e) {
            logger.error("Ошибка загрузки данных: {}", e.getMessage(), e);
            return null;
        }
    }

    private List<HeroStats> parseHeroData(String jsonData) {
        List<HeroStats> heroStatsList = new ArrayList<>();

        try {
            JsonNode rootNode = objectMapper.readTree(jsonData);

            for (JsonNode heroNode : rootNode) {
                HeroStats heroStats = new HeroStats();

                heroStats.setId(heroNode.has("id") ? heroNode.get("id").asInt() : 0);
                heroStats.setName(heroNode.has("name") ? heroNode.get("name").asText() : "");
                heroStats.setLocalizedName(heroNode.has("localized_name") ?
                        heroNode.get("localized_name").asText() : "");
                heroStats.setBaseHealth(heroNode.has("base_health") ?
                        heroNode.get("base_health").asInt() : 0);
                heroStats.setBaseMana(heroNode.has("base_mana") ?
                        heroNode.get("base_mana").asInt() : 0);
                heroStats.setBaseAttackMin(heroNode.has("base_attack_min") ?
                        heroNode.get("base_attack_min").asInt() : 0);
                heroStats.setBaseAttackMax(heroNode.has("base_attack_max") ?
                        heroNode.get("base_attack_max").asInt() : 0);
                heroStats.setMoveSpeed(heroNode.has("move_speed") ?
                        heroNode.get("move_speed").asInt() : 0);
                heroStats.setPrimaryAttr(heroNode.has("primary_attr") ?
                        heroNode.get("primary_attr").asText() : "");

                if (heroNode.has("roles")) {
                    List<String> rolesList = new ArrayList<>();
                    for (JsonNode roleNode : heroNode.get("roles")) {
                        rolesList.add(roleNode.asText());
                    }
                    heroStats.setRoles(rolesList.toArray(new String[0]));
                }

                heroStatsList.add(heroStats);
            }

            logger.info("Распаршено {} героев", heroStatsList.size());

        } catch (Exception e) {
            logger.error("Ошибка парсинга JSON: {}", e.getMessage(), e);
        }

        return heroStatsList;
    }

    private void writeToMySQL(Dataset<Row> heroDF) {
        try {
            String dbUrl = properties.getProperty(AppConstants.DB_URL, "jdbc:mysql://localhost:3306/opendota");
            String dbUser = properties.getProperty(AppConstants.DB_USER, "root");
            String dbPassword = properties.getProperty(AppConstants.DB_PASSWORD, "");
            String dbTable = properties.getProperty(AppConstants.DB_TABLE, "hero_stats");

            logger.info("Подключаемся к базе данных...");

            Dataset<Row> transformedDF = heroDF
                    .withColumnRenamed("id", "hero_id")
                    .withColumnRenamed("localizedName", "hero_name")
                    .withColumnRenamed("baseHealth", "base_health")
                    .withColumnRenamed("baseMana", "base_mana")
                    .withColumnRenamed("baseAttackMin", "base_attack_min")
                    .withColumnRenamed("baseAttackMax", "base_attack_max")
                    .withColumnRenamed("moveSpeed", "move_speed")
                    .withColumnRenamed("primaryAttr", "primary_attribute")
                    .withColumn("roles", org.apache.spark.sql.functions.array_join(
                            org.apache.spark.sql.functions.col("roles"), ","));

            createTableIfNotExists();

            Dataset<Row> columnsToWrite = transformedDF.select(
                    "hero_id",
                    "hero_name",
                    "base_health",
                    "base_mana",
                    "base_attack_min",
                    "base_attack_max",
                    "move_speed",
                    "primary_attribute",
                    "roles"
            );

            logger.info("Записываем данные в MySQL...");

            columnsToWrite.write()
                    .mode(SaveMode.Append)
                    .format("jdbc")
                    .option("url", dbUrl)
                    .option("dbtable", dbTable)
                    .option("user", dbUser)
                    .option("password", dbPassword)
                    .save();

            logger.info("Успешно сохранено {} записей в БД", transformedDF.count());

        } catch (Exception e) {
            logger.error(e);
        }
    }
}