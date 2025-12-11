package com.opendota.constants;

public class AppConstants {
    private AppConstants() { }

    public static final String USER_AGENT = "OpenDota-Spark-Processor/1.0";
    public static final String PROPERTIES_FILE = "application.properties";

    public static final String API_BASE_URL = "https://api.opendota.com/api";
    public static final String HERO_STATS_URL = API_BASE_URL + "/heroStats";

    public static final String DB_URL = "db.url";
    public static final String DB_USER = "db.user";
    public static final String DB_PASSWORD = "db.password";
    public static final String DB_TABLE = "db.table";


    public static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS hero_stats (" +
                    "id INT PRIMARY KEY AUTO_INCREMENT, " +
                    "hero_id INT NOT NULL, " +
                    "hero_name VARCHAR(100), " +
                    "base_health INT DEFAULT 0, " +
                    "base_mana INT DEFAULT 0, " +
                    "base_attack_min INT DEFAULT 0, " +
                    "base_attack_max INT DEFAULT 0, " +
                    "move_speed INT DEFAULT 0, " +
                    "primary_attribute VARCHAR(20), " +
                    "roles VARCHAR(200), " +
                    "processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                    "UNIQUE KEY unique_hero (hero_id)" +
                    ")";
}