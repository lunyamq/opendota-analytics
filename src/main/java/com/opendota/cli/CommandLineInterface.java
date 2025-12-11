package com.opendota.cli;

public class CommandLineInterface {
    public static void showHelp() {
        System.out.println("Commands:");
        System.out.println("  --help, -h           Показать эту таблицу");
        System.out.println("  --download           Скачать героев из API");
        System.out.println("  --list [limit]       Список героев (по умолч: 10)");
        System.out.println("  --find <name>        Найти героя по названию");
        System.out.println("  --view <id>          Найти героя по ID");
        System.out.println("  --add <json>         Добавить нового героя");
        System.out.println("  --update <id> <json> Обновить героя");
        System.out.println("  --delete <id>        Удалить героя");
        System.out.println("  --stats              Показать статистику");
        System.out.println("  --query <sql>        Выполнить SQL запрос");
        System.out.println("\nПримеры:");
        System.out.println("  java -jar app.jar --download");
        System.out.println("  java -jar app.jar --list 20");
        System.out.println("  java -jar app.jar --find \"Anti\"");
        System.out.println("  java -jar app.jar --view 1");
        System.out.println("  java -jar app.jar --add '{\"id\": 999, \"name\": \"test\"}'");
        System.out.println("  java -jar app.jar --update 1 '{\"move_speed\": 350}'");
        System.out.println("  java -jar app.jar --delete 1");
        System.out.println("  java -jar app.jar --query \"SELECT * FROM heroes WHERE move_speed > 300\"");
    }
}

// java --add-opens java.base/java.nio=ALL-UNNAMED -jar .\opendota-spark-1.0.jar --add '{\"id\": 999, \"name\": \"Test Hero\"}'
// java --add-opens java.base/java.nio=ALL-UNNAMED -jar .\opendota-spark-1.0.jar --update 999 '{\"move_speed\": 350}'