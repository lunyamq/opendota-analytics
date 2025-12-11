# OpenDota Data Processor

Проект для обработки и визуализации данных о героях Dota 2 из OpenDota API.

## Описание
Приложение загружает статистику героев Dota 2, сохраняет в MySQL базу данных и предоставляет инструменты для анализа через Spark и Grafana.

## Возможности
- **CRUD операции** с героями (добавление, чтение, обновление, удаление)
- **Загрузка данных** с OpenDota API
- **Сохранение** в MySQL базу данных
- **Обработка данных** через Apache Spark
- **Визуализация** в Grafana

## Технологии
- Java 8+
- Apache Spark
- MySQL
- OpenDota API
- Grafana (для визуализации)

## Структура базы данных
Таблица `hero_stats` содержит:
- `hero_id` - ID героя
- `hero_name` - Имя героя
- `base_health` - Базовое здоровье
- `base_mana` - Базовая мана
- `base_attack_min` - Минимальная атака
- `base_attack_max` - Максимальная атака
- `move_speed` - Скорость передвижения
- `primary_attribute` - Основной атрибут (agi/int/str/all)

## Быстрый старт
1. Установите MySQL и создайте базу opendota
2. Настройте подключение в application.properties 
3. Запустите OpenDotaProcessor.processHeroData()
4. Настройте Grafana с подключением к MySQL

## Конфигурация
Файл application.properties:
```properties
db.url=jdbc:mysql://localhost:3306/opendota_db?useSSL=false&serverTimezone=UTC
db.user=root
db.password=
db.table=hero_stats
```

## CLI
```bash
# Помощь
java -jar opendota.jar --help

# Скачать данные
java -jar opendota.jar --download

# Показать 20 героев
java -jar opendota.jar --list 20

# Найти героя по имени
java -jar opendota.jar --find "Anti-Mage"

# Найти героя по ID
java -jar opendota.jar --view 1

# Добавить героя (JSON)
java -jar opendota.jar --add '{"id": 999, "localized_name": "Новый герой", "base_health": 600}'

# Обновить героя
java -jar opendota.jar --update 1 '{"base_health": 700, "move_speed": 320}'

# Удалить героя
java -jar opendota.jar --delete 1
```