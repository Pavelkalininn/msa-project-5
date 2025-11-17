"""
Простой экспорт таблицы accounts_person из PostgreSQL в CSV
"""

import os
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def export_accounts_person():
    """Экспорт таблицы accounts_person в CSV"""

    # Параметры подключения из переменных окружения
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'password')
    }

    # Настройки выгрузки
    output_dir = os.getenv('OUTPUT_DIR', '/app/output')
    output_file = f"accounts_person_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    try:
        # Создаем подключение к БД
        connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_string)

        logger.info("Подключение к БД установлено")

        # SQL запрос для выгрузки всей таблицы
        query = "SELECT * FROM accounts_person"

        # Выполняем выгрузку в DataFrame
        logger.info("Начало выгрузки таблицы accounts_person...")
        df = pd.read_sql(query, engine)

        # Создаем директорию если не существует
        os.makedirs(output_dir, exist_ok=True)

        # Сохраняем в CSV
        output_path = os.path.join(output_dir, output_file)
        df.to_csv(output_path, index=False, encoding='utf-8')

        logger.info(f"Выгрузка завершена. Файл сохранен: {output_path}")
        logger.info(f"Количество выгруженных записей: {len(df)}")

        # Закрываем подключение
        engine.dispose()

        return True

    except Exception as e:
        logger.error(f"Ошибка при выгрузке: {e}")
        return False


if __name__ == "__main__":
    success = export_accounts_person()
    exit(0 if success else 1)