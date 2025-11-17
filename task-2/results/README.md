### Функциональные требования:

1. Один раз в сутки (ночью) необходимо выгружать данные из одной базы данных
2. Необходимо сохранять все выгруженные и агрегированные данные в csv файл

### Нефункциональные требования
1. Для для аггрегирования данных используется четыре таблицы
2. Ожидаемое количество строк в самой большой таблице - 20 000 строк


|                                                                                         | SpringBatch | ApacheAirflow | K8s Job | Spark   |
|-----------------------------------------------------------------------------------------|-------------|---------------|---------|---------|
| Наличие конфигурации CRON-расписания                                                    | +           | +             | +       | +       | 	
| Сложность реализации логики по обработке данных                                         | средняя     | низкая        | низкая  | высокая |	
| Ресурсоемкость решения (количество потребляемых ресурсов)                               | средняя     | низкая        | высокая | средняя |
| Масштабируемость решения под нагрузкой и сложность реализации                           | нет         | есть          | есть    | нет     |
| Сложность развертывания в облаке и интеграция с имеющейся микросервисной  архитектурой  | сложно      | легко         | средне  | легко   |	
| Удобство интеграции с системами логирования и мониторинга                               | низкое      | высокое       | высокое | низкое  |

### Описание решения

При текущих требованиях нет необходимости реализовывать сложное решение, 
имеющее возможность ветвления задач, Наиболее простое решение для настройки, 
при условии наличия опыта работы с kubernetes является k8s JOB.

Для настройки периодичности задачи достаточно опистать файл CronJob запускающий python script

    apiVersion: batch/v1
    kind: CronJob
    metadata:
      name: daily-etl
    spec:
      schedule: "0 2 * * *"   # запуск каждый день в 2:00
      concurrencyPolicy: Forbid   # не запускать, если старая ещё работает
      timeZone: "Europe/Moscow"  # поддержка часовых поясов с k8s 1.27+
      startingDeadlineSeconds: 200
      successfulJobsHistoryLimit: 2
      failedJobsHistoryLimit: 2
      jobTemplate:
        spec:
          template:
            spec:
              containers:
              - name: etl
                image: my-etl:latest
                command: ["python", "etl.py"]

Чего достаточно для текущей задачи. 

В python файле описать 

"""
ETL скрипт для генерации прайс-листов из PostgreSQL в CSV
Запускается ежедневно в 6:00 через k8s CronJob

    import os
    import logging
    import pandas as pd
    from sqlalchemy import create_engine, text
    from datetime import datetime
    import sys
    
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    logger = logging.getLogger(__name__)
    
    class PriceListGenerator:
        def __init__(self):
            # Получение параметров подключения из переменных окружения
            self.db_host = os.getenv('DB_HOST', 'localhost')
            self.db_port = os.getenv('DB_PORT', '5432')
            self.db_name = os.getenv('DB_NAME', 'shop_db')
            self.db_user = os.getenv('DB_USER', 'postgres')
            self.db_password = os.getenv('DB_PASSWORD', 'password')
            
            self.output_dir = os.getenv('OUTPUT_DIR', '/app/output')
            self.date_str = datetime.now().strftime('%Y%m%d')
            
            # SQL запрос для получения данных прайс-листа
            self.price_query = """
            SELECT 
                p.id as product_id,
                p.name as product_name,
                p.sku,
                p.description,
                c.name as category_name,
                cl.name as client_name,
                cl.id as client_id,
                COALESCE(cp.price, p.base_price) as final_price,
                p.stock_quantity,
                p.is_active,
                p.created_at,
                p.updated_at
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            CROSS JOIN clients cl
            LEFT JOIN client_prices cp ON cp.product_id = p.id AND cp.client_id = cl.id
            WHERE p.is_active = true
              AND p.stock_quantity > 0
              AND cl.is_active = true
            ORDER BY cl.name, c.name, p.name
            """
        
        def create_db_connection(self):
            """Создание подключения к PostgreSQL"""
            try:
                connection_string = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
                engine = create_engine(connection_string)
                logger.info(f"✅ Подключение к БД установлено: {self.db_host}:{self.db_port}/{self.db_name}")
                return engine
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к БД: {e}")
                raise
        
        def extract_data(self, engine):
            """Извлечение данных из БД"""
            try:
                logger.info("📊 Начало извлечения данных из БД...")
                
                with engine.connect() as conn:
                    # Выполняем запрос и загружаем в DataFrame
                    df = pd.read_sql(text(self.price_query), conn)
                    
                logger.info(f"✅ Данные извлечены успешно. Получено {len(df)} строк")
                return df
                
            except Exception as e:
                logger.error(f"❌ Ошибка при извлечении данных: {e}")
                raise
        
        def transform_data(self, df):
            """Трансформация данных (минимальная, по условию задачи)"""
            try:
                logger.info("🔄 Начало трансформации данных...")
                
                # Базовая очистка и форматирование
                df_clean = df.copy()
                
                # Заполнение пропущенных цен базовой ценой
                df_clean['final_price'] = df_clean['final_price'].fillna(df_clean['final_price'].mean())
                
                # Форматирование числовых полей
                df_clean['final_price'] = df_clean['final_price'].round(2)
                
                # Добавление временной метки
                df_clean['export_date'] = datetime.now().date()
                df_clean['export_timestamp'] = datetime.now()
                
                logger.info(f"✅ Трансформация завершена. Обработано {len(df_clean)} строк")
                return df_clean
                
            except Exception as e:
                logger.error(f"❌ Ошибка при трансформации данных: {e}")
                raise
        
        def generate_client_price_lists(self, df):
            """Генерация отдельных CSV файлов для каждого клиента"""
            try:
                logger.info("📁 Генерация прайс-листов по клиентам...")
                
                # Создаем директорию для выгрузки
                os.makedirs(self.output_dir, exist_ok=True)
                
                clients = df['client_id'].unique()
                generated_files = []
                
                for client_id in clients:
                    client_data = df[df['client_id'] == client_id]
                    client_name = client_data['client_name'].iloc[0]
                    
                    # Создаем безопасное имя файла
                    safe_client_name = "".join(c if c.isalnum() else "_" for c in client_name)
                    filename = f"price_list_{safe_client_name}_{self.date_str}.csv"
                    filepath = os.path.join(self.output_dir, filename)
                    
                    # Сохраняем CSV
                    client_data.to_csv(filepath, index=False, encoding='utf-8')
                    generated_files.append(filepath)
                    
                    logger.info(f"   ✅ Сгенерирован файл для {client_name}: {filename} ({len(client_data)} товаров)")
                
                # Также создаем общий файл
                common_filename = f"price_list_all_clients_{self.date_str}.csv"
                common_filepath = os.path.join(self.output_dir, common_filename)
                df.to_csv(common_filepath, index=False, encoding='utf-8')
                generated_files.append(common_filepath)
                
                logger.info(f"✅ Всего сгенерировано {len(generated_files)} файлов")
                return generated_files
                
            except Exception as e:
                logger.error(f"❌ Ошибка при генерации файлов: {e}")
                raise
        
        def generate_summary_report(self, df, generated_files):
            """Генерация отчета о выгрузке"""
            try:
                report = {
                    'export_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'total_products': df['product_id'].nunique(),
                    'total_clients': df['client_id'].nunique(),
                    'total_categories': df['category_name'].nunique(),
                    'total_records': len(df),
                    'average_price': df['final_price'].mean(),
                    'min_price': df['final_price'].min(),
                    'max_price': df['final_price'].max(),
                    'generated_files': len(generated_files),
                    'output_directory': self.output_dir
                }
                
                # Сохраняем отчет
                report_file = os.path.join(self.output_dir, f"export_report_{self.date_str}.txt")
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write("ОТЧЕТ О ВЫГРУЗКЕ ПРАЙС-ЛИСТОВ\n")
                    f.write("=" * 50 + "\n")
                    for key, value in report.items():
                        f.write(f"{key}: {value}\n")
                
                logger.info("📋 Отчет о выгрузке сгенерирован")
                return report
                
            except Exception as e:
                logger.error(f"❌ Ошибка при генерации отчета: {e}")
                raise
        
        def run_etl(self):
            """Основной метод выполнения ETL процесса"""
            logger.info("🚀 Запуск ETL процесса генерации прайс-листов")
            
            engine = None
            try:
                # Подключаемся к БД
                engine = self.create_db_connection()
                
                # ETL процесс
                raw_data = self.extract_data(engine)
                transformed_data = self.transform_data(raw_data)
                generated_files = self.generate_client_price_lists(transformed_data)
                report = self.generate_summary_report(transformed_data, generated_files)
                
                logger.info("🎉 ETL процесс успешно завершен!")
                logger.info(f"📊 Итоги: {report['total_records']} записей, {report['total_clients']} клиентов")
                
                return True
                
            except Exception as e:
                logger.error(f"💥 ETL процесс завершен с ошибкой: {e}")
                return False
                
            finally:
                if engine:
                    engine.dispose()
                    logger.info("🔌 Подключение к БД закрыто")
    
    def main():
        """Точка входа в приложение"""
        generator = PriceListGenerator()
        success = generator.run_etl()
        
        # Возвращаем код выхода для k8s
        sys.exit(0 if success else 1)
    
    if __name__ == "__main__":
        main()

Dockerfile

```dockerfile
    FROM python:3.11-slim
    WORKDIR /app
    COPY requirements.txt .
    RUN pip install --no-cache-dir -r requirements.txt
    COPY price_list_generator.py .
    RUN mkdir -p /app/output
    CMD ["python", "price_list_generator.py"]

```

requirements.txt

```text
    pandas>=1.5.0
    sqlalchemy>=1.4.0
    psycopg2-binary>=2.9.0
```


job.yaml

    apiVersion: batch/v1
    kind: CronJob
    metadata:
      name: price-list-generator
    spec:
      schedule: "0 6 * * *"  # Каждый день в 6:00
      concurrencyPolicy: Forbid
      jobTemplate:
        spec:
          template:
            spec:
              containers:
              - name: price-generator
                image: your-registry/price-list-generator:latest
                env:
                - name: DB_HOST
                  value: "postgresql-service"
                - name: DB_PORT
                  value: "5432"
                - name: DB_NAME
                  value: "shop_db"
                - name: DB_USER
                  valueFrom:
                    secretKeyRef:
                      name: db-credentials
                      key: username
                - name: DB_PASSWORD
                  valueFrom:
                    secretKeyRef:
                      name: db-credentials
                      key: password
                - name: OUTPUT_DIR
                  value: "/app/output"
                volumeMounts:
                - name: output-volume
                  mountPath: /app/output
              volumes:
              - name: output-volume
                persistentVolumeClaim:
                  claimName: price-list-pvc
              restartPolicy: OnFailure

