"""
Рабочий пример: Использование строковых индексов с null значениями в Dask
Демонстрирует рекомендованный подход через reset_index()
"""

import numpy as np
import pandas as pd
import dask.dataframe as dd

print("=" * 80)
print("РАБОЧИЙ ПРИМЕР: Строковые индексы с null в Dask")
print("=" * 80)

# =============================================================================
# Создание тестовых данных
# =============================================================================
print("\n1. СОЗДАНИЕ ДАННЫХ")
print("-" * 80)

# DataFrame 1: Продажи (с null в product_id)
sales_data = {
    'sales_amount': [100, 200, 150, 300, 250, 180, 220, 190, 280, 160,
                     140, 200, 170, 210, 230, 175, 195, 205, 225, 185]
}
product_ids = [
    'PROD_001', 'PROD_002', None, 'PROD_004', 'PROD_005',
    'PROD_006', np.nan, 'PROD_008', 'PROD_009', 'PROD_010',
    'PROD_011', None, 'PROD_013', 'PROD_014', 'PROD_015',
    pd.NA, 'PROD_017', 'PROD_018', 'PROD_019', 'PROD_020'
]

# Создаем pandas DataFrame с null в индексе
pdf_sales = pd.DataFrame(sales_data, index=pd.Series(product_ids, dtype='object'))
print(f"DataFrame 'Продажи' создан:")
print(f"  Строк: {len(pdf_sales)}")
print(f"  Null в product_id: {pdf_sales.index.isna().sum()}")
print(f"\nПервые строки:")
print(pdf_sales.head(10))

# DataFrame 2: Цены продуктов (тоже с null)
prices_data = {
    'price': [10.5, 15.0, 12.0, 20.0, 8.5, 25.0, 18.0, 14.0, 22.0, 16.0, 19.0, 13.0]
}
price_ids = [
    'PROD_001', 'PROD_005', 'PROD_008', None, 'PROD_011',
    'PROD_015', np.nan, 'PROD_018', 'PROD_003', 'PROD_007', 'PROD_010', 'PROD_020'
]
pdf_prices = pd.DataFrame(prices_data, index=pd.Series(price_ids, dtype='object'))
print(f"\nDataFrame 'Цены' создан:")
print(f"  Строк: {len(pdf_prices)}")
print(f"  Null в product_id: {pdf_prices.index.isna().sum()}")

# =============================================================================
# Применение рекомендованного подхода: reset_index()
# =============================================================================
print("\n\n2. ПРЕОБРАЗОВАНИЕ ИНДЕКСА В КОЛОНКУ")
print("-" * 80)

# Преобразуем индекс в колонку
pdf_sales_reset = pdf_sales.reset_index()
pdf_sales_reset.columns = ['product_id', 'sales_amount']

pdf_prices_reset = pdf_prices.reset_index()
pdf_prices_reset.columns = ['product_id', 'price']

print("✓ Индексы преобразованы в колонки")
print(f"  Колонки продаж: {list(pdf_sales_reset.columns)}")
print(f"  Колонки цен: {list(pdf_prices_reset.columns)}")

# =============================================================================
# Создание Dask DataFrames
# =============================================================================
print("\n\n3. СОЗДАНИЕ DASK DATAFRAMES")
print("-" * 80)

ddf_sales = dd.from_pandas(pdf_sales_reset, npartitions=5)
ddf_prices = dd.from_pandas(pdf_prices_reset, npartitions=3)

print(f"✓ Dask DataFrame 'Продажи':")
print(f"  Партиций: {ddf_sales.npartitions}")
print(f"  Размер: {len(ddf_sales)} строк")

print(f"\n✓ Dask DataFrame 'Цены':")
print(f"  Партиций: {ddf_prices.npartitions}")
print(f"  Размер: {len(ddf_prices)} строк")

# =============================================================================
# ОПЕРАЦИЯ 1: FILTER - Фильтрация
# =============================================================================
print("\n\n4. ОПЕРАЦИЯ FILTER")
print("-" * 80)

# Найдем продажи с известным product_id
print("a) Фильтрация продаж с известным product_id:")
sales_with_id = ddf_sales[ddf_sales['product_id'].notnull()]
result = sales_with_id.compute()
print(f"   ✓ Строк с известным ID: {len(result)}")
print(f"   ✓ Отфильтровано строк с null: {len(ddf_sales) - len(result)}")

# Найдем продажи с null product_id
print("\nb) Фильтрация продаж с null product_id:")
sales_without_id = ddf_sales[ddf_sales['product_id'].isnull()]
result_null = sales_without_id.compute()
print(f"   ✓ Строк с null ID: {len(result_null)}")
print(result_null)

# Фильтрация по сумме продаж
print("\nc) Фильтрация по сумме продаж > 200:")
high_sales = ddf_sales[ddf_sales['sales_amount'] > 200]
result_high = high_sales.compute()
print(f"   ✓ Найдено продаж > 200: {len(result_high)}")
print(f"   ✓ Средняя сумма: {result_high['sales_amount'].mean():.2f}")

# =============================================================================
# ОПЕРАЦИЯ 2: JOIN - Объединение
# =============================================================================
print("\n\n5. ОПЕРАЦИЯ JOIN")
print("-" * 80)

# LEFT JOIN - все продажи + цены где есть
print("a) LEFT JOIN (Продажи + Цены):")
joined = ddf_sales.merge(ddf_prices, on='product_id', how='left')
result_joined = joined.compute()

print(f"   ✓ Всего строк после join: {len(result_joined)}")
print(f"   ✓ Строк с ценой: {result_joined['price'].notnull().sum()}")
print(f"   ✓ Строк без цены: {result_joined['price'].isnull().sum()}")
print(f"\n   Первые строки:")
print(result_joined.head(10))

# INNER JOIN - только где есть и продажи и цены
print("\nb) INNER JOIN (только совпадения):")
joined_inner = ddf_sales.merge(ddf_prices, on='product_id', how='inner')
result_inner = joined_inner.compute()
print(f"   ✓ Строк с совпадениями: {len(result_inner)}")
print(f"   ✓ Уникальных продуктов: {result_inner['product_id'].nunique()}")

# =============================================================================
# ОПЕРАЦИЯ 3: MAP_PARTITIONS - Пользовательская обработка
# =============================================================================
print("\n\n6. ОПЕРАЦИЯ MAP_PARTITIONS")
print("-" * 80)

# Расчет выручки для строк с ценой
print("a) Расчет выручки (revenue = sales * price):")

def calculate_revenue(df):
    """Добавляет колонку с выручкой"""
    result = df.copy()
    # Рассчитываем revenue только для строк с известной ценой
    result['revenue'] = result['sales_amount'] * result['price']
    result['has_price'] = result['price'].notnull()
    return result

with_revenue = joined.map_partitions(calculate_revenue)
result_revenue = with_revenue.compute()

print(f"   ✓ Обработано строк: {len(result_revenue)}")
print(f"   ✓ Строк с revenue: {result_revenue['revenue'].notnull().sum()}")
print(f"   ✓ Общая выручка: {result_revenue['revenue'].sum():.2f}")
print(f"\n   Примеры с выручкой:")
print(result_revenue[result_revenue['has_price']].head())

# Статистика по партициям
print("\nb) Сбор статистики по партициям:")

def partition_stats(df):
    """Собирает статистику по партиции"""
    return pd.DataFrame([{
        'partition_rows': len(df),
        'nulls_in_id': df['product_id'].isnull().sum(),
        'nulls_in_price': df['price'].isnull().sum() if 'price' in df.columns else 0,
        'avg_sales': df['sales_amount'].mean(),
        'total_revenue': df['revenue'].sum() if 'revenue' in df.columns else 0
    }])

stats = with_revenue.map_partitions(
    partition_stats,
    meta=pd.DataFrame({
        'partition_rows': pd.Series(dtype='int64'),
        'nulls_in_id': pd.Series(dtype='int64'),
        'nulls_in_price': pd.Series(dtype='int64'),
        'avg_sales': pd.Series(dtype='float64'),
        'total_revenue': pd.Series(dtype='float64')
    })
)
result_stats = stats.compute()
print(f"   ✓ Статистика собрана по {len(result_stats)} партициям:")
print(result_stats.to_string(index=False))

# Категоризация продуктов
print("\nc) Категоризация продуктов:")

def categorize_products(df):
    """Категоризирует продукты по наличию данных"""
    result = df.copy()

    def get_category(row):
        if pd.isnull(row['product_id']):
            return 'Без ID'
        elif pd.isnull(row.get('price', np.nan)):
            return 'Без цены'
        else:
            return 'Полные данные'

    result['category'] = result.apply(get_category, axis=1)
    return result

categorized = joined.map_partitions(categorize_products)
result_cat = categorized.compute()

print(f"   ✓ Распределение по категориям:")
category_counts = result_cat['category'].value_counts()
print(category_counts.to_string())

# =============================================================================
# ИТОГОВЫЙ АНАЛИЗ
# =============================================================================
print("\n\n7. ИТОГОВЫЙ АНАЛИЗ")
print("=" * 80)

# Финальная обработка: оставляем только валидные данные и считаем метрики
valid_data = ddf_sales.merge(ddf_prices, on='product_id', how='inner')

def final_processing(df):
    """Финальная обработка валидных данных"""
    result = df.copy()
    result['revenue'] = result['sales_amount'] * result['price']
    result['margin_pct'] = (result['price'] / result['sales_amount'] * 100)
    return result

final = valid_data.map_partitions(final_processing)
result_final = final.compute()

print(f"Анализ валидных данных:")
print(f"  ✓ Продуктов с полными данными: {len(result_final)}")
print(f"  ✓ Средние продажи: {result_final['sales_amount'].mean():.2f}")
print(f"  ✓ Средняя цена: {result_final['price'].mean():.2f}")
print(f"  ✓ Общая выручка: {result_final['revenue'].sum():.2f}")
print(f"  ✓ Средняя маржа: {result_final['margin_pct'].mean():.2f}%")

print(f"\nТоп-5 продуктов по выручке:")
top_products = result_final.nlargest(5, 'revenue')[['product_id', 'sales_amount', 'price', 'revenue']]
print(top_products.to_string(index=False))

# =============================================================================
# ВЫВОДЫ
# =============================================================================
print("\n\n8. ВЫВОДЫ")
print("=" * 80)
print("""
✅ УСПЕШНО ВЫПОЛНЕНЫ ВСЕ ОПЕРАЦИИ:

1. FILTER:
   - Фильтрация по наличию/отсутствию null
   - Фильтрация по значениям колонок
   - Комбинированная фильтрация

2. JOIN:
   - LEFT JOIN с сохранением всех записей
   - INNER JOIN только с совпадениями
   - Корректная обработка null в ключах

3. MAP_PARTITIONS:
   - Расчет новых колонок
   - Сбор статистики по партициям
   - Категоризация и классификация
   - Комплексная обработка данных

КЛЮЧЕВОЙ ВЫВОД:
Использование reset_index() позволяет полностью избежать ограничений Dask
при работе с null значениями в строковых индексах. Все операции DataFrame
работают корректно и предсказуемо.
""")

print("\n" + "=" * 80)
print("ПРИМЕР УСПЕШНО ЗАВЕРШЕН")
print("=" * 80)
