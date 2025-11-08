"""
Расширенное исследование использования индексов на колонках с типом "строка",
имеющих пустые значения (NaN, NA, None) в Dask DataFrame.

Включает обходные пути и различные сценарии.
"""

import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.dataframe.utils import assert_eq

print("=" * 80)
print("ИССЛЕДОВАНИЕ: Строковые индексы с null-значениями в Dask DataFrame")
print("=" * 80)

# =============================================================================
# 1. Демонстрация ограничения Dask с null в строковых индексах
# =============================================================================
print("\n1. ОГРАНИЧЕНИЕ DASK: Non-numeric index с null значениями")
print("-" * 80)

# Создаем pandas DataFrame с null в строковом индексе
data = {
    'value': range(25),
    'category': ['A', 'B', 'C', 'D', 'E'] * 5,
    'numeric': np.random.randn(25)
}

index_values = [
    'item_0', 'item_1', None, 'item_3', np.nan,
    'item_5', 'item_6', 'item_7', None, 'item_9',
    pd.NA, 'item_11', 'item_12', 'item_13', np.nan,
    'item_15', None, 'item_17', 'item_18', 'item_19',
    'item_20', 'item_21', pd.NA, 'item_23', 'item_24'
]

pdf_with_nulls = pd.DataFrame(data, index=pd.Series(index_values, dtype='object'))

print(f"Pandas DataFrame с null в индексе:")
print(f"  - Форма: {pdf_with_nulls.shape}")
print(f"  - Тип индекса: {pdf_with_nulls.index.dtype}")
print(f"  - Null значений в индексе: {pdf_with_nulls.index.isna().sum()}")

print("\nПопытка создать Dask DataFrame:")
try:
    ddf = dd.from_pandas(pdf_with_nulls, npartitions=5)
    print("  ✓ Успешно (неожиданно!)")
except NotImplementedError as e:
    print(f"  ✗ NotImplementedError (ожидаемо): {e}")
except Exception as e:
    print(f"  ✗ Другая ошибка: {type(e).__name__}: {e}")

# =============================================================================
# 2. ОБХОДНОЙ ПУТЬ 1: Фильтрация null значений перед созданием Dask DataFrame
# =============================================================================
print("\n\n2. ОБХОДНОЙ ПУТЬ 1: Фильтрация null значений")
print("-" * 80)

# Фильтруем строки с null индексом
pdf_filtered = pdf_with_nulls.loc[~pdf_with_nulls.index.isna()]
print(f"После фильтрации:")
print(f"  - Исходных строк: {len(pdf_with_nulls)}")
print(f"  - После фильтрации: {len(pdf_filtered)}")
print(f"  - Удалено строк: {len(pdf_with_nulls) - len(pdf_filtered)}")

try:
    ddf_filtered = dd.from_pandas(pdf_filtered, npartitions=5)
    print(f"  ✓ Dask DataFrame создан успешно")
    print(f"  - Партиций: {ddf_filtered.npartitions}")
    print(f"  - Known divisions: {ddf_filtered.known_divisions}")

    # Тестируем операции на отфильтрованном DataFrame
    print("\n  Тестируем операции на отфильтрованном DataFrame:")

    # Filter
    print("    a) Filter по категории:")
    result = ddf_filtered[ddf_filtered['category'] == 'A'].compute()
    print(f"       ✓ Результат: {len(result)} строк")

    # Group by
    print("    b) GroupBy:")
    grouped = ddf_filtered.groupby('category')['value'].sum().compute()
    print(f"       ✓ Результат: {dict(grouped)}")

except Exception as e:
    print(f"  ✗ Ошибка: {type(e).__name__}: {e}")

# =============================================================================
# 3. ОБХОДНОЙ ПУТЬ 2: Замена null на строковое значение
# =============================================================================
print("\n\n3. ОБХОДНОЙ ПУТЬ 2: Замена null на строковое значение")
print("-" * 80)

# Заменяем null на специальное значение
pdf_replaced = pdf_with_nulls.copy()
pdf_replaced.index = pdf_replaced.index.fillna('__NULL__')
print(f"После замены null на '__NULL__':")
print(f"  - Null значений в индексе: {pdf_replaced.index.isna().sum()}")
print(f"  - Значений '__NULL__': {(pdf_replaced.index == '__NULL__').sum()}")

try:
    ddf_replaced = dd.from_pandas(pdf_replaced, npartitions=5)
    print(f"  ✓ Dask DataFrame создан успешно")
    print(f"  - Партиций: {ddf_replaced.npartitions}")

    print("\n  Операции с заменой null:")

    # Найти строки с '__NULL__' индексом
    print("    a) Поиск строк с '__NULL__' индексом:")
    null_rows = ddf_replaced[ddf_replaced.index == '__NULL__'].compute()
    print(f"       ✓ Найдено: {len(null_rows)} строк")

    # map_partitions для восстановления null
    def restore_nulls(df):
        df_copy = df.copy()
        df_copy.index = df_copy.index.where(df_copy.index != '__NULL__', np.nan)
        return df_copy

    print("    b) map_partitions для восстановления null:")
    restored = ddf_replaced.map_partitions(restore_nulls)
    result_restored = restored.compute()
    print(f"       ✓ Восстановлено null в индексе: {result_restored.index.isna().sum()}")

except Exception as e:
    print(f"  ✗ Ошибка: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# 4. ОБХОДНОЙ ПУТЬ 3: Reset index и работа с колонкой
# =============================================================================
print("\n\n4. ОБХОДНОЙ ПУТЬ 3: Reset index и работа с колонкой")
print("-" * 80)

# Сбрасываем индекс в колонку
pdf_reset = pdf_with_nulls.reset_index()
pdf_reset.columns = ['original_index', 'value', 'category', 'numeric']
print(f"После reset_index:")
print(f"  - Колонки: {list(pdf_reset.columns)}")
print(f"  - Null в 'original_index': {pdf_reset['original_index'].isna().sum()}")
print(pdf_reset.head(10))

try:
    ddf_reset = dd.from_pandas(pdf_reset, npartitions=5)
    print(f"  ✓ Dask DataFrame создан успешно")

    print("\n  Операции с null в колонке:")

    # Filter
    print("    a) Фильтрация строк с null индексом:")
    null_indices = ddf_reset[ddf_reset['original_index'].isna()].compute()
    print(f"       ✓ Найдено: {len(null_indices)} строк")

    # Join по колонке с null
    print("    b) Join по колонке с null значениями:")
    # Создаем второй DataFrame
    pdf2 = pd.DataFrame({
        'join_key': ['item_5', None, 'item_10', np.nan, 'item_15'],
        'extra': ['a', 'b', 'c', 'd', 'e']
    })
    ddf2 = dd.from_pandas(pdf2, npartitions=2)

    # Merge
    merged = ddf_reset.merge(ddf2, left_on='original_index', right_on='join_key', how='inner')
    result_merged = merged.compute()
    print(f"       ✓ Inner merge: {len(result_merged)} строк")
    print(result_merged)

    # map_partitions
    print("    c) map_partitions с обработкой null:")
    def count_nulls(df):
        return pd.Series({
            'total': len(df),
            'nulls_in_index': df['original_index'].isna().sum()
        })

    stats = ddf_reset.map_partitions(count_nulls, meta=pd.Series(dtype=object))
    result_stats = stats.compute()
    print(f"       ✓ Статистика по партициям:")
    for i, stat in enumerate(result_stats):
        print(f"         Партиция {i}: {stat}")

except Exception as e:
    print(f"  ✗ Ошибка: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# 5. СРАВНЕНИЕ: Числовой индекс с null значениями
# =============================================================================
print("\n\n5. СРАВНЕНИЕ: Числовой индекс с null значениями")
print("-" * 80)

# Создаем DataFrame с числовым индексом, содержащим null
numeric_index = [1.0, 2.0, np.nan, 4.0, 5.0,
                 6.0, np.nan, 8.0, 9.0, 10.0,
                 11.0, 12.0, np.nan, 14.0, 15.0,
                 16.0, 17.0, 18.0, np.nan, 20.0,
                 21.0, 22.0, 23.0, 24.0, 25.0]

pdf_numeric = pd.DataFrame(data, index=numeric_index)
print(f"Pandas DataFrame с числовым индексом и null:")
print(f"  - Тип индекса: {pdf_numeric.index.dtype}")
print(f"  - Null значений в индексе: {pdf_numeric.index.isna().sum()}")

try:
    ddf_numeric = dd.from_pandas(pdf_numeric, npartitions=5)
    print(f"  ✓ Dask DataFrame с числовым индексом и null создан!")
    print(f"  - Партиций: {ddf_numeric.npartitions}")
    print(f"  - Known divisions: {ddf_numeric.known_divisions}")

    # Операции
    print("\n  Операции с числовым индексом и null:")
    print("    a) Filter:")
    result = ddf_numeric[ddf_numeric['category'] == 'B'].compute()
    print(f"       ✓ {len(result)} строк")

    print("    b) map_partitions:")
    def process(df):
        df_copy = df.copy()
        df_copy['idx_is_null'] = df.index.isna()
        return df_copy
    processed = ddf_numeric.map_partitions(process)
    result_proc = processed.compute()
    print(f"       ✓ Обработано. Null индексов: {result_proc['idx_is_null'].sum()}")

except NotImplementedError as e:
    print(f"  ✗ NotImplementedError: {e}")
except Exception as e:
    print(f"  ✗ Ошибка: {type(e).__name__}: {e}")

# =============================================================================
# 6. ПРАКТИЧЕСКИЙ ПРИМЕР: Join, Filter, map_partitions с обходными путями
# =============================================================================
print("\n\n6. ПРАКТИЧЕСКИЙ ПРИМЕР: Полный workflow с null в индексе")
print("-" * 80)

try:
    # Используем подход с reset_index
    print("Сценарий: Есть два DataFrame с null в строковых индексах, нужно:")
    print("  1. Выполнить join")
    print("  2. Отфильтровать данные")
    print("  3. Обработать через map_partitions")

    # DataFrame 1
    df1_data = {'sales': np.random.randint(100, 1000, 20)}
    df1_index = [f'product_{i}' if i % 4 != 0 else None for i in range(20)]
    pdf1 = pd.DataFrame(df1_data, index=pd.Series(df1_index, dtype='object')).reset_index()
    pdf1.columns = ['product_id', 'sales']
    ddf1 = dd.from_pandas(pdf1, npartitions=5)

    # DataFrame 2
    df2_data = {'price': np.random.randint(10, 100, 15)}
    df2_index = [f'product_{i}' if i % 3 != 0 else np.nan for i in range(15)]
    pdf2 = pd.DataFrame(df2_data, index=pd.Series(df2_index, dtype='object')).reset_index()
    pdf2.columns = ['product_id', 'price']
    ddf2 = dd.from_pandas(pdf2, npartitions=5)

    print(f"\nDataFrame 1: {len(pdf1)} строк, null product_id: {pdf1['product_id'].isna().sum()}")
    print(f"DataFrame 2: {len(pdf2)} строк, null product_id: {pdf2['product_id'].isna().sum()}")

    # 1. JOIN
    print("\n1) Выполняем LEFT JOIN:")
    joined = ddf1.merge(ddf2, on='product_id', how='left')
    result_joined = joined.compute()
    print(f"   ✓ Результат: {len(result_joined)} строк")
    print(f"   - Строк с ценой: {result_joined['price'].notna().sum()}")
    print(f"   - Строк без цены: {result_joined['price'].isna().sum()}")

    # 2. FILTER
    print("\n2) Фильтруем только строки с известным product_id и ценой:")
    filtered = joined[
        joined['product_id'].notna() &
        joined['price'].notna()
    ]
    result_filtered = filtered.compute()
    print(f"   ✓ После фильтрации: {len(result_filtered)} строк")

    # 3. MAP_PARTITIONS
    print("\n3) Обрабатываем через map_partitions (добавляем revenue):")
    def calculate_revenue(df):
        df = df.copy()
        df['revenue'] = df['sales'] * df['price']
        df['product_status'] = df['product_id'].apply(
            lambda x: 'unknown' if pd.isna(x) else 'known'
        )
        return df

    processed = filtered.map_partitions(calculate_revenue)
    result_final = processed.compute()
    print(f"   ✓ Обработано: {len(result_final)} строк")
    print(f"   - Средний revenue: {result_final['revenue'].mean():.2f}")
    print(f"\nПервые строки результата:")
    print(result_final.head(10))

except Exception as e:
    print(f"  ✗ Ошибка: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# 7. ВЫВОДЫ И РЕКОМЕНДАЦИИ
# =============================================================================
print("\n\n7. ВЫВОДЫ И РЕКОМЕНДАЦИИ")
print("=" * 80)
print("""
ОСНОВНЫЕ ВЫВОДЫ:

1. ОГРАНИЧЕНИЕ DASK:
   ✗ Dask НЕ поддерживает создание DataFrame с non-numeric индексом, содержащим null
   ✗ Выбрасывается NotImplementedError с предложением отфильтровать null
   ✓ Числовые индексы с null могут работать (требует проверки в каждой версии)

2. ОБХОДНЫЕ ПУТИ (в порядке предпочтения):

   a) RESET_INDEX (Рекомендуется):
      ✓ Преобразовать индекс в обычную колонку через reset_index()
      ✓ Все операции (join, filter, map_partitions) работают корректно
      ✓ Полный контроль над обработкой null значений
      ✓ Можно позже восстановить индекс через set_index()

   b) ФИЛЬТРАЦИЯ NULL:
      ✓ Использовать data.loc[~data.index.isna()] перед созданием Dask DF
      ✗ Потеря данных (строки с null индексом удаляются)
      ✓ Простота реализации

   c) ЗАМЕНА NULL НА СТРОКУ:
      ✓ Заменить null на специальное значение (например, '__NULL__')
      ✓ Сохраняет все данные
      ✗ Нужно отслеживать и обрабатывать специальные значения
      ✗ Возможны коллизии если '__NULL__' существует в данных

3. ОПЕРАЦИИ С NULL В КОЛОНКАХ (после reset_index):

   JOIN:
   ✓ Inner join: null значения обычно не матчатся (стандартное поведение SQL)
   ✓ Left/Right join: сохраняет null в результате
   ✓ Outer join: объединяет null отдельно

   FILTER:
   ✓ Используйте .notna() / .isna() для проверки null
   ✓ Операторы сравнения с null возвращают False

   MAP_PARTITIONS:
   ✓ Полностью работоспособен
   ✓ Можно обрабатывать null внутри функции
   ✓ Сохраняет структуру данных

4. РЕКОМЕНДАЦИИ ПО РАБОТЕ:

   ✓ Всегда используйте reset_index() для индексов с null
   ✓ Храните исходный индекс в колонке для возможного восстановления
   ✓ Документируйте семантику null в вашем коде
   ✓ Используйте явные проверки .isna() / .notna()
   ✓ Тестируйте поведение join с null значениями

   ✗ Не полагайтесь на неявное поведение с null
   ✗ Не используйте операторы сравнения напрямую с null
   ✗ Избегайте смешивания различных типов null (None, np.nan, pd.NA)

5. ТИПОВОЙ ПАТТЕРН:

   # Исходные данные с null в индексе
   pdf = pd.DataFrame(data, index=index_with_nulls)

   # Преобразуем индекс в колонку
   pdf = pdf.reset_index()
   pdf.columns = ['original_index'] + list(pdf.columns[1:])

   # Создаем Dask DataFrame
   ddf = dd.from_pandas(pdf, npartitions=N)

   # Работаем с данными
   result = ddf[ddf['original_index'].notna()]  # Фильтрация
   joined = ddf.merge(other, on='original_index')  # Join
   processed = ddf.map_partitions(func)  # Обработка

   # При необходимости восстанавливаем индекс
   final = result.set_index('original_index', sorted=False)
""")

print("\n" + "=" * 80)
print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
print("=" * 80)
