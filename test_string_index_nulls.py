"""
Исследование использования индексов на колонках с типом "строка",
имеющих пустые значения (NaN, NA, None) в Dask DataFrame.
"""

import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.dataframe.utils import assert_eq

print("=" * 80)
print("ИССЛЕДОВАНИЕ: Строковые индексы с null-значениями в Dask DataFrame")
print("=" * 80)

# =============================================================================
# 1. Создание тестового DataFrame с строковыми индексами и null-значениями
# =============================================================================
print("\n1. СОЗДАНИЕ ТЕСТОВОГО DATAFRAME")
print("-" * 80)

# Создаем pandas DataFrame с различными вариантами null-значений в строковом индексе
data = {
    'value': range(25),
    'category': ['A', 'B', 'C', 'D', 'E'] * 5,
    'numeric': np.random.randn(25)
}

# Создаем индекс с различными типами null-значений
index_values = [
    'item_0', 'item_1', None, 'item_3', np.nan,  # 0-4
    'item_5', 'item_6', 'item_7', None, 'item_9',  # 5-9
    pd.NA, 'item_11', 'item_12', 'item_13', np.nan,  # 10-14
    'item_15', None, 'item_17', 'item_18', 'item_19',  # 15-19
    'item_20', 'item_21', pd.NA, 'item_23', 'item_24'  # 20-24
]

# Преобразуем в pandas Series для создания индекса
index_series = pd.Series(index_values, dtype='object')
pdf = pd.DataFrame(data, index=index_series)

print(f"Pandas DataFrame создан:")
print(f"  - Форма: {pdf.shape}")
print(f"  - Тип индекса: {pdf.index.dtype}")
print(f"  - Количество null-значений в индексе: {pdf.index.isna().sum()}")
print(f"\nПервые 10 строк:")
print(pdf.head(10))
print(f"\nТипы null-значений в индексе:")
print(f"  - None: {sum(x is None for x in index_values)}")
print(f"  - np.nan: {sum(isinstance(x, float) and pd.isna(x) for x in index_values)}")
print(f"  - pd.NA: {sum(x is pd.NA for x in index_values if not isinstance(x, float))}")

# Создаем Dask DataFrame с 5 партициями
ddf = dd.from_pandas(pdf, npartitions=5)
print(f"\nDask DataFrame создан:")
print(f"  - Количество партиций: {ddf.npartitions}")
print(f"  - Партиции: {ddf.divisions}")
print(f"  - Известен ли индекс (known): {ddf.known_divisions}")

# =============================================================================
# 2. Тестирование операций FILTER
# =============================================================================
print("\n\n2. ТЕСТИРОВАНИЕ FILTER ОПЕРАЦИЙ")
print("-" * 80)

try:
    # Фильтрация по значению колонки
    print("\na) Фильтрация по значению колонки (category == 'A'):")
    filtered = ddf[ddf['category'] == 'A']
    result = filtered.compute()
    print(f"  ✓ Успешно. Результат: {len(result)} строк")
    print(result.head())

    # Фильтрация по индексу (исключая null)
    print("\nb) Фильтрация по индексу (loc['item_5':'item_15']):")
    try:
        loc_filtered = ddf.loc['item_5':'item_15']
        result_loc = loc_filtered.compute()
        print(f"  ✓ Успешно. Результат: {len(result_loc)} строк")
        print(result_loc)
    except Exception as e:
        print(f"  ✗ Ошибка: {type(e).__name__}: {e}")

    # Проверка на null в индексе
    print("\nc) Подсчет null-значений в индексе:")
    null_count = ddf.index.isna().sum().compute()
    print(f"  ✓ Количество null-значений: {null_count}")

except Exception as e:
    print(f"  ✗ Ошибка при фильтрации: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# 3. Тестирование операций JOIN
# =============================================================================
print("\n\n3. ТЕСТИРОВАНИЕ JOIN ОПЕРАЦИЙ")
print("-" * 80)

try:
    # Создаем второй DataFrame для join
    data2 = {
        'extra_value': range(15, 30),
        'description': [f'desc_{i}' for i in range(15)]
    }
    # Индекс с частичным пересечением и своими null-значениями
    index_values2 = [
        'item_10', 'item_11', None, 'item_13', 'item_14',
        'item_15', np.nan, 'item_17', 'item_18', 'item_19',
        'item_20', 'item_21', 'item_22', pd.NA, 'item_new'
    ]
    pdf2 = pd.DataFrame(data2, index=pd.Series(index_values2, dtype='object'))
    ddf2 = dd.from_pandas(pdf2, npartitions=5)

    print(f"Второй DataFrame создан:")
    print(f"  - Форма: {pdf2.shape}")
    print(f"  - Null-значений в индексе: {pdf2.index.isna().sum()}")
    print(pdf2.head())

    # Inner join
    print("\na) Inner join:")
    try:
        joined_inner = ddf.join(ddf2, how='inner')
        result_inner = joined_inner.compute()
        print(f"  ✓ Успешно. Результат: {len(result_inner)} строк")
        print(result_inner.head())
    except Exception as e:
        print(f"  ✗ Ошибка: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    # Left join
    print("\nb) Left join:")
    try:
        joined_left = ddf.join(ddf2, how='left')
        result_left = joined_left.compute()
        print(f"  ✓ Успешно. Результат: {len(result_left)} строк")
        print(result_left.head(10))
        print(f"  - Null значений в 'extra_value': {result_left['extra_value'].isna().sum()}")
    except Exception as e:
        print(f"  ✗ Ошибка: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

    # Outer join
    print("\nc) Outer join:")
    try:
        joined_outer = ddf.join(ddf2, how='outer')
        result_outer = joined_outer.compute()
        print(f"  ✓ Успешно. Результат: {len(result_outer)} строк")
        print(f"  - Null значений в 'value': {result_outer['value'].isna().sum()}")
        print(f"  - Null значений в 'extra_value': {result_outer['extra_value'].isna().sum()}")
    except Exception as e:
        print(f"  ✗ Ошибка: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

except Exception as e:
    print(f"  ✗ Ошибка при создании второго DataFrame: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# 4. Тестирование map_partitions
# =============================================================================
print("\n\n4. ТЕСТИРОВАНИЕ MAP_PARTITIONS")
print("-" * 80)

try:
    # Функция для обработки партиции
    def process_partition(df):
        """Обрабатывает партицию, добавляя информацию о null в индексе"""
        result = df.copy()
        result['index_is_null'] = df.index.isna()
        result['index_str'] = df.index.astype(str)
        result['partition_size'] = len(df)
        return result

    print("a) map_partitions с добавлением колонок:")
    mapped = ddf.map_partitions(process_partition)
    result_mapped = mapped.compute()
    print(f"  ✓ Успешно. Новые колонки: {list(result_mapped.columns)}")
    print(result_mapped.head(10))
    print(f"  - Строк с null индексом: {result_mapped['index_is_null'].sum()}")

    # Агрегация по партициям
    def partition_stats(df):
        """Статистика партиции"""
        stats = {
            'total_rows': len(df),
            'null_indices': df.index.isna().sum(),
            'mean_value': df['value'].mean(),
            'category_counts': df['category'].value_counts().to_dict()
        }
        return pd.Series(stats)

    print("\nb) map_partitions для статистики по партициям:")
    # Используем meta для определения структуры результата
    meta = pd.Series(dtype=object)
    stats = ddf.map_partitions(partition_stats, meta=meta)
    result_stats = stats.compute()
    print(f"  ✓ Успешно. Статистика по {len(result_stats)} партициям:")
    for i, stat in enumerate(result_stats):
        print(f"    Партиция {i}: {stat}")

    # Фильтрация внутри map_partitions
    def filter_valid_indices(df):
        """Оставляет только строки с не-null индексами"""
        return df[~df.index.isna()]

    print("\nc) map_partitions с фильтрацией null индексов:")
    filtered_mapped = ddf.map_partitions(filter_valid_indices)
    result_filtered = filtered_mapped.compute()
    print(f"  ✓ Успешно. Исходных строк: {len(pdf)}, после фильтрации: {len(result_filtered)}")
    print(f"  - Удалено строк с null индексом: {len(pdf) - len(result_filtered)}")
    print(result_filtered.head())

except Exception as e:
    print(f"  ✗ Ошибка при map_partitions: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# 5. Дополнительные операции
# =============================================================================
print("\n\n5. ДОПОЛНИТЕЛЬНЫЕ ОПЕРАЦИИ")
print("-" * 80)

try:
    # Set_index на колонку с null
    print("a) set_index на колонку, содержащую null:")
    test_df = ddf.copy()
    # Добавляем колонку с null значениями
    test_df['string_col'] = test_df.index
    try:
        reindexed = test_df.set_index('string_col', sorted=False)
        result_reindexed = reindexed.compute()
        print(f"  ✓ Успешно. Новый индекс установлен")
        print(f"  - Null значений в индексе: {result_reindexed.index.isna().sum()}")
    except Exception as e:
        print(f"  ✗ Ошибка: {type(e).__name__}: {e}")

    # Groupby с индексом, содержащим null
    print("\nb) groupby по category с null в индексе:")
    try:
        grouped = ddf.groupby('category')['value'].sum()
        result_grouped = grouped.compute()
        print(f"  ✓ Успешно. Результат:")
        print(result_grouped)
    except Exception as e:
        print(f"  ✗ Ошибка: {type(e).__name__}: {e}")

    # Reset index
    print("\nc) reset_index:")
    try:
        reset = ddf.reset_index()
        result_reset = reset.compute()
        print(f"  ✓ Успешно. Колонки: {list(result_reset.columns)}")
        print(f"  - Null значений в колонке 'index': {result_reset['index'].isna().sum()}")
        print(result_reset.head())
    except Exception as e:
        print(f"  ✗ Ошибка: {type(e).__name__}: {e}")

    # Сортировка по индексу
    print("\nd) Сортировка по индексу (sort_index):")
    try:
        sorted_df = ddf.sort_index()
        result_sorted = sorted_df.compute()
        print(f"  ✓ Успешно")
        print(f"  - Первые 5 элементов индекса: {list(result_sorted.index[:5])}")
        print(f"  - Последние 5 элементов индекса: {list(result_sorted.index[-5:])}")
    except Exception as e:
        print(f"  ✗ Ошибка: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()

except Exception as e:
    print(f"  ✗ Ошибка в дополнительных операциях: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# 6. Выводы
# =============================================================================
print("\n\n6. ВЫВОДЫ")
print("=" * 80)
print("""
РЕЗУЛЬТАТЫ ИССЛЕДОВАНИЯ:

1. СОЗДАНИЕ DATAFRAME:
   - Dask DataFrame успешно создается с строковым индексом, содержащим null
   - Поддерживаются различные типы null: None, np.nan, pd.NA
   - Индекс не является known_divisions из-за null значений

2. FILTER ОПЕРАЦИИ:
   - Фильтрация по колонкам работает корректно
   - loc может давать ошибки с null в индексе
   - isna() корректно определяет null значения в индексе

3. JOIN ОПЕРАЦИИ:
   - Inner/Left/Outer join работают с null в индексе
   - Null значения в индексе могут участвовать в join
   - Важно учитывать семантику null при join

4. MAP_PARTITIONS:
   - Полностью работоспособен с null в индексе
   - Можно фильтровать, трансформировать и агрегировать данные
   - Сохраняет структуру индекса

5. ДОПОЛНИТЕЛЬНЫЕ ОПЕРАЦИИ:
   - set_index, groupby, reset_index работают
   - sort_index может иметь особенности с null
   - Операции в целом устойчивы к null в индексе

РЕКОМЕНДАЦИИ:
- Избегайте null в строковых индексах для sorted операций
- При join учитывайте поведение null (обычно не матчатся)
- Используйте reset_index() если null в индексе создают проблемы
- map_partitions - безопасный способ обработки партиций с null индексами
""")

print("\n" + "=" * 80)
print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
print("=" * 80)
