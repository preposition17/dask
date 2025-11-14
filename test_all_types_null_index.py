"""
Комплексное исследование индексов различных типов данных с null значениями в Dask.
Тестируются: int, float, datetime, Decimal, PyArrow, object и другие типы.
"""

import numpy as np
import pandas as pd
import dask.dataframe as dd
from decimal import Decimal
from datetime import datetime, timedelta
import sys

# Проверяем доступность PyArrow
try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    print("⚠ PyArrow не установлен, пропускаем PyArrow тесты")

print("=" * 80)
print("КОМПЛЕКСНОЕ ИССЛЕДОВАНИЕ: Индексы различных типов с null в Dask")
print("=" * 80)
print(f"\nВерсии библиотек:")
print(f"  Python: {sys.version.split()[0]}")
print(f"  Pandas: {pd.__version__}")
print(f"  NumPy: {np.__version__}")
import dask
print(f"  Dask: {dask.__version__}")
if PYARROW_AVAILABLE:
    print(f"  PyArrow: {pa.__version__}")

# =============================================================================
# Вспомогательные функции
# =============================================================================

def test_dataframe_creation(name, index_data, index_dtype=None):
    """Тестирует создание Dask DataFrame с заданным индексом"""
    print(f"\n{name}")
    print("-" * 60)

    try:
        # Создаем тестовые данные
        data = {'value': range(len(index_data)), 'category': ['A', 'B', 'C'] * (len(index_data) // 3 + 1)}
        data = {k: v[:len(index_data)] for k, v in data.items()}

        # Создаем индекс
        if index_dtype:
            index = pd.Series(index_data, dtype=index_dtype)
        else:
            index = pd.Series(index_data)

        pdf = pd.DataFrame(data, index=index)

        print(f"  Pandas DataFrame создан:")
        print(f"    Тип индекса: {pdf.index.dtype}")
        print(f"    Строк: {len(pdf)}")
        print(f"    Null в индексе: {pdf.index.isna().sum()}")
        print(f"    Первые значения индекса: {list(pdf.index[:5])}")

        # Пытаемся создать Dask DataFrame
        try:
            ddf = dd.from_pandas(pdf, npartitions=3)
            print(f"  ✅ Dask DataFrame создан успешно")
            print(f"    Партиций: {ddf.npartitions}")
            print(f"    Known divisions: {ddf.known_divisions}")

            # Тестируем базовые операции
            return test_operations(ddf, name)

        except NotImplementedError as e:
            print(f"  ❌ NotImplementedError: {str(e)[:100]}...")
            return {'success': False, 'error': 'NotImplementedError', 'message': str(e)}
        except Exception as e:
            print(f"  ❌ Ошибка: {type(e).__name__}: {str(e)[:100]}...")
            return {'success': False, 'error': type(e).__name__, 'message': str(e)}

    except Exception as e:
        print(f"  ❌ Ошибка при создании pandas DataFrame: {type(e).__name__}: {e}")
        return {'success': False, 'error': type(e).__name__, 'message': str(e)}


def test_operations(ddf, name):
    """Тестирует операции на Dask DataFrame"""
    results = {'success': True, 'operations': {}}

    try:
        # Filter
        print(f"  Тест Filter:")
        try:
            filtered = ddf[ddf['category'] == 'A'].compute()
            print(f"    ✅ Filter работает ({len(filtered)} строк)")
            results['operations']['filter'] = True
        except Exception as e:
            print(f"    ❌ Filter: {type(e).__name__}")
            results['operations']['filter'] = False

        # map_partitions
        print(f"  Тест map_partitions:")
        try:
            def add_col(df):
                df = df.copy()
                df['new_col'] = df['value'] * 2
                return df

            mapped = ddf.map_partitions(add_col).compute()
            print(f"    ✅ map_partitions работает ({len(mapped)} строк)")
            results['operations']['map_partitions'] = True
        except Exception as e:
            print(f"    ❌ map_partitions: {type(e).__name__}")
            results['operations']['map_partitions'] = False

        # Index operations
        print(f"  Тест операций с индексом:")
        try:
            null_count = ddf.index.isna().sum().compute()
            print(f"    ✅ index.isna() работает (null: {null_count})")
            results['operations']['index_isna'] = True
        except Exception as e:
            print(f"    ❌ index.isna(): {type(e).__name__}")
            results['operations']['index_isna'] = False

    except Exception as e:
        print(f"  ❌ Общая ошибка при тестировании: {type(e).__name__}: {e}")
        results['success'] = False

    return results


# =============================================================================
# 1. FLOAT ТИПЫ
# =============================================================================
print("\n" + "=" * 80)
print("1. FLOAT ТИПЫ")
print("=" * 80)

# 1.1 float64 (стандартный numpy float с NaN)
float64_index = [1.0, 2.5, np.nan, 4.0, 5.5, np.nan, 7.0, 8.5, 9.0]
result_float64 = test_dataframe_creation(
    "1.1. float64 (стандартный с np.nan)",
    float64_index
)

# 1.2 Float64 (nullable pandas float)
try:
    float64_nullable_index = pd.array([1.0, 2.5, pd.NA, 4.0, 5.5, pd.NA, 7.0, 8.5, 9.0], dtype="Float64")
    result_float64_nullable = test_dataframe_creation(
        "1.2. Float64 (nullable pandas с pd.NA)",
        float64_nullable_index
    )
except Exception as e:
    print(f"\n1.2. Float64 nullable: ❌ Ошибка создания: {e}")
    result_float64_nullable = {'success': False, 'error': 'Creation failed'}

# 1.3 float32
float32_index = np.array([1.0, 2.5, np.nan, 4.0, 5.5, np.nan, 7.0, 8.5, 9.0], dtype='float32')
result_float32 = test_dataframe_creation(
    "1.3. float32",
    float32_index
)

# =============================================================================
# 2. INTEGER ТИПЫ
# =============================================================================
print("\n" + "=" * 80)
print("2. INTEGER ТИПЫ")
print("=" * 80)

# 2.1 int64 (стандартный - не может содержать NaN напрямую)
print("\n2.1. int64 (стандартный - не поддерживает NaN)")
print("-" * 60)
print("  ⚠ Стандартный int64 не может содержать NaN")
print("  Для integer с null нужно использовать Int64 (nullable)")

# 2.2 Int64 (nullable pandas integer)
try:
    int64_nullable_index = pd.array([1, 2, pd.NA, 4, 5, pd.NA, 7, 8, 9], dtype="Int64")
    result_int64_nullable = test_dataframe_creation(
        "2.2. Int64 (nullable pandas с pd.NA)",
        int64_nullable_index
    )
except Exception as e:
    print(f"\n2.2. Int64 nullable: ❌ Ошибка создания: {e}")
    result_int64_nullable = {'success': False, 'error': 'Creation failed'}

# 2.3 Int32 (nullable)
try:
    int32_nullable_index = pd.array([1, 2, pd.NA, 4, 5, pd.NA, 7, 8, 9], dtype="Int32")
    result_int32_nullable = test_dataframe_creation(
        "2.3. Int32 (nullable)",
        int32_nullable_index
    )
except Exception as e:
    print(f"\n2.3. Int32 nullable: ❌ Ошибка создания: {e}")
    result_int32_nullable = {'success': False, 'error': 'Creation failed'}

# =============================================================================
# 3. DATETIME ТИПЫ
# =============================================================================
print("\n" + "=" * 80)
print("3. DATETIME ТИПЫ")
print("=" * 80)

# 3.1 datetime64[ns] с NaT
base_dt = pd.Timestamp('2024-01-01')
datetime_index = [
    base_dt,
    base_dt + pd.Timedelta(days=1),
    pd.NaT,
    base_dt + pd.Timedelta(days=3),
    base_dt + pd.Timedelta(days=4),
    pd.NaT,
    base_dt + pd.Timedelta(days=6),
    base_dt + pd.Timedelta(days=7),
    base_dt + pd.Timedelta(days=8)
]
result_datetime64 = test_dataframe_creation(
    "3.1. datetime64[ns] с pd.NaT",
    datetime_index
)

# 3.2 DatetimeTZDtype (с timezone)
try:
    datetime_tz_index = pd.DatetimeIndex(datetime_index, tz='UTC')
    result_datetime_tz = test_dataframe_creation(
        "3.2. datetime64[ns, UTC] (с timezone)",
        datetime_tz_index
    )
except Exception as e:
    print(f"\n3.2. datetime with timezone: ❌ Ошибка создания: {e}")
    result_datetime_tz = {'success': False, 'error': 'Creation failed'}

# 3.3 Timedelta
timedelta_index = [
    pd.Timedelta(days=1),
    pd.Timedelta(hours=12),
    pd.NaT,
    pd.Timedelta(minutes=30),
    pd.Timedelta(seconds=45),
    pd.NaT,
    pd.Timedelta(days=7),
    pd.Timedelta(hours=3),
    pd.Timedelta(days=2)
]
result_timedelta = test_dataframe_creation(
    "3.3. timedelta64[ns] с pd.NaT",
    timedelta_index
)

# =============================================================================
# 4. STRING/OBJECT ТИПЫ
# =============================================================================
print("\n" + "=" * 80)
print("4. STRING/OBJECT ТИПЫ")
print("=" * 80)

# 4.1 object (стандартный тип для строк)
object_index = ['str1', 'str2', None, 'str4', 'str5', np.nan, 'str7', pd.NA, 'str9']
result_object = test_dataframe_creation(
    "4.1. object (смешанные None/np.nan/pd.NA)",
    object_index
)

# 4.2 string (pandas string dtype)
try:
    string_index = pd.array(['str1', 'str2', pd.NA, 'str4', 'str5', pd.NA, 'str7', 'str8', 'str9'], dtype="string")
    result_string = test_dataframe_creation(
        "4.2. string (pandas dtype с pd.NA)",
        string_index
    )
except Exception as e:
    print(f"\n4.2. string dtype: ❌ Ошибка создания: {e}")
    result_string = {'success': False, 'error': 'Creation failed'}

# =============================================================================
# 5. BOOLEAN ТИПЫ
# =============================================================================
print("\n" + "=" * 80)
print("5. BOOLEAN ТИПЫ")
print("=" * 80)

# 5.1 boolean (nullable)
try:
    bool_index = pd.array([True, False, pd.NA, True, False, pd.NA, True, False, True], dtype="boolean")
    result_boolean = test_dataframe_creation(
        "5.1. boolean (nullable с pd.NA)",
        bool_index
    )
except Exception as e:
    print(f"\n5.1. boolean: ❌ Ошибка создания: {e}")
    result_boolean = {'success': False, 'error': 'Creation failed'}

# 5.2 bool (стандартный numpy - не может содержать NA)
print("\n5.2. bool (стандартный numpy - не поддерживает NA)")
print("-" * 60)
print("  ⚠ Стандартный numpy bool не может содержать NA")

# =============================================================================
# 6. DECIMAL
# =============================================================================
print("\n" + "=" * 80)
print("6. DECIMAL")
print("=" * 80)

decimal_index = [
    Decimal('1.5'),
    Decimal('2.7'),
    None,
    Decimal('4.2'),
    Decimal('5.9'),
    None,
    Decimal('7.3'),
    Decimal('8.1'),
    Decimal('9.6')
]
result_decimal = test_dataframe_creation(
    "6.1. Decimal (Python decimal с None)",
    decimal_index
)

# =============================================================================
# 7. PYARROW ТИПЫ
# =============================================================================
if PYARROW_AVAILABLE:
    print("\n" + "=" * 80)
    print("7. PYARROW ТИПЫ")
    print("=" * 80)

    # 7.1 PyArrow int64
    try:
        pa_int_index = pd.array([1, 2, None, 4, 5, None, 7, 8, 9], dtype=pd.ArrowDtype(pa.int64()))
        result_pa_int = test_dataframe_creation(
            "7.1. PyArrow int64 с None",
            pa_int_index
        )
    except Exception as e:
        print(f"\n7.1. PyArrow int64: ❌ Ошибка создания: {e}")
        result_pa_int = {'success': False, 'error': 'Creation failed'}

    # 7.2 PyArrow float64
    try:
        pa_float_index = pd.array([1.0, 2.5, None, 4.0, 5.5, None, 7.0, 8.5, 9.0], dtype=pd.ArrowDtype(pa.float64()))
        result_pa_float = test_dataframe_creation(
            "7.2. PyArrow float64 с None",
            pa_float_index
        )
    except Exception as e:
        print(f"\n7.2. PyArrow float64: ❌ Ошибка создания: {e}")
        result_pa_float = {'success': False, 'error': 'Creation failed'}

    # 7.3 PyArrow string
    try:
        pa_string_index = pd.array(['str1', 'str2', None, 'str4', 'str5', None, 'str7', 'str8', 'str9'],
                                   dtype=pd.ArrowDtype(pa.string()))
        result_pa_string = test_dataframe_creation(
            "7.3. PyArrow string с None",
            pa_string_index
        )
    except Exception as e:
        print(f"\n7.3. PyArrow string: ❌ Ошибка создания: {e}")
        result_pa_string = {'success': False, 'error': 'Creation failed'}

    # 7.4 PyArrow timestamp
    try:
        pa_ts_data = [
            pd.Timestamp('2024-01-01'),
            pd.Timestamp('2024-01-02'),
            None,
            pd.Timestamp('2024-01-04'),
            pd.Timestamp('2024-01-05'),
            None,
            pd.Timestamp('2024-01-07'),
            pd.Timestamp('2024-01-08'),
            pd.Timestamp('2024-01-09')
        ]
        pa_timestamp_index = pd.array(pa_ts_data, dtype=pd.ArrowDtype(pa.timestamp('ns')))
        result_pa_timestamp = test_dataframe_creation(
            "7.4. PyArrow timestamp с None",
            pa_timestamp_index
        )
    except Exception as e:
        print(f"\n7.4. PyArrow timestamp: ❌ Ошибка создания: {e}")
        result_pa_timestamp = {'success': False, 'error': 'Creation failed'}

# =============================================================================
# 8. CATEGORY
# =============================================================================
print("\n" + "=" * 80)
print("8. CATEGORY (Categorical)")
print("=" * 80)

try:
    cat_index = pd.Categorical(['cat1', 'cat2', None, 'cat1', 'cat2', None, 'cat3', 'cat1', 'cat3'])
    result_category = test_dataframe_creation(
        "8.1. Categorical с None",
        cat_index
    )
except Exception as e:
    print(f"\n8.1. Categorical: ❌ Ошибка создания: {e}")
    result_category = {'success': False, 'error': 'Creation failed'}

# =============================================================================
# 9. PERIOD
# =============================================================================
print("\n" + "=" * 80)
print("9. PERIOD")
print("=" * 80)

try:
    period_index = pd.PeriodIndex(['2024-01', '2024-02', pd.NaT, '2024-04', '2024-05',
                                   pd.NaT, '2024-07', '2024-08', '2024-09'], freq='M')
    result_period = test_dataframe_creation(
        "9.1. Period (месячный) с pd.NaT",
        period_index
    )
except Exception as e:
    print(f"\n9.1. Period: ❌ Ошибка создания: {e}")
    result_period = {'success': False, 'error': 'Creation failed'}

# =============================================================================
# 10. INTERVAL
# =============================================================================
print("\n" + "=" * 80)
print("10. INTERVAL")
print("=" * 80)

try:
    intervals = [
        pd.Interval(0, 1),
        pd.Interval(1, 2),
        None,
        pd.Interval(3, 4),
        pd.Interval(4, 5),
        None,
        pd.Interval(6, 7),
        pd.Interval(7, 8),
        pd.Interval(8, 9)
    ]
    interval_index = pd.IntervalIndex.from_tuples(
        [(0, 1), (1, 2), pd.NA, (3, 4), (4, 5), pd.NA, (6, 7), (7, 8), (8, 9)],
        closed='right'
    ) if pd.__version__ >= '1.5' else None

    if interval_index is None:
        # Для старых версий pandas
        interval_index = intervals

    result_interval = test_dataframe_creation(
        "10.1. Interval с None/NA",
        interval_index
    )
except Exception as e:
    print(f"\n10.1. Interval: ❌ Ошибка создания: {e}")
    result_interval = {'success': False, 'error': 'Creation failed'}

# =============================================================================
# ИТОГОВАЯ СВОДКА
# =============================================================================
print("\n" + "=" * 80)
print("ИТОГОВАЯ СВОДКА РЕЗУЛЬТАТОВ")
print("=" * 80)

summary = {
    "FLOAT": {
        "float64": result_float64.get('success', False),
        "Float64 (nullable)": result_float64_nullable.get('success', False),
        "float32": result_float32.get('success', False),
    },
    "INTEGER": {
        "int64": "N/A (не поддерживает NaN)",
        "Int64 (nullable)": result_int64_nullable.get('success', False),
        "Int32 (nullable)": result_int32_nullable.get('success', False),
    },
    "DATETIME": {
        "datetime64[ns]": result_datetime64.get('success', False),
        "datetime64[ns, tz]": result_datetime_tz.get('success', False),
        "timedelta64[ns]": result_timedelta.get('success', False),
    },
    "STRING/OBJECT": {
        "object": result_object.get('success', False),
        "string": result_string.get('success', False),
    },
    "BOOLEAN": {
        "boolean (nullable)": result_boolean.get('success', False),
        "bool": "N/A (не поддерживает NA)",
    },
    "DECIMAL": {
        "Decimal": result_decimal.get('success', False),
    },
    "CATEGORY": {
        "Categorical": result_category.get('success', False),
    },
    "PERIOD": {
        "Period": result_period.get('success', False),
    },
    "INTERVAL": {
        "Interval": result_interval.get('success', False),
    }
}

if PYARROW_AVAILABLE:
    summary["PYARROW"] = {
        "int64": result_pa_int.get('success', False),
        "float64": result_pa_float.get('success', False),
        "string": result_pa_string.get('success', False),
        "timestamp": result_pa_timestamp.get('success', False),
    }

for category, types in summary.items():
    print(f"\n{category}:")
    for type_name, success in types.items():
        if success == "N/A (не поддерживает NaN)" or success == "N/A (не поддерживает NA)":
            print(f"  {type_name:30s} ⚠ {success}")
        elif success:
            print(f"  {type_name:30s} ✅ Поддерживается")
        else:
            print(f"  {type_name:30s} ❌ Не поддерживается")

print("\n" + "=" * 80)
print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
print("=" * 80)

# Сохраняем результаты для отчета
import json
with open('test_results_all_types.json', 'w') as f:
    # Преобразуем результаты в JSON-совместимый формат
    json_summary = {}
    for cat, types in summary.items():
        json_summary[cat] = {}
        for type_name, success in types.items():
            if isinstance(success, bool):
                json_summary[cat][type_name] = "supported" if success else "not_supported"
            else:
                json_summary[cat][type_name] = str(success)

    json.dump(json_summary, f, indent=2)
    print("\n✅ Результаты сохранены в test_results_all_types.json")
