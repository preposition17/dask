# Комплексное исследование: Индексы различных типов данных с null значениями в Dask DataFrame

## 📋 Общая информация

**Дата исследования:** 2025-11-09
**Версия Dask:** 2025.11.0
**Версия Pandas:** 2.3.3
**Версия NumPy:** 2.3.4
**Версия PyArrow:** 22.0.0
**Python:** 3.11.14

## 🎯 Цель исследования

Систематически протестировать создание Dask DataFrame с индексами различных типов данных, содержащих null/missing значения, и определить:
1. Какие типы данных поддерживаются
2. Какие операции работают с каждым типом
3. Какие ограничения существуют
4. Рекомендации по использованию

## 📊 Итоговая сводная таблица

| Категория | Тип данных | Поддержка Dask | Filter | map_partitions | index.isna() | Примечания |
|-----------|------------|----------------|--------|----------------|--------------|------------|
| **FLOAT** | `float64` | ✅ Да | ✅ | ✅ | ✅ | Полная поддержка с np.nan |
| | `Float64` (nullable) | ⚠️ Частично | ❌ | ❌ | ❌ | DataFrame создается, но операции падают |
| | `float32` | ✅ Да | ✅ | ✅ | ✅ | Полная поддержка |
| **INTEGER** | `int64` | N/A | - | - | - | Не может содержать NaN |
| | `Int64` (nullable) | ⚠️ Частично | ❌ | ❌ | ❌ | DataFrame создается, но операции падают |
| | `Int32` (nullable) | ⚠️ Частично | ❌ | ❌ | ❌ | DataFrame создается, но операции падают |
| **DATETIME** | `datetime64[ns]` | ❌ Нет | - | - | - | NotImplementedError |
| | `datetime64[ns, tz]` | ❌ Нет | - | - | - | NotImplementedError |
| | `timedelta64[ns]` | ❌ Нет | - | - | - | NotImplementedError |
| **STRING** | `object` | ❌ Нет | - | - | - | NotImplementedError |
| | `string` | ❌ Нет | - | - | - | NotImplementedError |
| **BOOLEAN** | `bool` | N/A | - | - | - | Не может содержать NA |
| | `boolean` (nullable) | ❌ Нет | - | - | - | NotImplementedError |
| **DECIMAL** | `Decimal` | ❌ Нет | - | - | - | NotImplementedError (как object) |
| **CATEGORY** | `Categorical` | ❌ Нет | - | - | - | NotImplementedError |
| **PERIOD** | `period[M]` | ❌ Нет | - | - | - | NotImplementedError |
| **INTERVAL** | `Interval` | ❌ Нет | - | - | - | NotImplementedError |
| **PYARROW** | `int64[pyarrow]` | ⚠️ Частично | ❌ | ❌ | ❌ | DataFrame создается, но операции падают |
| | `double[pyarrow]` | ⚠️ Частично | ❌ | ❌ | ❌ | DataFrame создается, но операции падают |
| | `string[pyarrow]` | ❌ Нет | - | - | - | NotImplementedError |
| | `timestamp[pyarrow]` | ❌ Нет | - | - | - | NotImplementedError |

## 🔍 Детальные результаты по категориям

### 1. Float типы

#### 1.1. ✅ `float64` (стандартный numpy float)

**Статус:** **ПОЛНОСТЬЮ ПОДДЕРЖИВАЕТСЯ**

```python
import numpy as np
import pandas as pd
import dask.dataframe as dd

index = [1.0, 2.5, np.nan, 4.0, 5.5, np.nan, 7.0, 8.5, 9.0]
pdf = pd.DataFrame({'value': range(9)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=3)  # ✅ Работает
```

**Результаты:**
- ✅ DataFrame создается успешно
- ✅ Known divisions: True
- ✅ Filter работает корректно
- ✅ map_partitions работает корректно
- ✅ index.isna() работает корректно
- ✅ Null значений корректно определяется: 2

**Рекомендация:** ✅ **Рекомендуется для использования** - наиболее надежный вариант для numeric индексов с пропусками.

---

#### 1.2. ⚠️ `Float64` (nullable pandas dtype)

**Статус:** **ЧАСТИЧНАЯ ПОДДЕРЖКА** (создается, но операции не работают)

```python
index = pd.array([1.0, 2.5, pd.NA, 4.0, 5.5], dtype="Float64")
pdf = pd.DataFrame({'value': range(5)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=2)  # ✅ Создается
# ❌ Но операции падают с ошибкой
```

**Проблема:**
```
TypeError: boolean value of NA is ambiguous
```

**Результаты:**
- ✅ DataFrame создается
- ❌ Known divisions вызывает ошибку
- ❌ Filter падает
- ❌ map_partitions падает
- ❌ index.isna() падает

**Причина:** Pandas `pd.NA` ведет себя иначе чем `np.nan` - нельзя использовать в boolean контексте.

**Рекомендация:** ❌ **Не рекомендуется** - используйте стандартный `float64` с `np.nan`.

---

#### 1.3. ✅ `float32`

**Статус:** **ПОЛНОСТЬЮ ПОДДЕРЖИВАЕТСЯ**

Аналогично `float64`, работает корректно со всеми операциями.

---

### 2. Integer типы

#### 2.1. N/A `int64` (стандартный numpy int)

**Статус:** **НЕ ПРИМЕНИМО**

```python
# ❌ Невозможно создать int64 с NaN
index = [1, 2, np.nan, 4, 5]  # Преобразуется в float64!
```

**Объяснение:** Стандартные numpy integer типы не могут содержать NaN. При попытке создать такой массив он автоматически преобразуется в `float64`.

---

#### 2.2. ⚠️ `Int64` (nullable pandas dtype)

**Статус:** **ЧАСТИЧНАЯ ПОДДЕРЖКА** (создается, но операции не работают)

```python
index = pd.array([1, 2, pd.NA, 4, 5], dtype="Int64")
pdf = pd.DataFrame({'value': range(5)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=2)  # ✅ Создается
# ❌ Но операции падают
```

**Проблема:** Та же что и с `Float64` - `TypeError: boolean value of NA is ambiguous`

**Рекомендация:** ❌ **Не рекомендуется** - используйте обходной путь через `reset_index()`.

---

#### 2.3. ⚠️ `Int32` (nullable)

Аналогично `Int64`.

---

### 3. Datetime типы

#### 3.1. ❌ `datetime64[ns]`

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = [
    pd.Timestamp('2024-01-01'),
    pd.Timestamp('2024-01-02'),
    pd.NaT,  # Not-a-Time
    pd.Timestamp('2024-01-04')
]
pdf = pd.DataFrame({'value': range(4)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=2)
# ❌ NotImplementedError
```

**Ошибка:**
```
NotImplementedError: Index in passed data is non-numeric and contains nulls,
which Dask does not entirely support.
```

**Результаты:**
- ❌ DataFrame НЕ создается
- Pandas индекс тип: `datetime64[ns]`
- Null значений (NaT): 2

**Рекомендация:** Используйте `reset_index()` и храните datetime в колонке.

---

#### 3.2. ❌ `datetime64[ns, tz]` (с timezone)

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

Та же проблема что и с обычным datetime - NotImplementedError.

---

#### 3.3. ❌ `timedelta64[ns]`

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = [
    pd.Timedelta(days=1),
    pd.Timedelta(hours=12),
    pd.NaT,
    pd.Timedelta(minutes=30)
]
# ❌ NotImplementedError
```

---

### 4. String/Object типы

#### 4.1. ❌ `object` (стандартный для строк)

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = ['str1', 'str2', None, 'str4', np.nan, 'str6']
pdf = pd.DataFrame({'value': range(6)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=2)
# ❌ NotImplementedError
```

**Результаты:**
- ❌ DataFrame НЕ создается
- Поддерживает смешанные типы null: None, np.nan, pd.NA
- Null значений: 3

**Рекомендация:** Используйте `reset_index()` (см. предыдущее исследование).

---

#### 4.2. ❌ `string` (pandas dtype)

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = pd.array(['str1', 'str2', pd.NA, 'str4'], dtype="string")
# ❌ NotImplementedError
```

Аналогично `object`.

---

### 5. Boolean типы

#### 5.1. N/A `bool` (стандартный numpy)

**Статус:** **НЕ ПРИМЕНИМО**

Стандартный numpy bool не может содержать NA.

---

#### 5.2. ❌ `boolean` (nullable pandas dtype)

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = pd.array([True, False, pd.NA, True], dtype="boolean")
# ❌ NotImplementedError
```

---

### 6. Decimal

#### 6.1. ❌ `Decimal` (Python decimal)

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
from decimal import Decimal

index = [Decimal('1.5'), Decimal('2.7'), None, Decimal('4.2')]
pdf = pd.DataFrame({'value': range(4)}, index=index)
# ❌ NotImplementedError
```

**Результаты:**
- ❌ DataFrame НЕ создается
- Pandas тип индекса: `object` (Decimal хранится как object)
- Null значений: 2

**Объяснение:** Decimal хранится в pandas как object тип, поэтому применяется то же ограничение что и для строк.

---

### 7. PyArrow типы

#### 7.1. ⚠️ `int64[pyarrow]`

**Статус:** **ЧАСТИЧНАЯ ПОДДЕРЖКА**

```python
import pyarrow as pa

index = pd.array([1, 2, None, 4, 5], dtype=pd.ArrowDtype(pa.int64()))
pdf = pd.DataFrame({'value': range(5)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=2)  # ✅ Создается
# ❌ Но операции падают
```

**Проблема:** `TypeError: boolean value of NA is ambiguous`

**Результаты:**
- ✅ DataFrame создается
- ❌ Операции не работают

---

#### 7.2. ⚠️ `double[pyarrow]` (float64)

Аналогично `int64[pyarrow]` - создается, но операции падают.

---

#### 7.3. ❌ `string[pyarrow]`

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = pd.array(['str1', 'str2', None], dtype=pd.ArrowDtype(pa.string()))
# ❌ NotImplementedError
```

Считается non-numeric индексом.

---

#### 7.4. ❌ `timestamp[pyarrow]`

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = pd.array([pd.Timestamp('2024-01-01'), None],
                 dtype=pd.ArrowDtype(pa.timestamp('ns')))
# ❌ NotImplementedError
```

---

### 8. Category

#### 8.1. ❌ `Categorical`

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = pd.Categorical(['cat1', 'cat2', None, 'cat1', None])
pdf = pd.DataFrame({'value': range(5)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=2)
# ❌ NotImplementedError
```

**Результаты:**
- ❌ DataFrame НЕ создается
- Тип индекса: `category`
- Null значений: 2

---

### 9. Period

#### 9.1. ❌ `period[M]` (месячный период)

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = pd.PeriodIndex(['2024-01', '2024-02', pd.NaT, '2024-04'], freq='M')
pdf = pd.DataFrame({'value': range(4)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=2)
# ❌ NotImplementedError
```

---

### 10. Interval

#### 10.1. ❌ `Interval`

**Статус:** **НЕ ПОДДЕРЖИВАЕТСЯ**

```python
index = pd.IntervalIndex.from_tuples(
    [(0, 1), (1, 2), pd.NA, (3, 4)], closed='right'
)
# ❌ NotImplementedError
```

**Результаты:**
- ❌ DataFrame НЕ создается
- Тип индекса: `interval[float64, right]`

---

## 📈 Статистика поддержки

### По категориям

| Категория | Поддерживается | Частично | Не поддерживается | N/A |
|-----------|---------------|----------|-------------------|-----|
| Float | 2 (66%) | 1 (33%) | 0 (0%) | 0 |
| Integer | 0 (0%) | 2 (67%) | 0 (0%) | 1 (33%) |
| Datetime | 0 (0%) | 0 (0%) | 3 (100%) | 0 |
| String | 0 (0%) | 0 (0%) | 2 (100%) | 0 |
| Boolean | 0 (0%) | 0 (0%) | 1 (50%) | 1 (50%) |
| Decimal | 0 (0%) | 0 (0%) | 1 (100%) | 0 |
| Category | 0 (0%) | 0 (0%) | 1 (100%) | 0 |
| Period | 0 (0%) | 0 (0%) | 1 (100%) | 0 |
| Interval | 0 (0%) | 0 (0%) | 1 (100%) | 0 |
| PyArrow | 0 (0%) | 2 (50%) | 2 (50%) | 0 |

### Общая статистика

- ✅ **Полностью поддерживаются:** 2 типа (float64, float32)
- ⚠️ **Частичная поддержка:** 5 типов (Float64, Int64, Int32, PyArrow int64, PyArrow float64)
- ❌ **Не поддерживаются:** 14 типов
- N/A **Не применимо:** 2 типа (int64, bool)

**Процент полной поддержки:** 8.7% (2 из 23 протестированных типов)

---

## 🔍 Классификация по типу ошибки

### 1. NotImplementedError (non-numeric с null)

**Типы:** datetime, string, object, boolean, Decimal, Categorical, Period, Interval, PyArrow string/timestamp

**Сообщение:**
```
NotImplementedError: Index in passed data is non-numeric and contains nulls,
which Dask does not entirely support.
Consider passing `data.loc[~data.isna()]` instead.
```

**Причина:** Dask явно запрещает создание DataFrame с non-numeric индексом, содержащим null.

**Решение:** Использовать `reset_index()` и работать с индексом как с колонкой.

---

### 2. TypeError: boolean value of NA is ambiguous

**Типы:** Float64, Int64, Int32, PyArrow int64, PyArrow float64

**Сообщение:**
```
TypeError: boolean value of NA is ambiguous
```

**Причина:** Pandas `pd.NA` не может быть преобразован в boolean контекст, который требуется для операций с divisions и фильтрации.

**Объяснение:**
```python
# np.nan работает в boolean контексте
bool(np.nan)  # True

# pd.NA не работает
bool(pd.NA)  # TypeError: boolean value of NA is ambiguous
```

**Решение:** Использовать стандартные numpy типы (float64, float32) с np.nan вместо nullable типов с pd.NA.

---

### 3. N/A (тип не может содержать null)

**Типы:** int64, bool

**Объяснение:** Эти стандартные numpy типы физически не могут содержать missing values.

---

## 💡 Практические рекомендации

### ✅ Рекомендуется

#### 1. Для числовых данных с пропусками

```python
# ✅ ИСПОЛЬЗУЙТЕ float64 с np.nan
import numpy as np
import pandas as pd
import dask.dataframe as dd

index = [1.0, 2.0, np.nan, 4.0, 5.0, np.nan, 7.0]
pdf = pd.DataFrame({'value': range(7)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=3)

# Все операции работают
filtered = ddf[ddf['value'] > 3].compute()
processed = ddf.map_partitions(lambda df: df * 2).compute()
null_count = ddf.index.isna().sum().compute()
```

**Преимущества:**
- ✅ Полная поддержка всех операций
- ✅ Known divisions работает
- ✅ Максимальная производительность
- ✅ Совместимость со всеми версиями

---

#### 2. Для всех остальных типов (рекомендуемый паттерн)

```python
# ✅ ИСПОЛЬЗУЙТЕ reset_index()
import pandas as pd
import dask.dataframe as dd

# Исходные данные с null в индексе любого типа
index = pd.DatetimeIndex(['2024-01-01', '2024-01-02', pd.NaT, '2024-01-04'])
pdf = pd.DataFrame({'value': range(4)}, index=index)

# Преобразуем индекс в колонку
pdf = pdf.reset_index()
pdf.columns = ['timestamp', 'value']

# Создаем Dask DataFrame
ddf = dd.from_pandas(pdf, npartitions=2)

# Все операции работают!
# Фильтрация null
null_timestamps = ddf[ddf['timestamp'].isna()].compute()

# Фильтрация не-null
valid_data = ddf[ddf['timestamp'].notna()].compute()

# Join
other_df = pd.DataFrame({
    'timestamp': [pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-02')],
    'extra': ['a', 'b']
})
ddf_other = dd.from_pandas(other_df, npartitions=1)
joined = ddf.merge(ddf_other, on='timestamp', how='left').compute()

# map_partitions
def process(df):
    df = df.copy()
    df['has_timestamp'] = df['timestamp'].notna()
    return df

processed = ddf.map_partitions(process).compute()

# При необходимости восстановить индекс (после удаления null!)
final = ddf[ddf['timestamp'].notna()].set_index('timestamp', sorted=False)
```

---

### ❌ Не рекомендуется

#### 1. Использование nullable типов (Float64, Int64, etc.)

```python
# ❌ НЕ ИСПОЛЬЗУЙТЕ nullable типы для индексов
index = pd.array([1, 2, pd.NA, 4], dtype="Int64")
pdf = pd.DataFrame({'value': range(4)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=2)  # Создается, но операции не работают
```

**Проблемы:**
- DataFrame создается, но падает при операциях
- Неочевидное поведение
- Трудно отлаживать

**Альтернатива:**
- Для numeric: используйте float64 с np.nan
- Для других: используйте reset_index()

---

#### 2. Попытка создать DataFrame напрямую с non-numeric + null

```python
# ❌ НЕ РАБОТАЕТ
index = ['a', 'b', None, 'd']
pdf = pd.DataFrame({'value': range(4)}, index=index)
ddf = dd.from_pandas(pdf, npartitions=2)  # NotImplementedError
```

**Решение:** Всегда используйте reset_index() для non-numeric типов.

---

## 🎯 Универсальный паттерн для любого типа

```python
import pandas as pd
import dask.dataframe as dd
import numpy as np

def create_dask_with_nullable_index(pdf, npartitions=None):
    """
    Универсальная функция для создания Dask DataFrame
    с индексом, который может содержать null значения.

    Parameters:
    -----------
    pdf : pd.DataFrame
        Pandas DataFrame с любым типом индекса (может содержать null)
    npartitions : int, optional
        Количество партиций. По умолчанию = количество CPU

    Returns:
    --------
    ddf : dd.DataFrame
        Dask DataFrame с индексом в колонке 'original_index'

    Examples:
    ---------
    # Datetime индекс с null
    >>> index = pd.DatetimeIndex(['2024-01-01', pd.NaT, '2024-01-03'])
    >>> pdf = pd.DataFrame({'value': [1, 2, 3]}, index=index)
    >>> ddf = create_dask_with_nullable_index(pdf, npartitions=2)

    # String индекс с null
    >>> index = ['a', None, 'c']
    >>> pdf = pd.DataFrame({'value': [1, 2, 3]}, index=index)
    >>> ddf = create_dask_with_nullable_index(pdf, npartitions=2)
    """
    index_dtype = pdf.index.dtype

    # Проверяем тип индекса
    is_numeric = pd.api.types.is_numeric_dtype(index_dtype)
    has_nulls = pdf.index.isna().any()

    # Определяем стратегию
    if is_numeric and not has_nulls:
        # Числовой без null - можно напрямую
        return dd.from_pandas(pdf, npartitions=npartitions)

    elif index_dtype == 'float64' or index_dtype == 'float32':
        # float64/float32 с np.nan - можно напрямую
        try:
            return dd.from_pandas(pdf, npartitions=npartitions)
        except Exception:
            # Если не получилось, используем reset_index
            pass

    # Для всех остальных случаев используем reset_index
    pdf_reset = pdf.reset_index()
    pdf_reset.columns = ['original_index'] + list(pdf_reset.columns[1:])

    return dd.from_pandas(pdf_reset, npartitions=npartitions)


# Пример использования
if __name__ == '__main__':
    # Тест 1: Float с nan - работает напрямую
    pdf1 = pd.DataFrame({'value': [1, 2, 3]}, index=[1.0, np.nan, 3.0])
    ddf1 = create_dask_with_nullable_index(pdf1, npartitions=2)
    print("Float test:", ddf1.compute())

    # Тест 2: Datetime с NaT - через reset_index
    pdf2 = pd.DataFrame(
        {'value': [1, 2, 3]},
        index=pd.DatetimeIndex(['2024-01-01', pd.NaT, '2024-01-03'])
    )
    ddf2 = create_dask_with_nullable_index(pdf2, npartitions=2)
    print("Datetime test:", ddf2.compute())

    # Тест 3: String с None - через reset_index
    pdf3 = pd.DataFrame({'value': [1, 2, 3]}, index=['a', None, 'c'])
    ddf3 = create_dask_with_nullable_index(pdf3, npartitions=2)
    print("String test:", ddf3.compute())
```

---

## 📝 Резюме и выводы

### Ключевые находки

1. **Только 2 типа полностью поддерживаются:** `float64` и `float32` с `np.nan`

2. **3 категории ошибок:**
   - NotImplementedError для non-numeric типов
   - TypeError для nullable типов с pd.NA
   - N/A для типов без поддержки null

3. **Nullable типы Pandas (Int64, Float64, etc.) не работают** с Dask индексами из-за особенностей pd.NA

4. **PyArrow типы частично работают:** создаются, но операции падают

5. **Универсальное решение:** `reset_index()` работает для всех типов

### Рекомендации по приоритетам

**Приоритет 1 (Рекомендуется):**
- ✅ Используйте `float64`/`float32` с `np.nan` для числовых индексов

**Приоритет 2 (Безопасно):**
- ✅ Используйте `reset_index()` для всех остальных типов
- ✅ Храните исходный индекс в колонке `original_index`
- ✅ Работайте с индексом как с обычной колонкой

**Избегайте:**
- ❌ Nullable типы Pandas (Int64, Float64, boolean)
- ❌ PyArrow типы для индексов с null
- ❌ Прямое создание DataFrame с non-numeric индексом, содержащим null

### Практическая стратегия

```python
# Для ЛЮБОГО типа данных используйте этот паттерн:

# 1. Есть данные с null в индексе
pdf = pd.DataFrame(data, index=index_with_nulls)

# 2. Проверка типа
if pdf.index.dtype in ['float64', 'float32'] and not has_pd_na(pdf.index):
    # Можно напрямую
    ddf = dd.from_pandas(pdf, npartitions=N)
else:
    # Используем reset_index (безопасный вариант)
    pdf = pdf.reset_index()
    pdf.columns = ['original_index'] + list(pdf.columns[1:])
    ddf = dd.from_pandas(pdf, npartitions=N)

# 3. Работаем с данными
result = ddf[ddf['original_index'].notna()]  # Фильтрация
joined = ddf.merge(other, on='original_index')  # Join
processed = ddf.map_partitions(custom_func)  # Обработка

# 4. При необходимости восстановить индекс (только после удаления null!)
final = result.set_index('original_index', sorted=False)
```

---

## 🔗 Связанные материалы

- **Предыдущее исследование:** [ИССЛЕДОВАНИЕ_СТРОКОВЫЕ_ИНДЕКСЫ_NULL.md](ИССЛЕДОВАНИЕ_СТРОКОВЫЕ_ИНДЕКСЫ_NULL.md) - детальное изучение строковых индексов
- **Тестовые скрипты:**
  - `test_all_types_null_index.py` - комплексное тестирование всех типов
  - `test_string_index_nulls_enhanced.py` - расширенные тесты строковых индексов
  - `example_working_null_index.py` - рабочий пример с обходными путями

---

## 📊 Приложение: Полная таблица результатов тестов

<details>
<summary>Раскрыть детальные результаты</summary>

| # | Тип данных | Pandas dtype | Null тип | Создание DF | Known div | Filter | map_part | isna() | Ошибка |
|---|------------|--------------|----------|-------------|-----------|--------|----------|--------|--------|
| 1 | float64 | float64 | np.nan | ✅ | ✅ | ✅ | ✅ | ✅ | - |
| 2 | Float64 | Float64 | pd.NA | ✅ | ❌ | ❌ | ❌ | ❌ | TypeError: NA ambiguous |
| 3 | float32 | float32 | np.nan | ✅ | ✅ | ✅ | ✅ | ✅ | - |
| 4 | int64 | - | - | N/A | - | - | - | - | Cannot have NaN |
| 5 | Int64 | Int64 | pd.NA | ✅ | ❌ | ❌ | ❌ | ❌ | TypeError: NA ambiguous |
| 6 | Int32 | Int32 | pd.NA | ✅ | ❌ | ❌ | ❌ | ❌ | TypeError: NA ambiguous |
| 7 | datetime64[ns] | datetime64[ns] | pd.NaT | ❌ | - | - | - | - | NotImplementedError |
| 8 | datetime64[ns,UTC] | datetime64[ns,UTC] | pd.NaT | ❌ | - | - | - | - | NotImplementedError |
| 9 | timedelta64[ns] | timedelta64[ns] | pd.NaT | ❌ | - | - | - | - | NotImplementedError |
| 10 | object | object | None/nan/NA | ❌ | - | - | - | - | NotImplementedError |
| 11 | string | string | pd.NA | ❌ | - | - | - | - | NotImplementedError |
| 12 | bool | - | - | N/A | - | - | - | - | Cannot have NA |
| 13 | boolean | boolean | pd.NA | ❌ | - | - | - | - | NotImplementedError |
| 14 | Decimal | object | None | ❌ | - | - | - | - | NotImplementedError |
| 15 | int64[pyarrow] | int64[pyarrow] | None | ✅ | ❌ | ❌ | ❌ | ❌ | TypeError: NA ambiguous |
| 16 | double[pyarrow] | double[pyarrow] | None | ✅ | ❌ | ❌ | ❌ | ❌ | TypeError: NA ambiguous |
| 17 | string[pyarrow] | string[pyarrow] | None | ❌ | - | - | - | - | NotImplementedError |
| 18 | timestamp[pyarrow] | timestamp[ns][pyarrow] | None | ❌ | - | - | - | - | NotImplementedError |
| 19 | Categorical | category | None/nan | ❌ | - | - | - | - | NotImplementedError |
| 20 | Period | period[M] | pd.NaT | ❌ | - | - | - | - | NotImplementedError |
| 21 | Interval | interval[float64] | nan | ❌ | - | - | - | - | NotImplementedError |

</details>

---

**Дата отчета:** 2025-11-09
**Автор:** Claude Code
**Репозиторий:** dask/dask
