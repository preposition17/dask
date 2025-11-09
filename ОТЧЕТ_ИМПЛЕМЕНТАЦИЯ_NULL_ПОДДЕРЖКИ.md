# Отчет: Имплементация поддержки null значений в индексах Dask DataFrame

## 📋 Резюме

**Дата:** 2025-11-09
**Версия Dask:** 2025.11.0
**Статус:** Технический анализ и предложения по реализации

### Краткое описание проблемы

Dask DataFrame **не поддерживает** индексы с null значениями для:
- **Non-numeric типов** (datetime, string, object, etc.) - выбрасывается `NotImplementedError`
- **Nullable типов** (Int64, Float64, PyArrow) - DataFrame создается, но операции падают с `TypeError: boolean value of NA is ambiguous`

Только **float64/float32** с `np.nan` работают полностью.

### Цель отчета

Проанализировать:
1. **Текущую реализацию** и механизм divisions в Dask
2. **Root causes** проблем с null значениями
3. **Возможные подходы** к имплементации поддержки
4. **Сложность реализации** и трудозатраты
5. **Риски** и рекомендации

---

## 🔍 Анализ текущей реализации

### 1. Архитектура Dask DataFrame

Dask DataFrame использует концепцию **divisions** (границ партиций) для:
- Оптимизации операций (фильтрация, join, merge)
- Определения известности индекса (known_divisions)
- Партиционирования данных

```
DataFrame с 3 партициями:
┌──────────────┬──────────────┬──────────────┐
│  Partition 0 │  Partition 1 │  Partition 2 │
│  [0 ... 10]  │  [10 ... 20] │  [20 ... 30] │
└──────────────┴──────────────┴──────────────┘
Divisions: [0, 10, 20, 30]
```

**Требования к divisions:**
1. Должны быть **отсортированы** (проверяется сравнением)
2. Должны быть **уникальны** (кроме последнего элемента)
3. Должны поддерживать **операции сравнения** (<, >, ==, !=)

---

### 2. Проблемные места в коде

#### 2.1. Точка входа: from_pandas (dask_expr/_collection.py:4896-4900)

```python
if data.index.isna().any() and not _is_any_real_numeric_dtype(data.index):
    raise NotImplementedError(
        "Index in passed data is non-numeric and contains nulls, "
        "which Dask does not entirely support.\n"
        "Consider passing `data.loc[~data.isna()]` instead."
    )
```

**Проблема:** Жесткое ограничение на non-numeric индексы с null.

**Почему введено:**
- Защита от ошибок в последующем коде
- Избежание неопределенного поведения

---

#### 2.2. Проверка divisions (core.py:257-268)

```python
def check_divisions(divisions):
    if not isinstance(divisions, (list, tuple)):
        raise ValueError("New division must be list or tuple")
    divisions = list(divisions)
    if len(divisions) == 0:
        raise ValueError("New division must not be empty")
    if divisions != sorted(divisions):  # ← ПРОБЛЕМА!
        raise ValueError("New division must be sorted")
    if len(divisions[:-1]) != len(list(unique(divisions[:-1]))):
        msg = "New division must be unique, except for the last element"
        raise ValueError(msg)
```

**Проблема:**
```python
# С pd.NA:
divisions = [1, pd.NA, 3]
sorted_divs = sorted(divisions)  # [1, 3, <NA>]
divisions != sorted_divs  # [False, <NA>, False]
# Преобразование в bool: TypeError: boolean value of NA is ambiguous
```

**Затронутые типы:**
- Int64, Float64, boolean (pandas nullable)
- PyArrow int64, float64
- Любые типы с pd.NA

---

#### 2.3. Валидация divisions (utils.py:668-709)

```python
def valid_divisions(divisions):
    """Are the provided divisions valid?"""
    if not isinstance(divisions, (tuple, list)):
        return False

    if isinstance(divisions, tuple):
        divisions = list(divisions)

    if pd.isnull(divisions).any():
        return False  # ← Автоматически отклоняет divisions с null

    for i, x in enumerate(divisions[:-2]):
        if x >= divisions[i + 1]:  # ← ПРОБЛЕМА с nullable типами
            return False
        if isinstance(x, Number) and math.isnan(x):
            return False

    for x in divisions[-2:]:
        if isinstance(x, Number) and math.isnan(x):
            return False

    return divisions[-2] <= divisions[-1]  # ← ПРОБЛЕМА
```

**Проблемы:**
1. `pd.isnull(divisions).any()` возвращает `False` для divisions с null → они отклоняются
2. Операции сравнения (`>=`, `<=`) с nullable типами возвращают `pd.NA` вместо bool
3. Нет специальной обработки null значений в divisions

---

#### 2.4. Партиционирование с searchsorted (shuffle.py:110-138)

```python
def set_partitions_pre(s, divisions, ascending=True, na_position="last"):
    try:
        if ascending:
            partitions = divisions.searchsorted(s, side="right") - 1  # ← ПРОБЛЕМА
        else:
            partitions = len(divisions) - divisions.searchsorted(s, side="right") - 1
    except (TypeError, ValueError):
        # searchsorted fails if either divisions or s contains nulls and strings
        partitions = np.empty(len(s), dtype="int32")
        not_null = s.notna()
        divisions_notna = divisions[divisions.notna()]
        if ascending:
            partitions[not_null] = (
                divisions_notna.searchsorted(s[not_null], side="right") - 1
            )
        # ...

    nas = s.isna()
    partitions[nas] = len(divisions) - 2 if na_position == "last" else 0
    return partitions
```

**Проблема:**
```python
# searchsorted требует отсортированный массив
divisions = pd.array([1, pd.NA, 3], dtype="Int64")
s = pd.array([2], dtype="Int64")
divisions.searchsorted(s)  # ValueError: array must be sorted!
```

**Текущее решение:** Try-except блок с фильтрацией null, но:
- Работает только для некоторых случаев
- Неполная поддержка nullable типов
- Может давать неверные результаты

---

#### 2.5. Расчет divisions при shuffle (_shuffle.py:1340-1428)

```python
def _calculate_divisions(frame, other, npartitions, ...):
    # ...
    divisions, mins, maxes = compute(
        new_collection(RepartitionQuantiles(other, npartitions, upsample=upsample)),
        new_collection(other).map_partitions(M.min),
        new_collection(other).map_partitions(M.max),
    )

    # Проверка presorted
    if mins.isna().any() or maxes.isna().any():
        presorted = False
    else:
        maxes2 = maxes.iloc[: n - 1].reset_index(drop=True)
        mins2 = mins.iloc[1:].reset_index(drop=True)
        presorted = (
            mins.tolist() == mins.sort_values(ascending=ascending).tolist()  # ← ПРОБЛЕМА
            and maxes.tolist() == maxes.sort_values(ascending=ascending).tolist()
            and (maxes2 < mins2).all()  # ← ПРОБЛЕМА: сравнение с NA
        )
```

**Проблемы:**
1. Сравнение списков с pd.NA может вернуть pd.NA
2. `(maxes2 < mins2).all()` может содержать pd.NA → ambiguous boolean

---

### 3. Почему float64 работает, а Int64 нет?

| Аспект | float64 + np.nan | Int64 + pd.NA |
|--------|------------------|---------------|
| Null значение | `np.nan` (float) | `pd.NA` (singleton) |
| Сравнение с null | `np.nan < 5` → `False` | `pd.NA < 5` → `pd.NA` |
| Boolean контекст | `bool(np.nan)` → `True` | `bool(pd.NA)` → `TypeError` |
| Сортировка | `sorted([1, np.nan, 3])` → `[1, 3, nan]` | `sorted([1, pd.NA, 3])` → работает, но сравнения возвращают NA |
| searchsorted | Работает с NaN | **Не работает** с pd.NA |

**Ключевое отличие:** `np.nan` ведет себя как обычное float значение (хоть и специальное), в то время как `pd.NA` - это специальный объект с "propagating" семантикой (любая операция с NA возвращает NA).

---

## 🎯 Варианты решения

### Вариант 1: Минимальная имплементация (Простой)

**Цель:** Расширить поддержку на datetime и string типы с NaT/None.

#### Подход

1. **Убрать проверку в from_pandas** для datetime типов:
```python
# Было:
if data.index.isna().any() and not _is_any_real_numeric_dtype(data.index):
    raise NotImplementedError(...)

# Стало:
if data.index.isna().any() and not (_is_any_real_numeric_dtype(data.index)
                                      or _is_datetime_dtype(data.index)
                                      or _is_timedelta_dtype(data.index)):
    raise NotImplementedError(...)
```

2. **Обновить check_divisions** для обработки NaT:
```python
def check_divisions(divisions):
    if not isinstance(divisions, (list, tuple)):
        raise ValueError("New division must be list or tuple")
    divisions = list(divisions)
    if len(divisions) == 0:
        raise ValueError("New division must not be empty")

    # Новая логика: фильтруем NaT/None для сравнения
    non_null_divs = [d for d in divisions if pd.notna(d)]
    if non_null_divs != sorted(non_null_divs):
        raise ValueError("New division must be sorted")

    # Остальные проверки...
```

3. **Обновить valid_divisions** для datetime:
```python
def valid_divisions(divisions):
    # ...
    # Разрешить null для datetime типов
    if pd.isnull(divisions).any():
        if not _is_datetime_like(divisions[0]):
            return False

    # Сравнения только для non-null значений
    for i, x in enumerate(divisions[:-2]):
        if pd.notna(x) and pd.notna(divisions[i + 1]):
            if x >= divisions[i + 1]:
                return False
    # ...
```

#### Сложность реализации

**Оценка:** 🟢 Низкая (1-2 недели)

**Компоненты:**
- ✅ Изменения локализованы (3-4 файла)
- ✅ Не требует архитектурных изменений
- ✅ Можно протестировать инкрементально

**Трудозатраты:**
- Код: 20-30 часов
- Тесты: 15-20 часов
- Документация: 5 часов
- **Итого: 40-55 часов**

#### Что улучшится

| Тип | До | После |
|-----|----|----|
| datetime64[ns] + NaT | ❌ | ✅ |
| timedelta64[ns] + NaT | ❌ | ✅ |
| float64 + nan | ✅ | ✅ |
| object (string) | ❌ | ❌ |
| Int64 (nullable) | ⚠️ | ⚠️ |

**Ограничения:**
- ❌ Не решает проблему с nullable типами (Int64, Float64)
- ❌ Не поддерживает string с None
- ❌ Частичное решение

---

### Вариант 2: Поддержка nullable типов (Средний)

**Цель:** Добавить поддержку pandas nullable типов (Int64, Float64, boolean).

#### Подход

1. **Создать NA-aware функции сравнения:**
```python
def _na_safe_compare_lt(a, b):
    """Compare a < b, treating pd.NA as False (not propagating)"""
    if pd.isna(a) or pd.isna(b):
        return False
    result = a < b
    # Handle pd.NA result
    if isinstance(result, type(pd.NA)):
        return False
    return bool(result)

def _na_safe_compare_eq(a, b):
    """Compare a == b, treating pd.NA specially"""
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    result = a == b
    if isinstance(result, type(pd.NA)):
        return False
    return bool(result)
```

2. **Обновить check_divisions:**
```python
def check_divisions(divisions):
    # ...
    # Использовать NA-aware сортировку
    def na_safe_sort_key(x):
        if pd.isna(x):
            return (1, 0)  # NA всегда в конец
        return (0, x)

    sorted_divisions = sorted(divisions, key=na_safe_sort_key)
    if not all(_na_safe_compare_eq(a, b) for a, b in zip(divisions, sorted_divisions)):
        raise ValueError("New division must be sorted")
```

3. **Обновить searchsorted логику:**
```python
def set_partitions_pre(s, divisions, ascending=True, na_position="last"):
    # Всегда использовать fallback для nullable типов
    if _is_nullable_dtype(s.dtype) or _is_nullable_dtype(divisions.dtype):
        # Специальная обработка
        partitions = np.empty(len(s), dtype="int32")
        not_null_mask = s.notna()

        # Фильтруем null из divisions
        divisions_clean = divisions[divisions.notna()]

        # searchsorted только для non-null значений
        if len(divisions_clean) > 0:
            s_clean = s[not_null_mask]
            # Конвертируем в numpy для searchsorted (если nullable)
            if hasattr(s_clean, 'to_numpy'):
                s_clean = s_clean.to_numpy(dtype=s_clean.dtype.numpy_dtype, na_value=None)
            partitions[not_null_mask] = divisions_clean.searchsorted(s_clean, side="right") - 1

        # Null значения
        partitions[~not_null_mask] = len(divisions) - 2 if na_position == "last" else 0
        return partitions

    # Старая логика для обычных типов
    # ...
```

4. **Добавить helper функции:**
```python
def _is_nullable_dtype(dtype):
    """Check if dtype is pandas nullable or PyArrow"""
    from pandas.api.types import is_extension_array_dtype

    if is_extension_array_dtype(dtype):
        # Int64, Float64, boolean, string, PyArrow
        return True
    return False

def _convert_to_comparable(arr):
    """Convert nullable dtype to comparable numpy array"""
    if _is_nullable_dtype(arr.dtype):
        # Заменяем NA на sentinel значение
        if hasattr(arr, 'to_numpy'):
            # Для nullable integers/floats
            return arr.to_numpy(dtype='float64', na_value=np.nan)
    return arr
```

#### Сложность реализации

**Оценка:** 🟡 Средняя (3-4 недели)

**Компоненты:**
- ⚠️ Изменения в нескольких подсистемах (5-7 файлов)
- ⚠️ Требует новые helper функции
- ⚠️ Сложное тестирование edge cases

**Трудозатраты:**
- Код: 40-60 часов
- Тесты: 30-40 часов (много edge cases!)
- Документация: 8-10 часов
- Code review и итерации: 15-20 часов
- **Итого: 93-130 часов (2.5-3.5 недели)**

#### Что улучшится

| Тип | До | После |
|-----|----|----|
| datetime64[ns] + NaT | ❌ | ✅ |
| Int64 (nullable) | ⚠️ | ✅ |
| Float64 (nullable) | ⚠️ | ✅ |
| boolean (nullable) | ❌ | ✅ |
| PyArrow int64 | ⚠️ | ✅ |
| PyArrow float64 | ⚠️ | ✅ |
| object/string | ❌ | ❌ |

**Преимущества:**
- ✅ Решает проблему с nullable типами
- ✅ Совместимо с современными pandas практиками
- ✅ Поддержка PyArrow типов

**Ограничения:**
- ❌ Все еще не поддерживает object/string
- ⚠️ Производительность может снизиться из-за дополнительных проверок

---

### Вариант 3: Полная поддержка всех типов (Сложный)

**Цель:** Поддержать все типы данных с null, включая string/object.

#### Подход

Требует **архитектурных изменений** в концепции divisions:

1. **Новая система divisions:**
```python
class DivisionInfo:
    """Enhanced division information with null awareness"""
    def __init__(self, values, null_positions=None):
        self.values = values  # List of division values
        self.null_positions = null_positions or []  # Where nulls are
        self.dtype = self._infer_dtype(values)

    def is_sorted(self):
        """Check if divisions are sorted, accounting for nulls"""
        non_null_vals = [v for i, v in enumerate(self.values)
                         if i not in self.null_positions]
        return non_null_vals == sorted(non_null_vals)

    def compare(self, value):
        """Compare value with divisions, handling nulls"""
        if pd.isna(value):
            return self._null_partition_index()
        # ... NA-aware comparison logic
```

2. **Альтернативная стратегия для non-sortable типов:**
```python
def create_divisions_for_unsortable(data, npartitions):
    """Create divisions for types that can't be easily sorted (e.g., strings with null)"""
    # Опция 1: Использовать None divisions (unknown divisions)
    return (None,) * (npartitions + 1)

    # Опция 2: Hash-based partitioning
    # Вместо диапазонов, использовать хеши
    # Partition = hash(value) % npartitions
```

3. **Режим работы с unknown divisions:**

Для типов, которые трудно обработать (string с null), можно:
- Создавать DataFrame с `known_divisions=False`
- Использовать hash-based партиционирование вместо range-based
- Терять некоторые оптимизации, но сохранять функциональность

```python
def from_pandas(data, npartitions=None, sort=True, handle_nulls='auto'):
    """
    handle_nulls: 'auto', 'drop', 'allow_unknown_divisions'

    - 'auto': Пытается создать divisions, если не получается - unknown
    - 'drop': Удаляет строки с null (текущее поведение с предупреждением)
    - 'allow_unknown_divisions': Всегда создает с unknown divisions для типов с null
    """
    # ...
```

4. **Refactoring operations для работы с null:**

Многие операции нужно обновить:
- `merge`/`join` - обработка null ключей
- `groupby` - null группы
- `sort_values` - позиция null
- `loc`/`iloc` - индексация с null

#### Сложность реализации

**Оценка:** 🔴 Высокая (2-3 месяца)

**Компоненты:**
- 🔴 Архитектурные изменения в core
- 🔴 Изменения во множестве операций (15+ файлов)
- 🔴 Backward compatibility challenges
- 🔴 Обширное тестирование

**Трудозатраты:**
- Дизайн архитектуры: 40-60 часов
- Код (core changes): 80-120 часов
- Код (operations updates): 60-100 часов
- Тесты: 80-120 часов
- Документация: 20-30 часов
- Code review, рефакторинг: 40-60 часов
- **Итого: 320-490 часов (2-3 месяца)**

#### Что улучшится

| Тип | До | После |
|-----|----|----|
| **ВСЕ ТИПЫ** | Частично/Нет | ✅ |
| datetime64[ns] | ❌ | ✅ |
| string/object | ❌ | ✅ |
| Int64/Float64 | ⚠️ | ✅ |
| PyArrow все | Частично | ✅ |
| Categorical | ❌ | ✅ |
| Period/Interval | ❌ | ✅ |

**Преимущества:**
- ✅ Полная поддержка всех типов
- ✅ Consistent поведение
- ✅ Открывает новые возможности

**Риски:**
- 🔴 Breaking changes возможны
- 🔴 Performance regression
- 🔴 Длительная разработка
- 🔴 Сложность поддержки

---

## 📊 Сравнительная таблица вариантов

| Критерий | Вариант 1 (Простой) | Вариант 2 (Средний) | Вариант 3 (Полный) |
|----------|---------------------|---------------------|--------------------|
| **Сложность** | 🟢 Низкая | 🟡 Средняя | 🔴 Высокая |
| **Время разработки** | 1-2 недели | 3-4 недели | 2-3 месяца |
| **Трудозатраты** | 40-55 ч | 93-130 ч | 320-490 ч |
| **Затронутые файлы** | 3-4 | 5-7 | 15+ |
| **Поддержка datetime** | ✅ | ✅ | ✅ |
| **Поддержка nullable** | ❌ | ✅ | ✅ |
| **Поддержка string** | ❌ | ❌ | ✅ |
| **Breaking changes** | Минимум | Минимум | Возможны |
| **Performance impact** | Минимум | Небольшой | Средний |
| **Риски** | Низкие | Средние | Высокие |
| **Поддерживаемость** | ✅ Легкая | ✅ Средняя | ⚠️ Сложная |

---

## 🛠️ Рекомендуемый план имплементации

### Фаза 1: Quick Win (Вариант 1 - Datetime поддержка)

**Цель:** Быстро добавить поддержку datetime/timedelta с NaT

**Шаги:**
1. Обновить `from_pandas` в `dask_expr/_collection.py`
2. Обновить `check_divisions` в `core.py`
3. Обновить `valid_divisions` в `utils.py`
4. Добавить тесты для datetime с NaT
5. Обновить документацию

**Результат:**
- ✅ datetime64[ns] работает с NaT
- ✅ timedelta64[ns] работает с NaT
- 📈 Покрытие поддержки: 9.5% → ~20%

**Срок:** 1-2 недели

---

### Фаза 2: Nullable Types Support (Вариант 2)

**Цель:** Добавить поддержку pandas nullable и PyArrow типов

**Шаги:**
1. Создать helper модуль `_nullable_utils.py`:
   - NA-aware comparison функции
   - Детекция nullable типов
   - Конверсия для операций
2. Обновить `check_divisions` с NA-aware логикой
3. Обновить `valid_divisions`
4. Обновить `set_partitions_pre` в `shuffle.py`
5. Обновить `_calculate_divisions` в `_shuffle.py`
6. Добавить comprehensive тесты
7. Обновить документацию с примерами

**Результат:**
- ✅ Int64, Float64, boolean (nullable) работают
- ✅ PyArrow int64, float64, timestamp работают
- 📈 Покрытие поддержки: ~20% → ~50%

**Срок:** 3-4 недели после Фазы 1

---

### Фаза 3: Full Support (Опционально - Вариант 3)

**Цель:** Полная поддержка всех типов, включая string/object

**Шаги:**
1. Дизайн новой архитектуры divisions
2. Proof-of-concept имплементация
3. Рефакторинг core компонентов
4. Обновление всех операций
5. Миграционный план для breaking changes
6. Extensive testing
7. Performance benchmarking
8. Документация и migration guide

**Результат:**
- ✅ Все типы данных поддерживаются
- 📈 Покрытие поддержки: 100%

**Срок:** 2-3 месяца после Фазы 2

**Решение:** Требует обсуждения с core maintainers

---

## 📋 Детальный план работ (Фаза 1 + Фаза 2)

### Файлы для изменения

#### 1. `dask/dataframe/dask_expr/_collection.py`

**Изменение:** Ослабить проверку для datetime типов

```python
# Строки 4896-4900
# БЫЛО:
if data.index.isna().any() and not _is_any_real_numeric_dtype(data.index):
    raise NotImplementedError(...)

# СТАЛО:
from pandas.api.types import is_datetime64_any_dtype, is_timedelta64_dtype

if data.index.isna().any() and not (
    _is_any_real_numeric_dtype(data.index)
    or is_datetime64_any_dtype(data.index)
    or is_timedelta64_dtype(data.index)
    or _is_nullable_numeric_dtype(data.index)  # Фаза 2
):
    raise NotImplementedError(...)
```

**Трудозатраты:** 2-3 часа

---

#### 2. `dask/dataframe/core.py`

**Изменение:** NA-aware проверка divisions

```python
# Строки 257-268
def check_divisions(divisions):
    """Check that divisions are valid"""
    if not isinstance(divisions, (list, tuple)):
        raise ValueError("New division must be list or tuple")
    divisions = list(divisions)
    if len(divisions) == 0:
        raise ValueError("New division must not be empty")

    # NA-aware sorting check
    def sort_key(x):
        """Sort key that puts NA at the end"""
        if pd.isna(x):
            return (1, 0)  # NA sorts last
        return (0, x)

    try:
        sorted_divisions = sorted(divisions, key=sort_key)
    except TypeError:
        # Can't sort - likely incompatible types
        raise ValueError("Divisions contain incompatible types")

    # NA-aware equality check
    for orig, sorted_val in zip(divisions, sorted_divisions):
        if not _na_equals(orig, sorted_val):
            raise ValueError("New division must be sorted")

    # Uniqueness check (excluding last element)
    non_null_divs = [d for d in divisions[:-1] if pd.notna(d)]
    if len(non_null_divs) != len(set(non_null_divs)):
        raise ValueError("New division must be unique, except for the last element")

def _na_equals(a, b):
    """Check equality accounting for NA values"""
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    try:
        result = a == b
        # Handle pd.NA propagation
        if isinstance(result, type(pd.NA)):
            return False
        return bool(result)
    except TypeError:
        return False
```

**Трудозатраты:** 4-6 часов

---

#### 3. `dask/dataframe/utils.py`

**Изменение:** Обновить valid_divisions

```python
# Строки 668-709
def valid_divisions(divisions):
    """Are the provided divisions valid?"""
    if not isinstance(divisions, (tuple, list)):
        return False

    if isinstance(divisions, tuple):
        divisions = list(divisions)

    # Allow null for datetime and nullable types
    has_nulls = pd.isnull(divisions).any()
    if has_nulls:
        from pandas.api.types import is_datetime64_any_dtype, is_timedelta64_dtype
        # Check if first non-null element is datetime-like or nullable
        first_non_null = next((d for d in divisions if pd.notna(d)), None)
        if first_non_null is None:
            return False  # All null divisions

        is_datetime_like = (
            is_datetime64_any_dtype(type(first_non_null))
            or is_timedelta64_dtype(type(first_non_null))
            or _is_nullable_dtype(type(first_non_null))  # Фаза 2
        )

        if not is_datetime_like:
            return False

    # NA-aware comparison
    for i, x in enumerate(divisions[:-2]):
        y = divisions[i + 1]
        if pd.notna(x) and pd.notna(y):
            try:
                result = x >= y
                # Handle pd.NA result
                if isinstance(result, type(pd.NA)):
                    return False
                if result:
                    return False
            except TypeError:
                return False

        # Check for NaN in numeric types
        if isinstance(x, Number) and not isinstance(x, (bool, np.bool_)):
            try:
                if math.isnan(x):
                    return False
            except (TypeError, ValueError):
                pass

    # Check last two elements
    for x in divisions[-2:]:
        if isinstance(x, Number) and not isinstance(x, (bool, np.bool_)):
            try:
                if math.isnan(x):
                    return False
            except (TypeError, ValueError):
                pass

    # Final comparison
    a, b = divisions[-2], divisions[-1]
    if pd.notna(a) and pd.notna(b):
        try:
            result = a <= b
            if isinstance(result, type(pd.NA)):
                return False
            return bool(result)
        except TypeError:
            return False

    return True
```

**Трудозатраты:** 5-7 часов

---

#### 4. `dask/dataframe/shuffle.py`

**Изменение:** Улучшить обработку nullable типов в set_partitions_pre

```python
# Строки 110-138
def set_partitions_pre(s, divisions, ascending=True, na_position="last"):
    """Determine partition assignments for values"""

    # Detect nullable types early
    is_nullable_index = _is_nullable_dtype(s.dtype)
    is_nullable_divs = (
        hasattr(divisions, 'dtype') and _is_nullable_dtype(divisions.dtype)
    )

    # For nullable types, always use the fallback path
    if is_nullable_index or is_nullable_divs:
        return _set_partitions_nullable(s, divisions, ascending, na_position)

    # Original fast path for standard types
    try:
        if ascending:
            partitions = divisions.searchsorted(s, side="right") - 1
        else:
            partitions = len(divisions) - divisions.searchsorted(s, side="right") - 1
    except (TypeError, ValueError):
        # Fallback for edge cases
        return _set_partitions_nullable(s, divisions, ascending, na_position)

    # Handle NA values
    nas = s.isna()
    if hasattr(nas, 'values'):
        nas = nas.values
    partitions[nas] = len(divisions) - 2 if na_position == "last" else 0

    return partitions


def _set_partitions_nullable(s, divisions, ascending=True, na_position="last"):
    """Partition assignment for nullable dtypes"""
    partitions = np.empty(len(s), dtype="int32")
    not_null = s.notna()

    # Get non-null divisions
    if hasattr(divisions, 'notna'):
        divisions_mask = divisions.notna()
        divisions_notna = divisions[divisions_mask]
    else:
        divisions_notna = [d for d in divisions if pd.notna(d)]
        divisions_notna = pd.Index(divisions_notna)

    if len(divisions_notna) > 0:
        s_clean = s[not_null]

        # Convert to comparable format if needed
        if _is_nullable_dtype(s_clean.dtype):
            s_clean = _to_comparable_array(s_clean)
        if hasattr(divisions_notna, 'dtype') and _is_nullable_dtype(divisions_notna.dtype):
            divisions_notna = _to_comparable_array(divisions_notna)

        # Now safe to use searchsorted
        if ascending:
            partitions[not_null] = divisions_notna.searchsorted(s_clean, side="right") - 1
        else:
            partitions[not_null] = (
                len(divisions) - divisions_notna.searchsorted(s_clean, side="right") - 1
            )

    # Assign NA values
    partitions[~not_null] = len(divisions) - 2 if na_position == "last" else 0

    return partitions
```

**Трудозатраты:** 6-8 часов

---

#### 5. Новый файл: `dask/dataframe/_nullable_utils.py`

**Назначение:** Helper функции для работы с nullable типами

```python
"""Utilities for handling nullable dtypes (Int64, Float64, PyArrow, etc.)"""

import numpy as np
import pandas as pd
from pandas.api.types import is_extension_array_dtype


def _is_nullable_dtype(dtype):
    """Check if dtype is a nullable pandas or PyArrow type"""
    # Pandas nullable types
    nullable_types = (
        pd.Int8Dtype, pd.Int16Dtype, pd.Int32Dtype, pd.Int64Dtype,
        pd.UInt8Dtype, pd.UInt16Dtype, pd.UInt32Dtype, pd.UInt64Dtype,
        pd.Float32Dtype, pd.Float64Dtype,
        pd.BooleanDtype,
    )

    if isinstance(dtype, nullable_types):
        return True

    # PyArrow types
    if hasattr(pd, 'ArrowDtype'):
        if isinstance(dtype, pd.ArrowDtype):
            return True

    # String dtype
    if isinstance(dtype, pd.StringDtype):
        return True

    return False


def _is_nullable_numeric_dtype(arr_or_dtype):
    """Check if dtype is nullable numeric (Int64, Float64, etc.)"""
    dtype = getattr(arr_or_dtype, 'dtype', arr_or_dtype)

    numeric_nullable = (
        pd.Int8Dtype, pd.Int16Dtype, pd.Int32Dtype, pd.Int64Dtype,
        pd.UInt8Dtype, pd.UInt16Dtype, pd.UInt32Dtype, pd.UInt64Dtype,
        pd.Float32Dtype, pd.Float64Dtype,
    )

    if isinstance(dtype, numeric_nullable):
        return True

    # PyArrow numeric
    if hasattr(pd, 'ArrowDtype') and isinstance(dtype, pd.ArrowDtype):
        import pyarrow as pa
        pa_type = dtype.pyarrow_dtype
        return pa.types.is_integer(pa_type) or pa.types.is_floating(pa_type)

    return False


def _to_comparable_array(arr):
    """Convert nullable array to comparable numpy array"""
    if not _is_nullable_dtype(arr.dtype):
        return arr

    # For nullable numeric types, convert to float64 with NaN
    if _is_nullable_numeric_dtype(arr.dtype):
        return arr.to_numpy(dtype='float64', na_value=np.nan)

    # For other types, convert to object
    return arr.to_numpy(dtype='object', na_value=None)


def _na_safe_lt(a, b):
    """Less-than comparison that handles pd.NA"""
    if pd.isna(a) or pd.isna(b):
        return False

    try:
        result = a < b
        # Handle pd.NA propagation
        if isinstance(result, type(pd.NA)):
            return False
        return bool(result)
    except TypeError:
        return False


def _na_safe_le(a, b):
    """Less-than-or-equal comparison that handles pd.NA"""
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False

    try:
        result = a <= b
        if isinstance(result, type(pd.NA)):
            return False
        return bool(result)
    except TypeError:
        return False


def _na_safe_eq(a, b):
    """Equality comparison that handles pd.NA"""
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False

    try:
        result = a == b
        if isinstance(result, type(pd.NA)):
            return False
        return bool(result)
    except TypeError:
        return False
```

**Трудозатраты:** 8-10 часов

---

#### 6. Тесты

**Новые тестовые файлы:**

1. `dask/dataframe/tests/test_nullable_index.py` - тесты для nullable типов
2. `dask/dataframe/tests/test_datetime_null_index.py` - тесты для datetime с NaT
3. Обновить существующие тесты для совместимости

**Трудозатраты:** 30-40 часов

---

## 🎯 Метрики успеха

### Фаза 1 (Datetime поддержка)

**Критерии приемки:**
- ✅ `datetime64[ns]` с NaT создается без ошибок
- ✅ `timedelta64[ns]` с NaT создается без ошибок
- ✅ Filter, map_partitions работают корректно
- ✅ Join между DataFrame с datetime+NaT работает
- ✅ 95%+ покрытие тестами новой функциональности
- ✅ Нет регрессий в существующих тестах
- ✅ Документация обновлена

**KPI:**
- Поддержка типов: 9.5% → 20%
- Performance: не хуже ±5% от baseline

---

### Фаза 2 (Nullable типы)

**Критерии приемки:**
- ✅ Int64, Int32, Float64, Float32 (nullable) работают полностью
- ✅ PyArrow int64, float64, timestamp работают
- ✅ boolean (nullable) работает
- ✅ Все операции (filter, join, map_partitions, groupby) работают
- ✅ 95%+ покрытие тестами
- ✅ Benchmark показывает приемлемую производительность
- ✅ Обновлена документация с миграционным гидом

**KPI:**
- Поддержка типов: 20% → 50%
- Performance: не хуже ±10% от baseline для новых типов
- Performance: не хуже ±5% для существующих типов (float64)

---

## ⚠️ Риски и митигация

### Риск 1: Performance Degradation

**Описание:** Дополнительные проверки на nullable типы могут замедлить операции.

**Вероятность:** 🟡 Средняя

**Влияние:** 🟡 Среднее

**Митигация:**
- Добавить fast path для non-nullable типов (проверка в начале функции)
- Использовать caching для type checks
- Benchmarking на каждом этапе
- Оптимизация hot paths

```python
def set_partitions_pre(s, divisions, ...):
    # Fast path: если оба НЕ nullable - используем старую логику
    if not (_is_nullable_dtype(s.dtype) or _is_nullable_dtype(getattr(divisions, 'dtype', None))):
        # Original fast implementation
        return _set_partitions_fast(s, divisions, ...)

    # Slow path: nullable handling
    return _set_partitions_nullable(s, divisions, ...)
```

---

### Риск 2: Edge Cases и Bugs

**Описание:** Множество комбинаций типов и null значений → много edge cases.

**Вероятность:** 🔴 Высокая

**Влияние:** 🔴 Высокое

**Митигация:**
- **Comprehensive testing:**
  - Property-based testing (hypothesis)
  - Parametrized тесты для всех комбинаций типов
  - Stress testing с большими datasets
- **Поэтапный rollout:**
  - Фаза 1 → Фаза 2 → Фаза 3
  - Beta период с opt-in флагом
- **Documentation:**
  - Четкие примеры для каждого типа
  - Known limitations
  - Troubleshooting guide

---

### Риск 3: Breaking Changes

**Описание:** Изменения в поведении divisions могут сломать существующий код.

**Вероятность:** 🟢 Низкая (для Фазы 1-2)

**Влияние:** 🔴 Высокое

**Митигация:**
- **Backward compatibility:**
  - Существующие типы (float64) работают идентично
  - Новая функциональность - additive, не меняет существующую
- **Deprecation period:**
  - Если нужны breaking changes - использовать deprecation warnings
  - Минимум 2 релиза для deprecation
- **Versioning:**
  - Semantic versioning
  - Clear changelog
- **Testing:**
  - Запускать все существующие тесты
  - CI/CD с проверкой на регрессии

---

### Риск 4: Maintainability

**Описание:** Сложность кода увеличится, поддержка станет сложнее.

**Вероятность:** 🟡 Средняя

**Влияние:** 🟡 Среднее

**Митигация:**
- **Code organization:**
  - Выделить nullable логику в отдельные модули (_nullable_utils.py)
  - Clear separation of concerns
- **Documentation:**
  - Inline comments для сложной логики
  - Architecture decision records (ADRs)
  - Developer guide
- **Type hints:**
  - Добавить type hints во все новые функции
  - mypy проверки в CI
- **Code review:**
  - Mandatory review от core maintainers
  - Focus на читаемость и maintainability

---

## 📚 Требования к тестированию

### Unit тесты

**Фаза 1:**
- ✅ DataFrame creation с datetime64[ns] + NaT
- ✅ DataFrame creation с timedelta64[ns] + NaT
- ✅ Division validation с NaT
- ✅ Sorting divisions с NaT
- ✅ Edge cases: все NaT, один NaT, NaT в начале/середине/конце

**Фаза 2:**
- ✅ Каждый nullable тип (Int64, Int32, Int8, UInt64, UInt32, UInt8, Float64, Float32, boolean)
- ✅ PyArrow типы (int64, float64, timestamp, string если поддерживается)
- ✅ Division operations с pd.NA
- ✅ Comparison operations
- ✅ searchsorted с nullable types
- ✅ Edge cases для каждого типа

### Integration тесты

- ✅ from_pandas → filter → compute
- ✅ from_pandas → map_partitions → compute
- ✅ from_pandas → groupby → compute
- ✅ from_pandas → merge → compute
- ✅ from_pandas → join → compute
- ✅ from_pandas → sort_values → compute
- ✅ Многошаговые операции

### Performance тесты

```python
import pytest
import pandas as pd
import dask.dataframe as dd
import numpy as np

@pytest.mark.benchmark
def test_performance_datetime_null_index(benchmark):
    """Benchmark datetime index with nulls"""
    # Создаем большой DataFrame
    n = 1_000_000
    dates = pd.date_range('2020-01-01', periods=n, freq='1min')
    # Добавляем случайные NaT (5%)
    null_mask = np.random.random(n) < 0.05
    dates = dates.to_series()
    dates[null_mask] = pd.NaT

    pdf = pd.DataFrame({'value': np.random.randn(n)}, index=dates)

    def create_and_filter():
        ddf = dd.from_pandas(pdf, npartitions=100)
        return ddf[ddf['value'] > 0].compute()

    result = benchmark(create_and_filter)
    assert len(result) > 0

# Аналогично для других типов
```

### Property-based тесты

```python
from hypothesis import given, strategies as st
import pandas as pd
import dask.dataframe as dd

@given(
    values=st.lists(st.floats(allow_nan=True, allow_infinity=False), min_size=10, max_size=1000),
    npartitions=st.integers(min_value=1, max_value=10)
)
def test_property_float_index_with_nan(values, npartitions):
    """Property test: любой float index должен работать"""
    pdf = pd.DataFrame({'x': range(len(values))}, index=values)

    # Should not raise
    ddf = dd.from_pandas(pdf, npartitions=npartitions, sort=False)

    # Compute should work
    result = ddf.compute()
    assert len(result) == len(pdf)
```

---

## 📖 Документация

### 1. User Guide

**Новый раздел:** "Working with Missing Values in Index"

```markdown
# Working with Missing Values in Index

Dask DataFrame supports missing values (NaN, NaT, pd.NA) in indexes for certain data types.

## Supported Types

### Fully Supported
- `float64`, `float32` with `np.nan` ✅
- `datetime64[ns]` with `pd.NaT` ✅ (since version X.X.X)
- `timedelta64[ns]` with `pd.NaT` ✅ (since version X.X.X)
- `Int64`, `Float64` (nullable pandas types) ✅ (since version Y.Y.Y)
- PyArrow numeric types ✅ (since version Y.Y.Y)

### Not Supported
- `object`, `string` with `None` ❌
- `Categorical` with `None` ❌

## Examples

### Datetime Index with Missing Values

```python
import pandas as pd
import dask.dataframe as dd

# Create pandas DataFrame with datetime index containing NaT
dates = pd.to_datetime(['2024-01-01', '2024-01-02', None, '2024-01-04'])
df = pd.DataFrame({'value': [1, 2, 3, 4]}, index=dates)

# Create Dask DataFrame (works!)
ddf = dd.from_pandas(df, npartitions=2)

# Filter by date
result = ddf.loc['2024-01-01':'2024-01-02'].compute()

# Filter out missing dates
valid_dates = ddf[ddf.index.notna()].compute()
```

### Nullable Integer Index

```python
import pandas as pd
import dask.dataframe as dd

# Create index with missing values using nullable Int64
index = pd.array([1, 2, None, 4, 5], dtype="Int64")
df = pd.DataFrame({'value': list('abcde')}, index=index)

# Create Dask DataFrame
ddf = dd.from_pandas(df, npartitions=2)

# All operations work
filtered = ddf[ddf.index > 2].compute()
```

## Performance Considerations

- Operations with nullable types may be slightly slower than standard types
- For best performance with numeric data, use `float64` with `np.nan`
- Consider filtering out missing values if index operations are critical path

## Migration Guide

If you previously worked around the limitation using `reset_index()`:

```python
# Old workaround (still works)
df_reset = df.reset_index()
ddf = dd.from_pandas(df_reset, npartitions=2)

# New direct approach (simpler)
ddf = dd.from_pandas(df, npartitions=2)  # Works directly!
```
```

### 2. API Reference Updates

Обновить docstrings:

```python
def from_pandas(
    data,
    npartitions=None,
    chunksize=None,
    sort=True,
    name=None
):
    """
    Create a Dask DataFrame from a pandas DataFrame.

    Parameters
    ----------
    data : pandas.DataFrame or pandas.Series
        The pandas object to convert
    npartitions : int, optional
        Number of partitions
    chunksize : int, optional
        Rows per partition
    sort : bool, default True
        Whether to sort by index
    name : str, optional
        Name for the Dask object

    Returns
    -------
    dask.dataframe.DataFrame or dask.dataframe.Series

    Notes
    -----
    **Index with Missing Values:**

    Dask supports missing values in the index for the following types:

    - Numeric types: ``float64``, ``float32`` with ``np.nan``
    - Datetime types: ``datetime64[ns]``, ``timedelta64[ns]`` with ``pd.NaT``
    - Nullable types: ``Int64``, ``Float64``, etc. with ``pd.NA`` (since X.X.X)
    - PyArrow types: numeric and timestamp types (since X.X.X)

    For unsupported types (``object``, ``string``), use ``data.loc[~data.index.isna()]``
    or ``data.reset_index()`` before converting to Dask.

    Examples
    --------
    >>> import pandas as pd
    >>> import dask.dataframe as dd

    Create from pandas with datetime index containing NaT:

    >>> dates = pd.to_datetime(['2024-01-01', None, '2024-01-03'])
    >>> df = pd.DataFrame({'x': [1, 2, 3]}, index=dates)
    >>> ddf = dd.from_pandas(df, npartitions=2)
    """
```

### 3. Release Notes

```markdown
# Release X.X.X

## New Features

### Support for Missing Values in Datetime Index (#XXXX)

Dask DataFrame now supports `datetime64[ns]` and `timedelta64[ns]` indexes
containing missing values (`pd.NaT`).

```python
import pandas as pd
import dask.dataframe as dd

dates = pd.to_datetime(['2024-01-01', None, '2024-01-03'])
df = pd.DataFrame({'value': [1, 2, 3]}, index=dates)
ddf = dd.from_pandas(df, npartitions=2)  # Now works!
```

---

# Release Y.Y.Y

## New Features

### Support for Nullable and PyArrow Types in Index (#YYYY)

Extended support for missing values to pandas nullable types (Int64, Float64, boolean)
and PyArrow types.

```python
# Nullable Int64 index
index = pd.array([1, 2, None, 4], dtype="Int64")
df = pd.DataFrame({'x': list('abcd')}, index=index)
ddf = dd.from_pandas(df, npartitions=2)  # Works!

# PyArrow types
import pyarrow as pa
index = pd.array([1, None, 3], dtype=pd.ArrowDtype(pa.int64()))
df = pd.DataFrame({'x': list('abc')}, index=index)
ddf = dd.from_pandas(df, npartitions=2)  # Works!
```

## Performance

- Added fast-path optimization for non-nullable types (no performance regression)
- Nullable type operations are ~5-10% slower than standard float64 with NaN

## Breaking Changes

None - all changes are backward compatible.
```

---

## 💰 Оценка ROI

### Преимущества

**Для пользователей:**
- ✅ Более естественный API (не нужны workarounds)
- ✅ Лучшая совместимость с pandas
- ✅ Поддержка современных типов данных (PyArrow)
- ✅ Меньше data loss (не нужно удалять строки с null)

**Для экосистемы:**
- ✅ Улучшенная совместимость с pandas 2.x
- ✅ Подготовка к Arrow backend
- ✅ Alignment с другими distributed DataFrame библиотеками

### Затраты

| Компонент | Фаза 1 | Фаза 2 | Итого |
|-----------|--------|--------|-------|
| Разработка | 40-55 ч | 93-130 ч | 133-185 ч |
| Code review | 8-10 ч | 15-20 ч | 23-30 ч |
| QA/Testing | включено | включено | - |
| Документация | включено | включено | - |
| **Всего** | **1-1.5 недели** | **3-4 недели** | **4-5.5 недель** |

### ROI анализ

**Метрики:**
- Уменьшение github issues: ожидается -20% issues по теме "index with nulls"
- User satisfaction: улучшение на основе опросов
- Adoption rate: отслеживание использования новых типов

**Качественная польза:**
- Сокращение времени на поддержку workarounds в документации
- Меньше вопросов на Stack Overflow
- Улучшение репутации проекта

---

## 🎯 Рекомендации

### Краткосрочные (Немедленно)

1. **✅ Начать с Фазы 1** (datetime/timedelta поддержка)
   - Низкий риск
   - Высокая ценность для пользователей
   - Быстрая реализация (1-2 недели)

2. **📋 Создать GitHub Issue** для трекинга
   - Описать проблему
   - Запросить feedback от community
   - Собрать use cases

3. **👥 Обсудить с maintainers**
   - Получить buy-in на предложенный подход
   - Обсудить приоритизацию

### Среднесрочные (Следующие 1-2 месяца)

4. **✅ Имплементировать Фазу 2** (nullable types)
   - После успешного релиза Фазы 1
   - С comprehensive testing
   - С performance benchmarks

5. **📊 Собрать metrics**
   - Adoption rate новых типов
   - Performance impact
   - User feedback

### Долгосрочные (Обсуждение)

6. **❓ Оценить необходимость Фазы 3** (полная поддержка)
   - На основе user feedback
   - На основе cost/benefit анализа
   - Требует consensus от core team

7. **🔄 Рассмотреть альтернативы**
   - Hash-based partitioning для non-sortable типов?
   - Опциональные unknown divisions?
   - Integration с Arrow compute kernels?

---

## 📞 Следующие шаги

### Для contributor'а:

1. **Создать RFC (Request for Comments)** в Dask GitHub
2. Получить feedback от maintainers
3. Создать proof-of-concept PR для Фазы 1
4. Итерировать на основе review
5. После merge Фазы 1 - начать Фазу 2

### Для maintainer'а:

1. Review этого отчета
2. Обсудить на team meeting
3. Принять решение по приоритизации
4. Assign issue/PR reviewer'ов
5. Планирование релиза

### Для пользователя:

1. **Сейчас:** Использовать `reset_index()` workaround
2. **После Фазы 1:** Использовать datetime с NaT напрямую
3. **После Фазы 2:** Использовать nullable types
4. Предоставлять feedback через GitHub issues

---

## 📎 Приложения

### A. Код примеров

См. файлы:
- `test_all_types_null_index.py` - тесты всех типов
- `ОТЧЕТ_ТИПЫ_ДАННЫХ_С_NULL_В_ИНДЕКСАХ.md` - детальные результаты тестирования

### B. Benchmark результаты

(Будут добавлены после имплементации proof-of-concept)

### C. Ссылки

- [Pandas nullable dtypes documentation](https://pandas.pydata.org/docs/user_guide/integer_na.html)
- [PyArrow integration in pandas](https://pandas.pydata.org/docs/user_guide/pyarrow.html)
- [Dask DataFrame design docs](https://docs.dask.org/en/latest/dataframe-design.html)
- Related issues: (ссылки на существующие issues в Dask GitHub)

---

**Дата отчета:** 2025-11-09
**Автор:** Техническое исследование возможности имплементации
**Версия:** 1.0
