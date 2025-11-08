# Исследование: Строковые индексы с null-значениями в Dask DataFrame

## 📋 Резюме

Данное исследование изучает поведение Dask DataFrame при работе с индексами на колонках с типом "строка", содержащих пустые значения (NaN, NA, None).

## 🔍 Основное ограничение

**Dask НЕ поддерживает создание DataFrame с non-numeric индексом, содержащим null значения.**

```python
# ❌ Это вызовет NotImplementedError
pdf = pd.DataFrame({'value': [1, 2, 3]}, index=['a', None, 'c'])
ddf = dd.from_pandas(pdf, npartitions=2)
# NotImplementedError: Index in passed data is non-numeric and contains nulls,
# which Dask does not entirely support.
```

## ✅ Результаты тестирования

### 1. Числовые индексы с null

✅ **Работают!** Dask поддерживает числовые индексы с null значениями:

```python
# ✓ Это работает
pdf = pd.DataFrame({'value': [1, 2, 3]}, index=[1.0, np.nan, 3.0])
ddf = dd.from_pandas(pdf, npartitions=2)  # Успешно
```

**Результаты теста:**
- Партиций: 5
- Known divisions: True
- Filter, map_partitions работают корректно

### 2. Строковые индексы с null

❌ **Не работают** напрямую, требуют обходных путей.

## 🛠️ Обходные пути

### Метод 1: Фильтрация null значений (Простой)

```python
# Удаляем строки с null индексом
pdf = pd.DataFrame(data, index=index_with_nulls)
pdf_filtered = pdf.loc[~pdf.index.isna()]
ddf = dd.from_pandas(pdf_filtered, npartitions=5)
```

**Преимущества:**
- ✅ Простота реализации
- ✅ Гарантированная работоспособность

**Недостатки:**
- ❌ Потеря данных (строки с null удаляются)

**Результаты теста:**
- Исходных строк: 25
- После фильтрации: 18
- Удалено: 7 строк
- Filter, GroupBy работают корректно

### Метод 2: Замена null на строковое значение

```python
# Заменяем null на специальную строку
pdf = pd.DataFrame(data, index=index_with_nulls)
pdf.index = pdf.index.fillna('__NULL__')
ddf = dd.from_pandas(pdf, npartitions=5)

# Позже можно найти эти строки
null_rows = ddf[ddf.index == '__NULL__']
```

**Преимущества:**
- ✅ Сохраняет все данные
- ✅ Можно идентифицировать null строки

**Недостатки:**
- ❌ Нужно отслеживать специальные значения
- ❌ Возможны коллизии имен
- ⚠️ Восстановление null через map_partitions может быть проблематичным

**Результаты теста:**
- Null значений заменено: 7
- Поиск '__NULL__' индексов работает: ✅

### Метод 3: Reset index и работа с колонкой (Рекомендуется)

```python
# Преобразуем индекс в колонку
pdf = pd.DataFrame(data, index=index_with_nulls)
pdf = pdf.reset_index()
pdf.columns = ['original_index'] + list(pdf.columns[1:])
ddf = dd.from_pandas(pdf, npartitions=5)

# Все операции доступны
filtered = ddf[ddf['original_index'].isna()]  # Фильтрация
joined = ddf.merge(other, on='original_index')  # Join
processed = ddf.map_partitions(custom_func)  # Обработка

# При необходимости восстанавливаем индекс
ddf_indexed = ddf.set_index('original_index', sorted=False)
```

**Преимущества:**
- ✅ Сохраняет все данные
- ✅ Полный контроль над null значениями
- ✅ Все операции работают корректно
- ✅ Можно восстановить индекс

**Недостатки:**
- ⚠️ Требует дополнительного шага

**Результаты теста:**
- Null в колонке: 7
- Фильтрация null: работает ✅
- Join по колонке с null: работает ✅ (16 строк при inner join)
- map_partitions: работает ✅

## 📊 Тестирование операций

### JOIN операции

**Тест:** Inner join двух DataFrame с null в ключевых колонках

```python
# DataFrame 1: 20 строк, 5 null
# DataFrame 2: 15 строк, 5 null
joined = ddf1.merge(ddf2, on='product_id', how='left')
```

**Результат:**
- ✅ Left join: 40 строк
- Строк с совпадением: 33
- Строк без совпадения: 7
- ⚠️ **Важно:** null значения НЕ матчатся между собой (стандартное SQL поведение)

**Наблюдения:**
- Inner join с null в индексе показал 16 строк (все null совпали)
- Это означает, что pandas/dask считают все null равными при join

### FILTER операции

**Тест:** Фильтрация по категории на DataFrame с null индексом (через reset_index)

```python
filtered = ddf[ddf['category'] == 'A']
null_indices = ddf[ddf['original_index'].isna()]
```

**Результат:**
- ✅ Filter по колонкам: работает корректно
- ✅ Filter по наличию null: работает с `.isna()`
- ✅ Найдено 7 строк с null индексом

### MAP_PARTITIONS операции

**Тест 1:** Добавление колонок

```python
def process_partition(df):
    result = df.copy()
    result['index_is_null'] = df['original_index'].isna()
    result['partition_size'] = len(df)
    return result

mapped = ddf.map_partitions(process_partition)
```

**Результат:**
- ✅ Успешно выполнено
- Строк с null индексом: 7

**Тест 2:** Статистика по партициям

```python
def partition_stats(df):
    return pd.Series({
        'total': len(df),
        'nulls_in_index': df['original_index'].isna().sum()
    })

stats = ddf.map_partitions(partition_stats, meta=pd.Series(dtype=object))
```

**Результат:**
- ✅ Статистика собрана по 10 партициям (после join)
- map_partitions полностью работоспособен с null

## 📈 Практический пример

**Сценарий:** Анализ продаж с потенциально отсутствующими идентификаторами

```python
# 1. Создание данных
df1 = pd.DataFrame({
    'product_id': ['p1', None, 'p3', None, 'p5'],  # Есть null
    'sales': [100, 200, 300, 400, 500]
}).reset_index(drop=True)

df2 = pd.DataFrame({
    'product_id': ['p1', 'p3', None, 'p7'],  # Есть null
    'price': [10, 20, 30, 40]
}).reset_index(drop=True)

ddf1 = dd.from_pandas(df1, npartitions=2)
ddf2 = dd.from_pandas(df2, npartitions=2)

# 2. Join
joined = ddf1.merge(ddf2, on='product_id', how='left')

# 3. Filter: Только строки с известным product_id
filtered = joined[joined['product_id'].notnull() & joined['price'].notnull()]

# 4. map_partitions: Расчет выручки
def calc_revenue(df):
    df['revenue'] = df['sales'] * df['price']
    return df

result = filtered.map_partitions(calc_revenue).compute()
```

## 📝 Рекомендации

### DO ✅

1. **Используйте reset_index()** для работы с индексами, содержащими null
2. **Храните исходный индекс** в колонке для возможного восстановления
3. **Документируйте семантику null** в вашем коде
4. **Используйте явные проверки** `.isna()` / `.notnull()`
5. **Тестируйте поведение join** с null значениями для вашего случая

### DON'T ❌

1. **Не полагайтесь на неявное поведение** с null
2. **Не используйте операторы сравнения** напрямую с null (используйте `.isna()`)
3. **Избегайте смешивания типов null** (None, np.nan, pd.NA)
4. **Не создавайте Dask DataFrame** с non-numeric индексом, содержащим null

## 🎯 Типовой паттерн использования

```python
# 1. Исходные данные с null в строковом индексе
pdf = pd.DataFrame(
    {'value': [1, 2, 3, 4, 5]},
    index=['a', None, 'c', np.nan, 'e']
)

# 2. Преобразуем индекс в колонку
pdf = pdf.reset_index()
pdf.columns = ['original_index', 'value']

# 3. Создаем Dask DataFrame
ddf = dd.from_pandas(pdf, npartitions=2)

# 4. Работаем с данными
# Фильтрация строк с известным индексом
valid = ddf[ddf['original_index'].notnull()]

# Join с другим DataFrame
joined = ddf.merge(other_df, on='original_index', how='left')

# Обработка через map_partitions
def process(df):
    df['idx_type'] = df['original_index'].apply(
        lambda x: 'null' if pd.isna(x) else 'valid'
    )
    return df

processed = ddf.map_partitions(process)

# 5. При необходимости восстанавливаем индекс
# Только после фильтрации null значений!
final = valid.set_index('original_index', sorted=False)
```

## 📂 Файлы исследования

1. `test_string_index_nulls.py` - Первоначальный тест, демонстрирующий ограничение
2. `test_string_index_nulls_enhanced.py` - Расширенное тестирование с обходными путями
3. `ИССЛЕДОВАНИЕ_СТРОКОВЫЕ_ИНДЕКСЫ_NULL.md` - Данный отчет

## 🔗 Полезные ссылки

- Dask DataFrame документация: https://docs.dask.org/en/latest/dataframe.html
- Pandas missing data: https://pandas.pydata.org/docs/user_guide/missing_data.html
- Dask GitHub (issues): https://github.com/dask/dask/issues

## 💡 Выводы

1. **Dask имеет явное ограничение** на non-numeric индексы с null значениями
2. **Лучший обходной путь** - использовать `reset_index()` и работать с индексом как с колонкой
3. **Числовые индексы с null работают** без проблем
4. **Все операции** (join, filter, map_partitions) работают корректно при использовании рекомендованного подхода
5. **Null значения в join** имеют специфическое поведение - в Pandas они матчатся, в SQL - нет

---

**Дата исследования:** 2025-11-08
**Версия Dask:** 2025.11.0
**Версия Pandas:** 2.2.x
**Python:** 3.11.14
