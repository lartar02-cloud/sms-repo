import requests
import pandas as pd
from datetime import datetime

# Дата для отчёта — сегодня
DATE = datetime.now().strftime("%Y-%m-%d")

# Ссылки и клиенты (как в твоём bat-файле)
URL1 = "http://10.96.0.13:8000/internal/services/api/v1/report/records"
URL2 = "http://10.96.0.14:8000/internal/services/api/v1/report/records"

CLIENTS1 = [343, 344, 316, 313, 315, 317, 312, 326, 329, 327, 342, 325, 335, 314, 345, 330, 332, 331, 340, 319, 346, 347, 348]
CLIENTS2 = [402, 403, 404]

# Функция для получения данных с одного сервера
def fetch_data(url, clients):
    payload = {
        "client_list": clients,
        "start_date": f"{DATE} 00:00:00",
        "end_date": f"{DATE} 23:59:59"
    }
    response = requests.post(url, json=payload)
    response.raise_for_status()  # Если ошибка — сразу покажет
    return response.json()

# Получаем данные
print("Загружаем данные с первого сервера...")
data1 = fetch_data(URL1, CLIENTS1)

print("Загружаем данные со второго сервера...")
data2 = fetch_data(URL2, CLIENTS2)

# Объединяем в один список и делаем таблицу
all_records = data1 + data2
df = pd.DataFrame(all_records)

# Читаем справочники из папки data
print("Читаем справочники...")
df_akk = pd.read_csv('data/sprAkK.csv', encoding='utf-8')
df_shl = pd.read_csv('data/sprShl.csv', encoding='utf-8')
df_bpdn = pd.read_csv('data/sprBPDN.csv', encoding='utf-8')

# Соединяем (merge) как в Power Query
df = df.merge(df_akk, left_on='customer_id', right_on='id', how='left')
df = df.merge(df_shl, on='gw_line', how='left')
df = df.merge(df_bpdn, left_on='operator_name', right_on='name', how='left')

# Пока просто сохраняем полную таблицу — чтобы увидеть, что данные пришли
df.to_csv(f"full_data_{DATE}.csv", index=False, encoding='utf-8-sig')

# Простая сводная по аккаунтам и статусам (замени 'status' на реальное имя колонки со статусом!)
# Если колонка со статусом называется не 'status' — измени ниже
pivot = pd.pivot_table(
    df,
    index='account_name',          # или какое там реальное имя колонки с названием аккаунта
    columns='status',              # ← ← ← важное место — имя колонки со статусами (queued, transmitted и т.д.)
    aggfunc='size',
    fill_value=0
)

pivot['назвлено'] = pivot.sum(axis=1)

# Сохраняем сводку
pivot.to_csv(f"summary_{DATE}.csv", encoding='utf-8-sig')

# Простой markdown-отчёт
with open("REPORT.md", "w", encoding="utf-8") as f:
    f.write(f"# Отчёт SMS за {DATE}\n\n")
    f.write("## Сводная таблица\n\n")
    f.write(pivot.to_markdown())
    f.write("\n\nДанные успешно загружены и обработаны.")

print("Готово! Файлы сохранены.")
