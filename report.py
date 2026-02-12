import requests
import pandas as pd
from datetime import datetime

# --- Дата отчёта ---
DATE = datetime.now().strftime("%Y-%m-%d")

# --- Два запроса к серверам ---
def get_data(url, clients):
    payload = {
        "client_list": clients,
        "start_date": f"{DATE} 00:00:00",
        "end_date": f"{DATE} 23:59:59"
    }
    resp = requests.post(url, json=payload)
    return resp.json()

data1 = get_data("http://10.96.0.13:8000/internal/services/api/v1/report/records",
                 [343, 344, 316, 313, 315, 317, 312, 326, 329, 327, 342, 325, 335, 314, 345, 330, 332, 331, 340, 319, 346, 347, 348 402, 403, 404])

# --- Объединяем сырые данные ---
all_data = data1 + data2
df_raw = pd.DataFrame(all_data)

# --- Читаем справочники ---
df_akc = pd.read_csv('data/sprAkK.csv', encoding='utf-8')
df_shl = pd.read_csv('data/sprShl.csv', encoding='utf-8')
df_opr = pd.read_csv('data/sprBPDN.csv', encoding='utf-8')

# --- Сливаем по ключам ---
df = (df_raw
    .merge(df_akc, left_on='customer_id', right_on='id', how='left')
    .merge(df_shl, on='gw_line', how='left')
    .merge(df_opr, left_on='operator_name', right_on='name', how='left'))

# --- Сводная: считаем по статусам ---
# Предполагаю, что статусы в колонке 'status' — замени, если по-другому
df = df['status'].str.lower()  # чтобы не зависело от регистра

pivot = df.groupby('account_name') .value_counts().unstack(fill_value=0)

# Переименовываем статусы под твои названия
pivot = pivot.rename(columns={
    'queued': 'queued',
    'transmitted': 'transmitted',
    'forwarded': 'доставлено',
    'sent': 'sent',
    'in_processing': 'in processing',
    'blocked': 'заблокировано',
    'accepted': 'accepted',
    'error': 'ошибки',
    'forbidden': 'forbidden'
})

# Итоговая колонка
pivot = pivot.sum(axis=1)

# Проценты
pivot = (pivot / pivot['назвлено'] * 100).round(1)
pivot = (pivot / pivot * 100).round(1)

# Порядок колонок как у тебя
cols = ['назвлено', 'доставлено', 'доставлено_%', 'ошибки', 'ошибки_%',
        'sent', 'in processing', 'заблокировано', 'accepted', 'queued', 'transmitted', 'forbidden']
pivot = pivot # --- Сохраняем ---
pivot.to_csv(f"daily_report_{DATE}.csv", encoding="utf-8-sig")

# Markdown-отчёт
with open("REPORT.md", "w", encoding="utf-8") as f:
    f.write(f"# Отчёт за {DATE}\n\n")
    f.write(pivot.to_markdown())
    f.write("\n\n### Общий итог")
    total = pivot.sum()
    total = (total / total * 100).round(1)
    total = (total / total * 100).round(1)
    f.write(total .to_markdown())

print("Готово. Файлы: daily_report.csv и REPORT.md")