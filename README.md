# goit-de-hw-03 — Apache Spark: обробка даних

Скрипт [main.py](main.py) працює з трьома CSV-файлами з папки `data/` (users, purchases, products):

1. Завантажує кожен файл в окремий DataFrame — `p1_*`
2. Видаляє рядки з пропущеними значеннями (`dropna`) — `p2_*`
3. Рахує загальну суму покупок (`quantity * price`) за кожною категорією — `p3_*`
4. Рахує суму покупок за категоріями для віку від 18 до 25 включно — `p4_*`
5. Рахує частку (%) кожної категорії від усіх витрат людей віком 18–25 — `p5_*`
6. Виводить 3 категорії з найбільшою часткою витрат у віці 18–25 — `p6_*`

Скриншоти `df.show()` лежать у папці [screenshots/](screenshots/).

## Результати

| Категорія | Усього | Вік 18–25 | Частка 18–25, % |
|---|---|---|---|
| Home | 1438.9 | 361.1 | 29.90 |
| Sports | 1755.5 | 310.5 | 25.71 |
| Electronics | 1141.9 | 249.6 | 20.67 |
| Clothing | 696.1 | 245.0 | 20.29 |
| Beauty | 441.7 | 41.4 | 3.43 |

**Топ-3 категорії (18–25):** Home, Sports, Electronics.

## Запуск

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
export JAVA_HOME=/opt/homebrew/opt/openjdk@17   # потрібна Java 17+
.venv/bin/python main.py
```
