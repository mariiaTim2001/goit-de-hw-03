import io
import os
from contextlib import redirect_stdout

from PIL import Image, ImageDraw, ImageFont
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = "data"
SCREENSHOTS_DIR = "screenshots"
FONT_PATH = "/System/Library/Fonts/Menlo.ttc"


def save_screenshot(text: str, filename: str) -> None:
    """Render console text to a PNG image."""
    font = ImageFont.truetype(FONT_PATH, 16)
    lines = text.rstrip("\n").split("\n")
    line_h = 22
    width = max(int(font.getlength(line)) for line in lines) + 40
    height = line_h * len(lines) + 40
    img = Image.new("RGB", (width, height), (30, 30, 30))
    draw = ImageDraw.Draw(img)
    for i, line in enumerate(lines):
        color = (120, 200, 255) if line.startswith(">>>") else (230, 230, 230)
        draw.text((20, 20 + i * line_h), line, font=font, fill=color)
    img.save(os.path.join(SCREENSHOTS_DIR, filename))


def show(df, title: str, filename: str, n: int = 20) -> None:
    """Print df.show() to the console and save it as a screenshot."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        print(f">>> {title}")
        df.show(n)
    output = buf.getvalue()
    print(output)
    save_screenshot(output, filename)


def main() -> None:
    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("goit-de-hw-03")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Завантаження CSV-файлів в окремі DataFrame
    users_df = spark.read.csv(f"{DATA_DIR}/users.csv", header=True, inferSchema=True)
    purchases_df = spark.read.csv(f"{DATA_DIR}/purchases.csv", header=True, inferSchema=True)
    products_df = spark.read.csv(f"{DATA_DIR}/products.csv", header=True, inferSchema=True)

    show(users_df, f"users_df (rows: {users_df.count()})", "p1_users.png", 10)
    show(purchases_df, f"purchases_df (rows: {purchases_df.count()})", "p1_purchases.png", 10)
    show(products_df, f"products_df (rows: {products_df.count()})", "p1_products.png", 10)

    # 2. Очищення даних: видалення рядків з пропущеними значеннями
    users_clean = users_df.dropna()
    purchases_clean = purchases_df.dropna()
    products_clean = products_df.dropna()

    show(users_clean,
         f"users_df.dropna() (rows: {users_df.count()} -> {users_clean.count()})",
         "p2_users_clean.png", 10)
    show(purchases_clean,
         f"purchases_df.dropna() (rows: {purchases_df.count()} -> {purchases_clean.count()})",
         "p2_purchases_clean.png", 10)
    show(products_clean,
         f"products_df.dropna() (rows: {products_df.count()} -> {products_clean.count()})",
         "p2_products_clean.png", 10)

    # Об'єднана таблиця покупок з вартістю кожної покупки
    purchases_full = (
        purchases_clean
        .join(products_clean, on="product_id", how="inner")
        .join(users_clean, on="user_id", how="inner")
        .withColumn("purchase_amount", F.col("quantity") * F.col("price"))
    )

    # 3. Загальна сума покупок за кожною категорією продуктів
    total_by_category = (
        purchases_full
        .groupBy("category")
        .agg(F.round(F.sum("purchase_amount"), 2).alias("total_amount"))
        .orderBy(F.desc("total_amount"))
    )
    show(total_by_category, "Total purchase amount by category", "p3_total_by_category.png")

    # 4. Сума покупок за категоріями для віку 18-25 включно
    young_purchases = purchases_full.filter(F.col("age").between(18, 25))
    young_by_category = (
        young_purchases
        .groupBy("category")
        .agg(F.round(F.sum("purchase_amount"), 2).alias("total_amount_18_25"))
        .orderBy(F.desc("total_amount_18_25"))
    )
    show(young_by_category, "Purchase amount by category, age 18-25",
         "p4_by_category_age_18_25.png")

    # 5. Частка покупок за категоріями від сумарних витрат (вік 18-25)
    young_total = young_by_category.agg(F.sum("total_amount_18_25")).collect()[0][0]
    young_share = (
        young_by_category
        .withColumn("percentage",
                    F.round(F.col("total_amount_18_25") / F.lit(young_total) * 100, 2))
        .orderBy(F.desc("percentage"))
    )
    show(young_share, f"Share of spending by category, age 18-25 (total: {young_total:.2f})",
         "p5_share_by_category_age_18_25.png")

    # 6. Топ-3 категорії з найвищим відсотком витрат (вік 18-25)
    top3 = young_share.limit(3)
    show(top3, "Top 3 categories by share of spending, age 18-25",
         "p6_top3_categories_age_18_25.png")

    spark.stop()


if __name__ == "__main__":
    main()
