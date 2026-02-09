import pandas as pd
import matplotlib.pyplot as plt
import os

# CSV path and output folder
CSV_PATH = '/opt/airflow/store_files/raw_store_transactions.csv'
OUTPUT_DIR = '/opt/airflow/store_files/charts'


os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Loading data...")
df = pd.read_csv(CSV_PATH)
print(f"Total records: {len(df)}")

for col in ['MRP', 'CP', 'DISCOUNT', 'SP']:
    df[col] = df[col].replace(r'[\$,]', '', regex=True).astype(float)

# Sales by category
print("\n1. Generating chart: Sales by Category...")
sales_category = df.groupby('PRODUCT_CATEGORY')['SP'].sum().sort_values(ascending=False)
plt.figure(figsize=(10, 6))
sales_category.plot(kind='bar', color='steelblue')
plt.title('Sales by Product Category')
plt.xlabel('Category')
plt.ylabel('Total Sales (SP)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig(f'{OUTPUT_DIR}/01_sales_by_category.png')
plt.close()
print("   Saved: 01_sales_by_category.png")

# Sales by store
print("\n2. Generating chart: Sales by Store...")
sales_store = df.groupby('STORE_LOCATION')['SP'].sum().sort_values(ascending=False)
plt.figure(figsize=(10, 6))
sales_store.plot(kind='bar', color='darkorange')
plt.title('Sales by Store Location')
plt.xlabel('Location')
plt.ylabel('Total Sales (SP)')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig(f'{OUTPUT_DIR}/02_sales_by_store.png')
plt.close()
print("   Saved: 02_sales_by_store.png")

# Discount distribution
print("\n3. Generating chart: Discount Distribution...")
plt.figure(figsize=(10, 6))
plt.hist(df['DISCOUNT'], bins=20, color='green', edgecolor='black', alpha=0.7)
plt.title('Discount Distribution')
plt.xlabel('Discount')
plt.ylabel('Frequency')
plt.tight_layout()
plt.savefig(f'{OUTPUT_DIR}/03_discount_distribution.png')
plt.close()
print("   Saved: 03_discount_distribution.png")

# Profit margin by category
print("\n4. Generating chart: Profit Margin by Category...")
df['MARGIN'] = df['SP'] - df['CP']
margin_category = df.groupby('PRODUCT_CATEGORY')['MARGIN'].mean().sort_values(ascending=False)
plt.figure(figsize=(10, 6))
colors = ['green' if x > 0 else 'red' for x in margin_category]
margin_category.plot(kind='bar', color=colors)
plt.title('Average Profit Margin by Category (SP - CP)')
plt.xlabel('Category')
plt.ylabel('Average Margin')
plt.xticks(rotation=45, ha='right')
plt.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
plt.tight_layout()
plt.savefig(f'{OUTPUT_DIR}/04_profit_margin_by_category.png')
plt.close()
print("   Saved: 04_profit_margin_by_category.png")

print(f"\n✓ Analysis complete! Charts saved to: {OUTPUT_DIR}")
