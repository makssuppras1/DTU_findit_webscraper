# Thesis metadata (combined)

Files: `thesis_meta_combined.csv` (semicolon-separated) and `thesis_meta_combined.parquet`.

## Load the data

```python
import pandas as pd

# Parquet (recommended)
df = pd.read_parquet("Thesis_meta/thesis_meta_combined.parquet")

# Or CSV
df = pd.read_csv("Thesis_meta/thesis_meta_combined.csv", sep=";")
```

## Example: titles and authors by year

```python
subset = df[["Title", "Author", "Publication Year"]].dropna(subset=["Publication Year"])
subset["Publication Year"] = subset["Publication Year"].astype(int)
print(subset.head(10))
```

## Example: filter by keyword

```python
# Rows where keywords contain "machine learning"
ml = df[df["keywords_ts"].str.contains("machine learning", case=False, na=False)]
print(ml[["Title", "Author", "Publication Year"]])
```

## Example: count theses per year

```python
years = df["Publication Year"].dropna().astype(int)
print(years.value_counts().sort_index().tail(20))
```

## Main columns

| Column | Description |
|--------|-------------|
| Title | Thesis title |
| Author | Author name(s), pipe-separated if multiple |
| Publication Year | Year |
| abstract_ts | Abstract text |
| keywords_ts | Keywords, pipe-separated |
| Affiliations | Institution(s) |
| ID | Record ID |
