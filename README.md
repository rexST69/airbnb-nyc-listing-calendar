# airbnb-nyc-listing-calendar

An archived Apache Spark and Hadoop analysis of New York City short-term rental
pricing, occupancy, market segmentation, and modeled yield opportunities.

This README is the complete project record. The actual Scala sources remain in
[`scripts/`](scripts/) and the SBT build metadata remains in [`project/`](project/).
Large raw and generated datasets were kept outside Git in HDFS because of their
size.

## What was analyzed

- 19,406 validated active listings
- More than 14 million forward calendar rows
- Four market regimes identified with a Gaussian Mixture Model
- Forward occupancy estimated with a Gradient-Boosted Trees model
- Five simulated prices compared using expected RevPAR

## Repository layout

```text
.
├── README.md
├── LICENSE
├── .gitignore
├── build.sbt
├── project/
│   ├── build.properties
│   └── plugins.sbt
└── scripts/src/main/scala/
		├── ingestion/AirbnbDataIngestion.scala
		├── segmentation/MarketSegmentation.scala
		└── optimization/YieldOptimizer.scala
```

The repository intentionally does not contain committed raw CSV files,
generated Parquet stores, model artifacts, or cloud credentials. The source
data and outputs belonged to the following HDFS locations:

```text
/airbnb/raw/listings.csv.gz
/airbnb/raw/calendar.csv.gz
/airbnb/processed/listing_metrics
/airbnb/processed/segmented_listings
/airbnb/processed/yield_optimization_results
```

## Environment and architecture

The completed analysis ran as a single-node Hadoop/Spark workload on a Google
Cloud VM configured as a compact data lake.

| Setting | Recorded value |
| --- | --- |
| Instance | GCP Compute Engine e2-standard-4, 4 vCPUs, 16 GB memory |
| Zone | asia-south2-b |
| Operating environment | CentOS/RHEL Stream, OpenJDK 17.0.20.1 |
| Hadoop | 3.3.6 with NameNode, DataNode, and SecondaryNameNode |
| NameNode | `hdfs://127.0.0.1:9000` |
| Spark | 3.5.1 with Spark SQL and MLlib |
| Spark settings | 8 GB driver memory and 16 shuffle partitions |
| Build | SBT 1.9.8, Scala 2.12.18, sbt-assembly |

The data flow was:

```text
CSV.GZ inputs -> HDFS raw storage -> Spark ingestion -> Parquet metrics
																			-> GMM segmentation
																			-> GBT occupancy model
																			-> price simulation and yield results
```

## Data and ingestion

The listing extract contained property attributes, coordinates, amenity counts,
host and review metrics, and listed base prices. It was about 30 MB compressed
and 180 MB uncompressed. The calendar extract contained more than 14 million
listing/date rows for the next 365 days, with availability flags and date-level
price quotes. It was about 100 MB compressed and 1.1 GB uncompressed.

Price strings were stripped of currency symbols and commas before being cast to
numeric values. Listing identifiers and model fields were explicitly typed,
invalid identifiers and non-positive prices were removed, and calendar rows
were aggregated by listing.

Forward occupancy was defined as:

`forward occupancy = unavailable calendar days / tracked calendar days`

Only listings with at least 30 tracked calendar days were retained. Baseline
RevPAR was calculated from the listed base price and inferred occupancy, then
written as Snappy-compressed Parquet to `/airbnb/processed/listing_metrics`.

### Ingestion issues recorded

| Issue | Resolution |
| --- | --- |
| Cloudflare blocked VM downloads | Files were transferred through an authenticated browser session and SSH tunnel. |
| Albany file was selected | The incorrect 467-row extract was replaced with New York City data. |
| Spark could not resolve `hdfs:///...` | Reader paths used `hdfs://127.0.0.1:9000`. |
| A cast expression broke a join | The cast aggregation was projected and aliased as `listing_id`. |

## Analytical pipeline

### Ingestion and baseline metrics

[`AirbnbDataIngestion.scala`](scripts/src/main/scala/ingestion/AirbnbDataIngestion.scala)
read the raw listing and calendar files, cleaned prices, calculated inferred
occupancy and current RevPAR, joined the datasets, and wrote the listing metric
Parquet store.

### Market segmentation

[`MarketSegmentation.scala`](scripts/src/main/scala/segmentation/MarketSegmentation.scala)
standardized these eight numeric features:

- accommodates
- bedrooms
- beds
- listed price
- minimum nights
- number of reviews
- review score
- reviews per month

A four-component Gaussian Mixture Model was trained for 30 EM iterations with
seed 42. The resulting descriptive regimes were:

| Regime | Listings | Average price | Occupancy | RevPAR |
| --- | ---: | ---: | ---: | ---: |
| Luxury Escapes | 615 | $929.66 | 31.1% | $258.58 |
| Mass-Market Core Urban | 12,723 | $232.50 | 33.3% | $74.88 |
| Premium Executive | 1,404 | $709.20 | 26.3% | $165.83 |
| Residential Extended | 4,664 | $320.24 | 24.0% | $80.72 |

The segmented output was written to `/airbnb/processed/segmented_listings`.

### Demand modeling and price simulation

[`YieldOptimizer.scala`](scripts/src/main/scala/optimization/YieldOptimizer.scala)
compared each listing price with its neighborhood median and used that ratio as
a model feature. A distributed `GBTRegressor` used `maxDepth = 5` and
`maxIter = 30` to estimate forward occupancy from listing size, review
activity, room type, market regime, and local price position.

Each listing was rescored at 70%, 85%, 100%, 115%, and 130% of its current
price. The selected price maximized:

`expected RevPAR = simulated price * predicted occupancy`

The selected price was classified relative to the current price as:

| Category | Rule |
| --- | --- |
| Underpriced yield opportunity | Optimal price at least 115% of current price |
| Overpriced vacancy trap | Optimal price at most 85% of current price |
| Price-optimized equilibrium | Optimal price within 15% of current price |

The final output was written to `/airbnb/processed/yield_optimization_results`.

## Findings

### Portfolio impact

| Category | Listings | Current price | Optimal price | Current RevPAR | Optimal RevPAR | Gain |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Underpriced yield opportunity | 16,841 | $323.12 | $417.61 | $92.99 | $119.28 | +$26.29 |
| Price-optimized equilibrium | 2,005 | $194.46 | $199.48 | $64.66 | $70.17 | +$5.50 |
| Overpriced vacancy trap | 560 | $334.99 | $255.77 | $45.35 | $65.30 | +$19.96 |

About 87% of validated active inventory was classified as underpriced. The
average gain multiplied across that group represented approximately $442,700 in
additional modeled revenue per day. Among the 11,075 listings with established
booking history, the average modeled uplift was 54.3%.

### Neighborhoods with the most headroom

| Neighborhood | Listings | Current price | Optimal price | RevPAR gain |
| --- | ---: | ---: | ---: | ---: |
| Theater District | 240 | $662.24 | $858.04 | +$91.44 |
| Tribeca | 65 | $1,358.63 | $1,755.19 | +$82.18 |
| Midtown | 1,234 | $641.61 | $828.67 | +$62.50 |
| Financial District | 301 | $806.73 | $1,043.45 | +$56.35 |
| Gramercy | 85 | $544.11 | $700.63 | +$52.12 |
| Lower East Side | 294 | $545.51 | $705.74 | +$50.99 |
| Arverne | 57 | $258.41 | $333.41 | +$44.43 |
| Long Island City | 222 | $443.80 | $571.47 | +$43.25 |
| Ozone Park | 56 | $198.39 | $252.82 | +$42.88 |
| Brownsville | 56 | $212.12 | $273.37 | +$41.06 |

The largest modeled gains appeared in Manhattan tourist and business areas.
Long Island City and Ozone Park also showed modeled headroom among outer-borough
transit hubs.

### Room type results

| Room type | Category | Listings | Current price | Optimal price | Gain |
| --- | --- | ---: | ---: | ---: | ---: |
| Entire home/apt | Underpriced | 8,936 | $343.67 | $444.37 | +$28.41 |
| Entire home/apt | Equilibrium | 958 | $249.23 | $258.93 | +$4.83 |
| Entire home/apt | Overpriced | 217 | $512.42 | $380.36 | +$22.23 |
| Private room | Underpriced | 7,440 | $264.72 | $342.28 | +$21.65 |
| Private room | Equilibrium | 992 | $125.93 | $128.06 | +$5.04 |
| Private room | Overpriced | 279 | $118.58 | $92.92 | +$8.17 |
| Hotel room | Underpriced | 326 | $1,150.30 | $1,477.92 | +$78.82 |
| Shared room | Underpriced | 139 | $187.17 | $242.29 | +$15.62 |

The room-type output contained only the combinations shown above, so those
counts do not sum to the full portfolio totals. Entire homes represented the
largest underpriced group; hotel rooms had the largest per-listing modeled gain.

## Definitions

| Term | Definition in this project |
| --- | --- |
| Active listing | A listing that passed identifier, price, and calendar coverage filters. |
| Forward occupancy | The share of tracked calendar dates marked unavailable. |
| RevPAR | Listed or simulated price multiplied by occupancy. |
| Baseline RevPAR | Current listed base price multiplied by inferred occupancy. |
| Expected RevPAR | Simulated price multiplied by predicted occupancy. |
| Price ratio | Listing price divided by its neighborhood median listed price. |
| GMM | Gaussian Mixture Model used to assign four market regimes. |
| GBTRegressor | Spark MLlib Gradient-Boosted Trees regressor used to estimate occupancy. |
| Underpriced yield opportunity | Selected simulated price at least 115% of current price. |
| Overpriced vacancy trap | Selected simulated price at most 85% of current price. |
| Price-optimized equilibrium | Selected simulated price within 15% of current price. |

## Limitations and interpretation

- Occupancy was inferred from unavailable calendar days. The calendar did not
	distinguish confirmed bookings from dates blocked by a host, so occupancy and
	baseline RevPAR may be overstated.
- Baseline RevPAR used the current listed base price rather than date-level
	calendar price quotes.
- Modeled gains came from cross-sectional differences between listings, not
	controlled price experiments. They are not observed causal impacts.
- Only five prices between 70% and 130% of the current rate were evaluated. A
	true optimum may lie outside that grid.
- The analysis covered one New York City snapshot and may not transfer to other
	markets or seasons.

The results indicated substantial modeled pricing headroom in active inventory,
particularly in high-demand Manhattan neighborhoods and among entire-home
listings. They also indicated a separate population where a lower price was
associated with higher expected RevPAR because the modeled occupancy response
outweighed the lower nightly rate.

Possible follow-up directions identified in the analysis included automated
revenue management, vacancy alerts near check-in, and live testing of
neighborhood rate changes. These were interpretations of the model output, not
validated operational outcomes.

## Build record

The Scala application is defined by [`build.sbt`](build.sbt), uses Scala
2.12.18, and declares Spark 3.5.1 as a provided dependency because the original
cluster supplied the Spark runtime. SBT metadata and the assembly plugin are in
[`project/`](project/).

Raw data, generated Parquet, packaged JARs, credentials, and the original DOCX
artifact are excluded by [`.gitignore`](.gitignore).