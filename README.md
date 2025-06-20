# Annuity Risk & Payout Forecasting Pipeline

A comprehensive data pipeline for modeling annuity risk, projecting payouts, and analyzing profitability for insurance companies like Venerable Holdings.

## 🎯 Project Overview
#main
This project simulates annuity contract lifecycles to help insurers:
- **Forecast cash flows** from annuity payouts
- **Model risk exposure** to longevity and market volatility  
- **Calculate profitability** per policy and across portfolios
- **Optimize reserve planning** for future liabilities

## 🏗️ Architecture

```
Data Sources → ETL (PySpark) → Redshift → Forecasting Engine → BI Dashboard
```

### Tech Stack
- **Storage**: Amazon Redshift (data warehouse)
- **Processing**: PySpark (distributed computing)
- **ETL**: AWS Glue (data pipeline orchestration)
- **Visualization**: Jupyter Notebooks + Plotly
- **Languages**: Python, SQL

## 📊 Data Model

### 1. Annuity Policies (`annuity_policies.csv`)
Simulates individual annuity contracts with realistic distributions:

| Column | Description | Example |
|--------|-------------|---------|
| `policy_id` | Unique policy identifier | P00001 |
| `age` | Policyholder age at issue | 65 |
| `gender` | M or F | M |
| `start_date` | Contract start date | 2015-06-01 |
| `deferral_years` | Years before payouts begin | 10 |
| `initial_value` | Initial investment amount | $250,000 |
| `rider_type` | GMWB, GMIB, or None | GMWB |
| `status` | deferred or payout | deferred |

### 2. Market Returns (`market_returns.csv`)
Annual market performance for account value growth:

| Column | Description | Example |
|--------|-------------|---------|
| `year` | Calendar year | 2025 |
| `return_pct` | Annual return percentage | 6.2 |

### 3. Mortality Rates (`mortality_rates.csv`)
Age and gender-specific death probabilities:

| Column | Description | Example |
|--------|-------------|---------|
| `age` | Policyholder age | 65 |
| `gender` | M or F | M |
| `mortality_rate` | Annual death probability | 0.0123 |

## 🚀 Quick Start

### 1. Set Up Virtual Environment

Create and activate a Python virtual environment:

```bash
# Create virtual environment
python3 -m venv annuity_pred

# Activate virtual environment
# On macOS/Linux:
source annuity_pred/bin/activate

# On Windows:
# annuity_pred\Scripts\activate

# Verify activation (you should see (annuity_pred) in your terminal)
which python
```

### 2. Install Dependencies

With your virtual environment activated, install the required packages:

```bash
pip install -r requirements.txt
```

### 3. Generate Synthetic Data

```bash
python src/generate_synthetic_data.py
```

This creates:
- `data/annuity_policies.csv` (10,000 synthetic policies)
- `data/market_returns.csv` (2000-2050 market data)
- `data/mortality_rates.csv` (ages 45-100 mortality)

### 4. Run ETL Pipeline

```bash
python src/etl_pipeline.py
```

### 5. Generate Forecasts

```bash
python src/forecast_engine.py
```

## 🔧 Key Features

### Rider Logic Implementation
- **GMWB (Guaranteed Minimum Withdrawal Benefit)**: 5% annual withdrawals for life
- **GMIB (Guaranteed Minimum Income Benefit)**: 6% roll-up during deferral, then annuitization
- **Fee Structures**: 1% annual rider fees, 0.5% admin fees

### Risk Modeling
- **Longevity Risk**: Monte Carlo simulation of death ages
- **Market Risk**: Stress testing with historical crash scenarios
- **Interest Rate Risk**: Sensitivity analysis on discount rates

### Profitability Analysis
- **Per Policy**: Expected profit/loss calculation
- **Portfolio Level**: Aggregate risk metrics
- **Cohort Analysis**: Performance by age, gender, rider type

## 📈 Business Value

This pipeline helps insurers like Venerable:

1. **Acquire Legacy Blocks**: Model profitability of annuity portfolios
2. **Reserve Planning**: Calculate required capital for future payouts
3. **Risk Management**: Identify high-exposure policies
4. **Pricing Optimization**: Adjust rider fees based on risk profiles

## 🎯 Interview Talking Points

When discussing this project:

- **"I built a complete ETL pipeline using PySpark and AWS Glue to process annuity data"**
- **"The forecasting engine models GMWB and GMIB rider payouts with realistic mortality assumptions"**
- **"I can show you how to identify which annuity blocks are profitable vs. risky"**
- **"The system handles the same data complexity Venerable faces with legacy variable annuity portfolios"**

## 📁 Project Structure

```
annuity-forecasting/
├── annuity_pred/                   # Virtual environment (created by user)
├── data/                          # Generated datasets
│   ├── annuity_policies.csv
│   ├── market_returns.csv
│   └── mortality_rates.csv
├── src/                           # Source code
│   ├── generate_synthetic_data.py
│   ├── etl_pipeline.py
│   ├── forecast_engine.py
│   └── utils.py
├── notebooks/                     # Jupyter notebooks
│   └── forecast_demo.ipynb
├── requirements.txt
└── README.md
```

## 🔮 Next Steps

1. **AWS Integration**: Deploy to Redshift and S3
2. **Real-time Updates**: Daily data ingestion from APIs
3. **ML Models**: Predictive modeling for surrender rates
4. **Dashboard**: Real-time BI dashboard with Tableau/PowerBI

## 💡 Development Tips

- **Always activate your virtual environment** before working on the project:
  ```bash
  source annuity_pred/bin/activate
  ```
- **Deactivate when done**:
  ```bash
  deactivate
  ```
- **Update requirements** if you add new packages:
  ```bash
  pip freeze > requirements.txt
  ```

---

*Built for Venerable Holdings interview preparation - demonstrating ETL, PySpark, AWS, and financial domain expertise.* 