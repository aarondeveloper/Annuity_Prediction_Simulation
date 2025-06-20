"""
Synthetic Annuity Policy Data Generator
Generates realistic annuity contract data for risk modeling and forecasting.
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os

# Initialize Faker for realistic data generation
fake = Faker()

def generate_annuity_policies(n_policies=1000):
    """
    Generate synthetic annuity policy data with realistic distributions.
    
    Args:
        n_policies (int): Number of policies to generate
        
    Returns:
        pd.DataFrame: DataFrame with synthetic annuity policy data
    """
    
    # Set random seed for reproducibility
    np.random.seed(42)
    Faker.seed(42)
    
    # Generate policy IDs
    policy_ids = [f'P{i:05d}' for i in range(1, n_policies + 1)]
    
    # Age distribution - most annuity buyers are 50-75
    ages = np.random.normal(62, 8, n_policies)
    ages = np.clip(ages, 45, 80).astype(int)
    
    # Gender distribution - slightly more women buy annuities
    genders = np.random.choice(['M', 'F'], size=n_policies, p=[0.45, 0.55])
    
    # Start dates - spread over last 20 years, more recent contracts
    start_dates = []
    for _ in range(n_policies):
        # Weight towards more recent years
        year = np.random.choice(
            list(range(2005, 2025)),
            p=[0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10, 0.11, 
               0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19, 0.20, 0.21]
        )
        month = np.random.randint(1, 13)
        day = np.random.randint(1, 29)
        start_dates.append(f"{year}-{month:02d}-{day:02d}")
    
    # Deferral years - most people defer 0-15 years
    deferral_years = np.random.choice(
        [0, 5, 10, 15, 20],
        size=n_policies,
        p=[0.25, 0.30, 0.25, 0.15, 0.05]  # More people defer 5-10 years
    )
    
    # Initial values - log-normal distribution for realistic amounts
    initial_values = np.random.lognormal(mean=11.5, sigma=0.5, size=n_policies)
    initial_values = np.clip(initial_values, 50000, 1000000)
    initial_values = (initial_values / 1000).astype(int) * 1000  # Round to nearest 1000
    
    # Rider types with realistic distribution
    rider_types = np.random.choice(
        ['None', 'GMWB', 'GMIB'],
        size=n_policies,
        p=[0.40, 0.35, 0.25]  # 40% no rider, 35% GMWB, 25% GMIB
    )
    
    # Status - most policies are still in deferral
    # Calculate if policy should be in payout based on start_date + deferral_years
    statuses = []
    current_date = datetime.now()
    
    for i in range(n_policies):
        start_date = datetime.strptime(start_dates[i], '%Y-%m-%d')
        deferral_end = start_date + timedelta(days=deferral_years[i] * 365)
        
        if deferral_end <= current_date:
            statuses.append('payout')
        else:
            statuses.append('deferred')
    
    # Create DataFrame
    df = pd.DataFrame({
        'policy_id': policy_ids,
        'age': ages,
        'gender': genders,
        'start_date': start_dates,
        'deferral_years': deferral_years,
        'initial_value': initial_values,
        'rider_type': rider_types,
        'status': statuses
    })
    
    return df

def generate_market_returns(start_year=2000, end_year=2050):
    """
    Generate synthetic market returns based on historical S&P 500 patterns.
    
    Args:
        start_year (int): Start year for returns
        end_year (int): End year for returns
        
    Returns:
        pd.DataFrame: DataFrame with year and return percentage
    """
    
    np.random.seed(42)
    
    years = list(range(start_year, end_year + 1))
    
    # Generate returns with realistic parameters
    # Historical S&P 500: ~7% mean, ~15% std dev
    returns = np.random.normal(loc=7.0, scale=15.0, size=len(years))
    
    # Add some market cycles and extreme events
    # 2008-like crash
    crash_year = 2008
    if crash_year in years:
        crash_idx = years.index(crash_year)
        returns[crash_idx] = -37.0
    
    # 2020 COVID crash
    covid_year = 2020
    if covid_year in years:
        covid_idx = years.index(covid_year)
        returns[covid_idx] = -20.0
    
    # Some boom years
    boom_years = [2003, 2009, 2013, 2017, 2021]
    for year in boom_years:
        if year in years:
            year_idx = years.index(year)
            returns[year_idx] = np.random.uniform(20, 30)
    
    # Cap extreme values
    returns = np.clip(returns, -50, 50)
    
    df = pd.DataFrame({
        'year': years,
        'return_pct': returns.round(2)
    })
    
    return df

def generate_mortality_rates():
    """
    Generate synthetic mortality rates based on actuarial tables.
    
    Returns:
        pd.DataFrame: DataFrame with age and mortality rate
    """
    
    np.random.seed(42)
    
    ages = list(range(45, 101))  # Ages 45-100
    
    # Create realistic mortality curve
    # Mortality increases exponentially with age
    base_rates = []
    for age in ages:
        if age < 60:
            # Low mortality for younger ages
            rate = 0.001 + (age - 45) * 0.0005
        elif age < 80:
            # Exponential increase
            rate = 0.005 * np.exp((age - 60) * 0.08)
        else:
            # Very high mortality for elderly
            rate = 0.05 + (age - 80) * 0.02
        
        # Add some noise
        rate += np.random.normal(0, rate * 0.1)
        rate = max(0.0001, rate)  # Ensure positive
        base_rates.append(rate)
    
    # Gender-specific adjustments
    male_rates = [rate * 1.2 for rate in base_rates]  # Men have higher mortality
    female_rates = [rate * 0.8 for rate in base_rates]  # Women have lower mortality
    
    # Create separate dataframes for male and female
    male_df = pd.DataFrame({
        'age': ages,
        'gender': 'M',
        'mortality_rate': [round(rate, 4) for rate in male_rates]
    })
    
    female_df = pd.DataFrame({
        'age': ages,
        'gender': 'F',
        'mortality_rate': [round(rate, 4) for rate in female_rates]
    })
    
    # Combine
    mortality_df = pd.concat([male_df, female_df], ignore_index=True)
    
    return mortality_df

def main():
    """Generate all synthetic datasets and save to CSV files."""
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    print("Generating synthetic annuity policy data...")
    policies_df = generate_annuity_policies(n_policies=1000)
    policies_df.to_csv('data/annuity_policies.csv', index=False)
    print(f"Generated {len(policies_df)} annuity policies")
    
    print("Generating market returns data...")
    returns_df = generate_market_returns()
    returns_df.to_csv('data/market_returns.csv', index=False)
    print(f"Generated {len(returns_df)} years of market returns")
    
    print("Generating mortality rates data...")
    mortality_df = generate_mortality_rates()
    mortality_df.to_csv('data/mortality_rates.csv', index=False)
    print(f"Generated mortality rates for ages 45-100")
    
    # Print summary statistics
    print("\n=== DATA SUMMARY ===")
    print(f"Annuity Policies:")
    print(f"  - Total policies: {len(policies_df)}")
    print(f"  - Average age: {policies_df['age'].mean():.1f}")
    print(f"  - Average initial value: ${policies_df['initial_value'].mean():,.0f}")
    print(f"  - Rider distribution:")
    print(policies_df['rider_type'].value_counts())
    print(f"  - Status distribution:")
    print(policies_df['status'].value_counts())
    
    print(f"\nMarket Returns:")
    print(f"  - Years covered: {returns_df['year'].min()} - {returns_df['year'].max()}")
    print(f"  - Average return: {returns_df['return_pct'].mean():.2f}%")
    print(f"  - Min return: {returns_df['return_pct'].min():.2f}%")
    print(f"  - Max return: {returns_df['return_pct'].max():.2f}%")
    
    print(f"\nMortality Rates:")
    print(f"  - Age range: {mortality_df['age'].min()} - {mortality_df['age'].max()}")
    print(f"  - Average mortality rate: {mortality_df['mortality_rate'].mean():.4f}")
    
    print("\nData files saved to 'data/' directory:")
    print("  - annuity_policies.csv")
    print("  - market_returns.csv") 
    print("  - mortality_rates.csv")

if __name__ == "__main__":
    main() 