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

def generate_annuity_policies(n_policies=10000):
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
        years_range = list(range(2005, 2025))
        # Create linearly increasing weights to make recent years more likely
        weights = np.arange(1, len(years_range) + 1)
        probabilities = weights / np.sum(weights)
        
        year = int(np.random.choice(
            years_range,
            p=probabilities
        ))
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
    #e^11.5 = approx 98,971
    initial_values = np.random.lognormal(mean=11.5, sigma=0.5, size=n_policies)
    initial_values = np.clip(initial_values, 50000, 1000000)
    initial_values = (initial_values / 1000).astype(int) * 1000  # Round to nearest 1000
    
    # Rider types with realistic distribution
    rider_types = np.random.choice(
        ['None', 'GMWB', 'GMIB'],
        size=n_policies,
        p=[0.40, 0.35, 0.25]  # 40% no rider, 35% GMWB, 25% GMIB
    )
    
    # Add Death Benefit based on Rider Type
    # Create an empty array to hold the death benefit types
    death_benefit_types = np.empty(n_policies, dtype=object)
    
    # First, determine which policies have death benefits (only ~80% do)
    has_death_benefit = np.random.choice([True, False], size=n_policies, p=[0.80, 0.20])
    
    # Policies with no living benefit rider typically have the most basic death benefit
    no_rider_mask = (rider_types == 'None')
    rider_mask = ~no_rider_mask
    
    # Assign death benefit types only to policies that have death benefits
    for i in range(n_policies):
        if not has_death_benefit[i]:
            death_benefit_types[i] = 'None'
        elif no_rider_mask[i]:
            # Basic policies with death benefit get AccountValue
            death_benefit_types[i] = 'AccountValue'
        else:
            # Policies with riders that have death benefits get enhanced options
            death_benefit_types[i] = np.random.choice(
                ['AccountValue', 'ReturnOfPremium', 'SteppedUp'],
                p=[0.5, 0.4, 0.1]  # 50% basic, 40% ROP, 10% SteppedUp for policies with riders
            )
    
    # Add beneficiary designation probabilities based on rider type
    has_beneficiary = np.zeros(n_policies, dtype=bool)
    
    # Higher probability for policies with death benefit riders
    for i, (rider_type, has_db) in enumerate(zip(rider_types, has_death_benefit)):
        if not has_db:
            # No death benefit = no beneficiary needed
            has_beneficiary[i] = False
        elif rider_type == 'None':
            # Basic policies: 85-90% have beneficiaries
            has_beneficiary[i] = np.random.choice([True, False], p=[0.87, 0.13])
        elif rider_type == 'GMWB':
            # GMWB: 80-85% have beneficiaries (slightly lower as focus is on lifetime income)
            has_beneficiary[i] = np.random.choice([True, False], p=[0.82, 0.18])
        elif rider_type == 'GMIB':
            # GMIB: 85-90% have beneficiaries (similar to basic policies)
            has_beneficiary[i] = np.random.choice([True, False], p=[0.87, 0.13])
    
    # Add fee structures based on rider type and death benefit type
    rider_fees = np.zeros(n_policies)
    me_fees = np.zeros(n_policies)  # Mortality and Expense fees
    admin_fees = np.zeros(n_policies)
    death_benefit_fees = np.zeros(n_policies)  # Additional fees for enhanced death benefits
    
    for i, (rider_type, death_benefit_type, has_db) in enumerate(zip(rider_types, death_benefit_types, has_death_benefit)):
        # Base M&E fees vary by rider type and whether there's a death benefit
        if not has_db:
            # No death benefit = lower M&E fees (no mortality risk to cover)
            if rider_type == 'None':
                me_fees[i] = np.random.uniform(0.005, 0.009)  # 0.5% - 0.9% for basic policies
            elif rider_type == 'GMWB':
                me_fees[i] = np.random.uniform(0.008, 0.012)  # 0.8% - 1.2% for GMWB
            elif rider_type == 'GMIB':
                me_fees[i] = np.random.uniform(0.009, 0.013)  # 0.9% - 1.3% for GMIB
        else:
            # Has death benefit = higher M&E fees (covers mortality risk)
            if rider_type == 'None':
                me_fees[i] = np.random.uniform(0.008, 0.012)  # 0.8% - 1.2% for basic policies
            elif rider_type == 'GMWB':
                me_fees[i] = np.random.uniform(0.012, 0.016)  # 1.2% - 1.6% for GMWB
            elif rider_type == 'GMIB':
                me_fees[i] = np.random.uniform(0.013, 0.017)  # 1.3% - 1.7% for GMIB (highest risk)
        
        # Administrative fees (consistent across types)
        admin_fees[i] = np.random.uniform(0.001, 0.003)
        
        # Rider-specific fees
        if rider_type == 'None':
            rider_fees[i] = 0.0  # No rider fee for basic policies
        elif rider_type == 'GMWB':
            # GMWB rider fees: 0.6% to 1.2% (higher due to lifetime guarantee)
            rider_fees[i] = np.random.uniform(0.006, 0.012)
        elif rider_type == 'GMIB':
            # GMIB rider fees: 0.7% to 1.3% (highest due to income conversion guarantee)
            rider_fees[i] = np.random.uniform(0.007, 0.013)
        
        # Death benefit fees (only if death benefit exists)
        if not has_db or death_benefit_type == 'None':
            death_benefit_fees[i] = 0.0  # No death benefit = no death benefit fee
        elif death_benefit_type == 'AccountValue':
            death_benefit_fees[i] = 0.0  # Basic death benefit included in M&E
        elif death_benefit_type == 'ReturnOfPremium':
            # Return of Premium: 0.2% - 0.4% additional fee
            death_benefit_fees[i] = np.random.uniform(0.002, 0.004)
        elif death_benefit_type == 'SteppedUp':
            # Stepped Up: 0.3% - 0.6% additional fee (most expensive)
            death_benefit_fees[i] = np.random.uniform(0.003, 0.006)
    
    # Calculate total annual fees
    total_fees = me_fees + admin_fees + rider_fees + death_benefit_fees
    
    # Status - most policies are still in deferral
    # Calculate if policy should be in payout based on start_date + deferral_years
    statuses = []
    current_date = datetime.now()
    
    for i in range(n_policies):
        start_date = datetime.strptime(start_dates[i], '%Y-%m-%d')
        deferral_end = start_date + timedelta(days=int(deferral_years[i] * 365))
        
        if deferral_end <= current_date:
            statuses.append('payout')
        else:
            statuses.append('deferred')
    
    # Create DataFrame
    policies_df = pd.DataFrame({
        'policy_id': policy_ids,
        'age': ages,
        'gender': genders,
        'start_date': start_dates,
        'deferral_years': deferral_years,
        'initial_value': initial_values,
        'rider_type': rider_types,
        'has_death_benefit': has_death_benefit,
        'death_benefit_type': death_benefit_types,
        'has_beneficiary': has_beneficiary,
        'me_fees': me_fees,
        'admin_fees': admin_fees,
        'rider_fees': rider_fees,
        'death_benefit_fees': death_benefit_fees,
        'total_fees': total_fees,
        'status': statuses
    })
    
    return policies_df

def main():
    """Generate synthetic annuity policy data and save to a CSV file."""
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    print("Generating synthetic annuity policy data...")
    policies_df = generate_annuity_policies(n_policies=25000)
    policies_df.to_csv('data/annuity_policies.csv', index=False)
    print(f"Generated {len(policies_df)} annuity policies.")
    
    # Print summary statistics for the generated policies
    print("\n=== ANNUITY POLICY DATA SUMMARY ===")
    print(f"  - Total policies: {len(policies_df)}")
    print(f"  - Average age: {policies_df['age'].mean():.1f}")
    print(f"  - Average initial value: ${policies_df['initial_value'].mean():,.0f}")
    print(f"  - Rider distribution:")
    print(policies_df['rider_type'].value_counts(normalize=True).round(2))
    print(f"\n  - Death Benefit distribution:")
    print(policies_df['has_death_benefit'].value_counts(normalize=True).round(2))
    print(f"\n  - Death Benefit Type distribution:")
    print(policies_df['death_benefit_type'].value_counts(normalize=True).round(2))
    print(f"\n  - Beneficiary distribution:")
    print(policies_df['has_beneficiary'].value_counts(normalize=True).round(2))
    print(f"\n  - Fee distribution:")
    print(f"  - Mortality and Expense fees: {policies_df['me_fees'].mean():.4f}")
    print(f"  - Administrative fees: {policies_df['admin_fees'].mean():.4f}")
    print(f"  - Rider fees: {policies_df['rider_fees'].mean():.4f}")
    print(f"  - Death Benefit fees: {policies_df['death_benefit_fees'].mean():.4f}")
    print(f"  - Total annual fees: {policies_df['total_fees'].mean():.4f}")
    print(f"\n  - Status distribution:")
    print(policies_df['status'].value_counts(normalize=True).round(2))
    
    print("\nData file saved to 'data/' directory:")
    print("  - annuity_policies.csv")

if __name__ == "__main__":
    main() 