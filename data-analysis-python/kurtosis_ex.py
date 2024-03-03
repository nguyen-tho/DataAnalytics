import numpy as np
import matplotlib.pyplot as plt
from scipy import stats

# Define sample datasets
income_high = np.random.normal(loc=100000, scale=50000, size=100)  # Leptokurtic (high earners)
income_balanced = np.random.normal(loc=50000, scale=20000, size=100)  # Mesokurtic (balanced)
income_low = np.random.uniform(low=30000, high=40000, size=100)  # Platykurtic (low variation)

test_high_low = np.concatenate((np.random.normal(loc=70, scale=15, size=20), np.random.normal(loc=30, scale=5, size=20)))  # Leptokurtic (outliers)
test_normal = np.random.normal(loc=50, scale=10, size=100)  # Mesokurtic (normal)
test_low = np.random.uniform(low=45, high=55, size=100)  # Platykurtic (low variation)

# Calculate kurtosis
kurtosis_high_income = stats.kurtosis(income_high)
kurtosis_balanced_income = stats.kurtosis(income_balanced)
kurtosis_low_income = stats.kurtosis(income_low)

kurtosis_high_low = stats.kurtosis(test_high_low)
kurtosis_normal = stats.kurtosis(test_normal)
kurtosis_low = stats.kurtosis(test_low)

# Print results
print("Kurtosis of high-income distribution:", kurtosis_high_income)
print("Kurtosis of balanced income distribution:", kurtosis_balanced_income)
print("Kurtosis of low-income distribution:", kurtosis_low_income)

print("\nKurtosis of high-low test scores:", kurtosis_high_low)
print("Kurtosis of normal test scores:", kurtosis_normal)
print("Kurtosis of low test scores:", kurtosis_low)

# Visualize distributions (optional)
plt.hist(income_high, label="High Income")
plt.hist(income_balanced, label="Balanced Income")
plt.hist(income_low, label="Low Income")
plt.legend()
plt.show()

# Similar plots can be created for test score distributions
