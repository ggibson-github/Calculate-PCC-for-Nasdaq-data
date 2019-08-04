# Calculate-PCC-for-Nasdaq-data
Calculate PCC for Nasdaq data using python CPU multicore processing

A python file containing code that will look at a folder containing a list of files, each of which contains stock data for a single stock and the file is named with the stocks symbol. 100 days of data is taken from each file and then Pearson Coefficient Correlation is ran against each combination of stocks using the pearsonr function from scipy.stats.stats.
