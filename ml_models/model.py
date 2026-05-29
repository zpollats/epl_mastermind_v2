import duckdb
import xgboost
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.models import DummyClassifier, LogisticRegression, LinearRegression
from sklearn.preprocessing import StandardScaler

# Load data

