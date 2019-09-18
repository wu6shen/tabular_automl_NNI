import pandas as pd
from numba import jit
import numpy as np
import libaa
from fe_util import *

@timmer
def process_by_c(df):
    pool = Pool(4)

    for col in df:
        if col.startswith("C"):
            df[col] = pd.DataFrame(libaa.convert(df[col].replace(np.nan, "", regex=True).values)).replace(-1, np.nan)

#file_name = 'train.tiny.csv'
#df = pd.read_csv(file_name)
file_name = './data/train-cp.txt'
df = pd.read_csv(file_name, sep="\t")
#process(df)

#df = df.replace(np.nan, "", regex = True)
print("read done!")

#sample_col = ['crosscount_C12_C8']
#sample_col = ['aggregate_var_I11_C1']
sample_col = ['crosscount_C11_C6', 'crosscount_C16_C17', 'aggregate_var_I11_C1', 'aggregate_var_I9_C2', 'crosscount_C11_C14', 'crosscount_C26_C5', 'aggregate_min_I11_C23', 'crosscount_C17_C5', 'crosscount_C11_C13', 'aggregate_min_I9_C4', 'crosscount_C18_C21', 'crosscount_C12_C3', 'crosscount_C12_C8', 'crosscount_C12_C2', 'crosscount_C12_C13', 'aggregate_median_I9_C24', 'aggregate_mean_I9_C5', 'aggregate_mean_I12_C24', 'crosscount_C16_C20', 'aggregate_var_I9_C3', 'aggregate_mean_I10_C15', 'crosscount_C19_C4', 'crosscount_C19_C21', 'crosscount_C17_C4', 'aggregate_mean_I11_C23', 'crosscount_C10_C21', 'aggregate_max_I11_C21', 'crosscount_C2_C7', 'crosscount_C11_C23', 'crosscount_C19_C7', 'crosscount_C11_C17', 'crosscount_C12_C4', 'aggregate_min_I9_C16', 'crosscount_C11_C26', 'aggregate_mean_I11_C6', 'crosscount_C21_C24', 'count_C7', 'crosscount_C23_C7', 'crosscount_C24_C4', 'aggregate_max_I9_C5', 'crosscount_C2_C5', 'crosscount_C22_C24', 'crosscount_C2_C23', 'aggregate_var_I11_C17', 'crosscount_C19_C2', 'count_C11', 'aggregate_max_I9_C8', 'aggregate_min_I9_C14', 'aggregate_max_I9_C24', 'aggregate_min_I11_C21', 'crosscount_C1_C12', 'crosscount_C26_C9', 'crosscount_C2_C4', 'aggregate_median_I9_C16', 'crosscount_C24_C7', 'crosscount_C24_C8', 'crosscount_C11_C12', 'crosscount_C17_C23', 'crosscount_C21_C6', 'crosscount_C12_C3', 'crosscount_C17_C26', 'crosscount_C14_C23', 'aggregate_max_I10_C21', 'crosscount_C17_C18', 'aggregate_mean_I11_C17', 'crosscount_C12_C9', 'crosscount_C17_C26', 'crosscount_C23_C8', 'crosscount_C16_C26', 'crosscount_C2_C3', 'aggregate_min_I11_C22', 'crosscount_C16_C18', 'crosscount_C12_C5', 'aggregate_max_I11_C24', 'aggregate_median_I11_C21', 'crosscount_C11_C9', 'crosscount_C17_C20', 'crosscount_C15_C26', 'crosscount_C14_C20', 'aggregate_median_I9_C20', 'aggregate_mean_I9_C6', 'crosscount_C17_C18', 'crosscount_C21_C8', 'crosscount_C2_C26', 'crosscount_C13_C9', 'crosscount_C13_C16', 'crosscount_C15_C17', 'crosscount_C16_C4', 'crosscount_C23_C3', 'aggregate_median_I12_C23', 'aggregate_min_I10_C16', 'aggregate_min_I9_C17', 'crosscount_C15_C19', 'aggregate_median_I9_C13', 'crosscount_C11_C17', 'aggregate_max_I11_C23', 'crosscount_C1_C17', 'crosscount_C17_C6', 'crosscount_C21_C26', 'crosscount_C13_C21', 'crosscount_C2_C20', 'crosscount_C10_C17', 'crosscount_C19_C24', 'crosscount_C17_C23', 'crosscount_C14_C25', 'crosscount_C12_C15', 'crosscount_C2_C8', 'crosscount_C12_C13', 'aggregate_var_I9_C24', 'aggregate_min_I12_C17', 'aggregate_min_I11_C17', 'crosscount_C3_C6', 'crosscount_C18_C21', 'aggregate_mean_I9_C16', 'crosscount_C15_C17', 'crosscount_C17_C22', 'crosscount_C12_C21', 'aggregate_mean_I11_C18', 'aggregate_median_I12_C16', 'aggregate_median_I11_C23', 'crosscount_C3_C5', 'crosscount_C16_C3', 'crosscount_C16_C25', 'crosscount_C19_C23', 'crosscount_C1_C17', 'crosscount_C3_C9', 'crosscount_C10_C12', 'crosscount_C11_C16'] 

pdf = name2feature_multi(df, sample_col, 'Label')
#pdf = name2feature(df, sample_col, 'Label')
#pdf.to_csv('test.csv')
