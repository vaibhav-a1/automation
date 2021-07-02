import pandas as pd
import csv


with open("/Users/vaibhavverma/automation-1/ImpalaFile/test1.csv","r") as f1:
    reader = csv.reader(f1)
    header1 =next(reader)
    Impalafile = f1.readlines()
##print(header1)

with open("/Users/vaibhavverma/automation-1/SnowflakeFile/test1.csv","r") as f2:
    reader = csv.reader(f2)
    header2 = next(reader)
    Snowflakefile = f2.readlines()

##print(header2)

list1=list(set(header2) - set(header1))
list2=list(set(header1) - set(header2))
list1.sort()
list2.sort()
print("Mismatched columns in Snowlake: ", list1)
print("Mismatched columns in Impala: ", list2)

with open("/Users/vaibhavverma/automation-1/output/column_mismatch.csv", "w") as outfile:
    outfile.write("Mismatched columns in Snowlake:")
    outfile.write(str("\n"))
    outfile.write(str(list1))
    outfile.write(str("\n"))
    outfile.write(str("\n"))
    outfile.write(str("Mismatched columns in Impala:"))
    outfile.write(str("\n"))
    outfile.write(str(list2))

df1=pd.read_csv('/Users/vaibhavverma/automation-1/ImpalaFile/test1.csv')
df2=pd.read_csv('/Users/vaibhavverma/automation-1/SnowflakeFile/test1.csv')


df_join = df1.merge(right = df2,
                    left_on = df1.columns.to_list(),
                    right_on = df2.columns.to_list(),
                    how = 'outer')

df1.rename(columns= lambda x: x + '_Impala', inplace= True )
df2.rename(columns= lambda x: x + '_Snowflake', inplace= True )


df_join = df1.merge(right = df2,
                    left_on = df1.columns.to_list(),
                    right_on = df2.columns.to_list(),
                    how = 'outer')

records_present_in_df1_not_in_df2 = df_join.loc[df_join[df2.columns.to_list()].isnull().all(axis = 1), df1.columns.to_list()]
# %%

# %%
records_present_in_df2_not_in_df1 = df_join.loc[df_join[df1.columns.to_list()].isnull().all(axis = 1), df2.columns.to_list()]
# %%#
pd.DataFrame(records_present_in_df1_not_in_df2).to_csv('/Users/vaibhavverma/automation-1/output/impala.csv')
pd.DataFrame(records_present_in_df2_not_in_df1).to_csv('/Users/vaibhavverma/automation-1/output/snowflake.csv')

# %%
