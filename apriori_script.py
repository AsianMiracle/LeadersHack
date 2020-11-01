import pandas as pd
from sqlalchemy import create_engine
from apyori import apriori 

engine = create_engine('postgresql://postgres:postgres@193.37.212.191:5432/postgres')

def remove_trans(li):
    for l in li:
        l[:] = (ele for ele in l if ((ele != None) and (str(ele) != 'nan') and (str(ele) != 'None')))

# Data Preprocessing 
book = pd.read_sql("SELECT * FROM book_exp2;", engine)
book.set_index('userid',inplace=True)
activ = pd.read_sql("SELECT * FROM activ_exp;", engine)
activ.set_index('userid',inplace=True)
mero = pd.read_sql("SELECT * FROM mero_exp1;", engine)
mero.set_index('userid',inplace=True)
dataset = pd.merge(activ, mero, how='outer', right_index=True, left_index=True).merge(book, how='outer', right_index=True, left_index=True)
ds = pd.merge(activ, mero, how='outer', right_index=True, left_index=True).merge(book, how='outer', right_index=True, left_index=True)   
        
transactions = [] 
for i in range(0, dataset.shape[0]): 
    transactions.append([str(dataset.values[i,j]) for j in range(0, dataset.shape[1])])

remove_trans(transactions)

# Apriori algorithm
rules = apriori(transactions, min_support = 0.0001, min_confidence = 0.01, min_lift = 1, min_length = 2)

# Visualising the results
results = list(rules)
lo = []
for i in range(0, len(results)):
    lo.append(results[i][0])
df = pd.DataFrame(lo)
lo = df.values.tolist()
remove_trans(lo)

df.to_sql("apriory_output", engine, if_exists='replace')