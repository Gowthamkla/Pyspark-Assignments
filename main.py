from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Assign1")
sc = SparkContext(conf=conf)

users= sc.textFile(r'C:\Users\91960\Desktop\pyspark ass\Spark 1 Ass\users.csv')
transactions=sc.textFile(r'C:\Users\91960\Desktop\pyspark ass\Spark 1 Ass\transactions.csv')

rdd_user = users.map(lambda l: l.split(",")).map(lambda l :(l[0],l[3]))
rdd_transaction=transactions.map(lambda l: l.split(",")).map(lambda l :(l[2],l[4]))

def generatekeyvalue (a):
   key = a[1]
   value = a[0]
   return key,value

merged_rdd=rdd_user.join(rdd_transaction).values().map(generatekeyvalue).distinct().countByKey()
print (merged_rdd)

products = transactions.map(lambda l: l.split(",")).map(lambda l:(l[2],l[4])).groupByKey()
print(list((product[0], list(product[1])) for product in products.collect()))

price = transactions.map(lambda l: l.split(",")).map(lambda l:(l[1],l[3])).distinct()

user = transactions.map(lambda l: l.split(",")).map(lambda l:(l[1],l[2]))

user_price_product = price.join(user)

def getmappings (val):
    product_id = val[0]
    key_value = val[1]
    product_price = key_value[0]
    user_id = key_value[1]
    return (user_id,(product_id,product_price))


total_spendings = user_price_product.map(getmappings).sortByKey().collect()
print (total_spendings)

