from pyspark import SparkContext
from pyspark.sql import Row,functions
from pyspark.sql import SQLContext

def movien():
    movien1 = {}
    i=1
    with open("C:/spark-3.0.0-preview2-bin-hadoop2.7/ml-100k/u.item") as f:
        for line in f:
            
            fields = line.split('|')
            #movien1 = Row(movieid=int(field[0]),names=str(field[1]))
            movien1[int(fields[0])] = fields[1]
            
    #movien will store movie names in movie id index.
    return movien1


def parseInput(lines):
    field = lines.split()
    return Row(movieid=int(field[1]),rating=float(field[2]))

sc = SparkContext(appName="Movies_Rating")
sqlContext = SQLContext(sc)

movieNames = movien()

#loading data from local
m1 = sc.textFile("u.data",200)

#mapping
mapper = m1.map(parseInput)

#Create Dataframe
dataset = sqlContext.createDataFrame(mapper)
#find average of ratings for each movieid

avg = dataset.groupBy("movieID").avg("rating")
#Count occurance of each movieid
count = dataset.groupBy("movieID").count()

joinD = count.join(avg,"movieID")
#print first 10 records
topten = joinD.sort("avg(rating)").take(10)


#print(topten)
#print the details of first 10 records
for r in topten:
    print(movieNames[r[0]],r[1],r[2])
sc.stop()






