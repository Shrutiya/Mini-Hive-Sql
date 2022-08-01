#LOAD command : LOAD test/iris.csv AS (sepal.length:float,sepal.width:float,petal.length:float,petal.width:float,variety:string)


#SELECT command : SELECT petal.length from test/iris.csv
#select petal.length,petal.width from test/iris.csv
#SELECT petal.length from test/iris.csv where petal.length>2 aggregate_by sum  

###################################################################################################################################

import os
import pickle #module to store the schema in a binary file


datatypes=["int","float","string"]
where_cond=["=",">=",">","<=","<"]
aggregate=["sum","count","avg","min","max"]


def add_to_mapper(index,condition):
	f = open('map.py', 'r')
	lines = f.readlines()
	f.close()
	#print(lines)
	a = 'index=' + str(index) + ';'
	a += 'condition=' + str(condition) + ';'
	a += "\n"
	#print(a)
	lines[4] = a
	f = open('map.py', 'w')
	f.writelines(lines)
	f.close()


def add_to_reducer(column_name,agg):
	f = open('reduce.py', 'r')
	lines = f.readlines()
	f.close()
	#print(lines)
	a = 'column_name='+str(column_name)+";"
	a += "agg="+ "'" + agg + "'" + '\n'
	#print(a)
	lines[4] = a
	f = open('reduce.py', 'w')
	f.writelines(lines)
	f.close()

def mapred(db_name,table_name):
	os.system("hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -file map.py -mapper 'python map.py' -file reduce.py -reducer 'python reduce.py' -input /"+db_name+"/"+table_name+" -output /output1/")

	os.system("hdfs dfs -cat /output1/part-00000")

	os.system("hdfs dfs -rm -r /output1")


while(1):
	main_command=input("command >>>").lower()
	command=main_command.split()
	if(command[0]=="load"):
		if(len(command)==4):
			db_name,table_name=command[1].split("/")

			#print(db_name,table_name)

			result=os.system("hdfs dfs -ls /"+db_name+"/")
			if(result==0):
				print("Database already exists")
			else:
				os.system("hdfs dfs -mkdir -p /"+db_name)
			result_1=os.system("hdfs dfs -test -e /"+db_name+"/"+table_name)
			if(result_1==0):
				print("Table exists already")
			else:
				os.system("hadoop dfs -put /home/coder/Desktop/"+table_name+" /"+db_name+"/")
				#Creating a dictionary to hold the syntax of schema which will be encoded in a binary format and stored in HDFS		
				schema=command[-1].split(",")
				schema[0]=schema[0][1:]
				schema[-1]=schema[-1][:-1]
				stored_schema=dict()
				index=0
				for col in schema:
					col=col.split(":")
					stored_schema[col[0]]=(col[1],index)
					index+=1
				#print(stored_schema)
				f=open("stored_schema_"+table_name+".bd","wb")
				pickle.dump(stored_schema,f)
				f.close()
				os.system("hadoop dfs -put /home/coder/Documents/stored_schema_"+table_name+".bd /"+db_name)
		else:
			print("LOAD syntax not correct")


	elif(command[0]=="delete"):
		db_name,table_name=command[1].split("/")
		#print(db_name, table_name)

		result=os.system("hdfs dfs -ls /"+db_name+"/")
		if(result!=0):
			print("Database does not exist")
		else:
			if(table_name!=""):
				result_1=os.system("hdfs dfs -test -e /"+db_name+"/"+table_name)
				if(result_1!=0):
					print("Table does not exist")
				else:
					os.system("hdfs dfs -rm -r /"+db_name+"/"+table_name)
	
			else:
				os.system("hdfs dfs -rm -r /"+db_name)

	
	elif(command[0]=="select" or command[0]=="project"):
		if(command[2]!="from" or ("/" not in command[3])):
			print("Wrong Syntax")
		else:			
			for i in range(len(command)):
				if(command[i]=="from"):
					from_index=i
					break
			db_name,table_name=command[from_index+1].split("/")
			result=os.system("hdfs dfs -ls /"+db_name+"/")
			if(result!=0):
				print("Database does not exist")
			else:
				result_1=os.system("hdfs dfs -test -e /"+db_name+"/"+table_name)
				if(result_1!=0):
					print("Table does not exist")
				else:
					os.system("hdfs dfs -cat /"+db_name+"/stored_schema_"+table_name+".bd >> schema.txt")
					f=open("schema.txt","rb")
					f=f.read()
					stored_schema=pickle.loads(f)
					print(stored_schema)
					if(len(command[from_index:])==2):
						if(command[1]=="*"):
							if(len(command)!=4):
								print("invalid syntax")
								continue
							else:
								column_name=list(stored_schema.keys())
								index=list(range(len(stored_schema)))
						else:
							columns=command[1].split(",")
							column_name=columns
							index=list()
							for column in columns:								
								index.append(stored_schema[column][1])
						condition = ()
						agg=""
						#print(column_name,index,condition,agg)
						add_to_mapper(index,condition)
						add_to_reducer(column_name,agg)
						mapred(db_name,table_name)
					elif(len(command[from_index:])==4 or len(command[from_index:])==6):
						if(command[from_index+2]!="where" and command[from_index+2]!="aggregate_by"):
							print("invalid syntax, WHERE/AGGREGATE_BY condition not correctly written")  
						elif(command[from_index+2]=="where"):
							compare=0
							for i in where_cond:
								if i in command[from_index+3]:
									compare=i
									break
							if(not compare):
								print("Comparator not correctly specified")
								continue
							
							col,value=command[from_index+3].split(compare)
							#print(col,value,compare,stored_schema[col][0]!=datatypes[1])
							if(compare!="=" and stored_schema[col][0]!=datatypes[0] and stored_schema[col][0]!=datatypes[1]):
								print("Invalid data type given for comparision in WHERE condition")
							else:
								columns=command[1].split(",")
								column_name=columns
								index=list()
								for column in columns:								
									index.append(stored_schema[column][1])
								condition=(stored_schema[col][1],compare,value,stored_schema[col][0])
								agg=""
								if(len(command[from_index:])==4):
									#print(column_name,index,condition,agg)
									add_to_mapper(index,condition)
									add_to_reducer(column_name,agg)
									mapred(db_name,table_name)
								elif(len(command)==8 and command[6]!="aggregate_by"):
									print("invalid syntax for aggregate_by")
								else:
									if(command[7] not in aggregate):
										print("invalid aggregate action")
									elif(len(column_name)!=1):
										print("Wrong syntax for aggregate")
									else:
										agg=command[7]
										#print(column_name,index,condition,agg)
										add_to_mapper(index,condition)
										add_to_reducer(column_name,agg)
										mapred(db_name,table_name)
						elif(len(command)==6 and command[4]=="aggregate_by"):
							if(command[5] not in aggregate):
								print("invalid aggregate action")
							else:
								columns=command[1].split(",")
								column_name=columns
								index=list()
								for column in columns:								
									index.append(stored_schema[column][1])
								if(len(column_name)!=1):
									print("Wrong syntax for aggregate")
								else:
									condition=()
									agg=command[5]
									#print(column_name,index,condition,agg)
									add_to_mapper(index,condition)
									add_to_reducer(column_name,agg)
									mapred(db_name,table_name)
					else:
						print("invalid syntax")

						



	elif(command[0]=="exit"):
		print("Thank You")
		break
#print(command)
#os.system("bin/hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -file /home/shubha/map.py -mapper 'python3 map.py' -file /home/shubha/reduce.py -reducer 'python3 reduce.py' -input /input/* -output /output/o1.txt")



