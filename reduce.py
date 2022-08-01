#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
column_name=['petal.length', 'petal.width'];agg=''

'''
agg- type of aggregation (count,sum,avg,max,min)
column_name- list of name of the columns that needs to be selected

'''


infile=sys.stdin

#for pretty print
row_format ="{:>10}" * (len(column_name) + 1)
column_name = [x.upper() for x in column_name] 

key=""
value=""
count=0
final_val=0
i=0#counter variable

mini=99999999999999
maxi=0

my_list=[]

for line in infile:
	
	line=line.strip()
	my_list=line.split(",")
	length=len(my_list)
	value=my_list.pop()
	if(length==2):
		key=my_list[0]
	
	
	try:
		value=float(value)
	except ValueError:
		continue
	
	
	if(not agg):
		
		if(i==0):
			print(row_format.format("", *column_name))
		print(row_format.format("", *my_list))

	else:
		
		if(agg=='count' or agg=='avg'):
			count+=value
		if(agg=='avg' or agg=='sum' or agg=='min' or agg=='max'):
			
			try:
				key=float(key)
			except ValueError:
				print("value error")
				continue
			final_val+=key
			mini=min(key,mini)
			maxi=max(key,maxi)
	i+=1

if(agg=="avg"):
	print(column_name[0].upper()+"_AVERAGE")
	print("----------")
	print(final_val/count)
elif(agg=="count"):
	print(column_name[0].upper()+"_COUNT")
	print("----------")
	print(count)
elif(agg=="sum"):
	print(column_name[0].upper()+"_SUM")
	print("----------")
	print(final_val)
elif(agg=="min"):
	print(column_name[0].upper()+"_MIN")
	print("----------")
	print(mini)
elif(agg=="max"):
	print(column_name[0].upper()+"_MAX")
	print("----------")
	print(maxi)

