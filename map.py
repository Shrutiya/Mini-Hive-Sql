#!/usr/bin/python3
import sys
import csv
#import pydoop.hdfs as hdfs
index=[2, 3];condition=(4, '=', '"virginica"', 'string');


'''
index-index is a list of the indices of the column that is selected by the user 
condition-where condition if any as a tuple of index of column to checked,operator(">","<","=",">=","<="), value and the data type
'''
def print_col():
	string=""
	for ind in index:
		string+=str(my_list[ind])+','
	print("%s" % (string+'1'))
	return

def check_condition(conditon):
	check_index=condition[0]
	value=condition[2]
	if(condition[3]=='string'):
		if(condition[1]=='=' and my_list[check_index].lower()==value):
			print_col()
		elif(condition[1]=='>' or condition[1]=='<' or condition[1]=='>=' or condition[1]=='<='):
			print("String comparison not valid")
	else:
		if(condition[3]=='int'):
			try:
				my_list[check_index]=int(my_list[check_index])
				value=int(condition[2])
			except ValueError:
				print("Int Syntax Error")
		elif(condition[3]=='float'):
			try:
				my_list[check_index]=float(my_list[check_index])
				value=float(condition[2]) 
			except ValueError:
				print("Float Syntax Error")
		
		if(condition[1]=='>' and my_list[check_index]>value):
			print_col()

		elif(condition[1]=='<' and my_list[check_index]<value):
			print_col()

		elif(condition[1]=='=' and my_list[check_index]==value):
			print_col()

		elif(condition[1]=='>=' and my_list[check_index]>=value):
			print_col()

		elif(condition[1]=='<=' and my_list[check_index]<=value):
			print_col()


infile = sys.stdin

my_list=[]

i=0
for line in infile:
	
	if(i==0):
		i+=1
		continue
	
	line=line.strip()
	
	
	if(line==''):
		print("line empty")
		continue
	i+=1
	my_list=line.split(',')
	
	
	if( not condition ):
		print_col()
	else:
		check_condition(condition)
	
