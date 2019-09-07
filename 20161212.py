import sys
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword

# SQL PARSING

class SQLParser(object):
	def parse_sql_columns(sql):
		columns = []
		parsed = sqlparse.parse(sql)
		stmt = parsed[0]
		for token in stmt.tokens:
			if isinstance(token, IdentifierList):
				for identifier in token.get_identifiers():
					columns.append(str(identifier))
			if isinstance(token, Identifier):
				columns.append(str(token))
			if token.ttype is Keyword:  # from
				break
		return columns

	def get_table_name(token):
		parent_name = token.get_parent_name()
		real_name = token.get_real_name()
		if parent_name:
			return parent_name + "." + real_name
		else:
			return real_name

	def parse_sql_tables(sql):
		tables = []
		parsed = sqlparse.parse(sql)
		stmt = parsed[0]
		from_seen = False
		for token in stmt.tokens:
			if from_seen:
				if token.ttype is Keyword:
					continue
				else:
					if isinstance(token, IdentifierList):
						for identifier in token.get_identifiers():
							tables.append(SQLParser.get_table_name(identifier))
					elif isinstance(token, Identifier):
						tables.append(SQLParser.get_table_name(token))
					else:
						pass
			if token.ttype is Keyword and token.value.upper() == "FROM":
				from_seen = True
		return tables



# DB PARSING
class TableMetadata:
	'''Class to create objects which store metadata of each table'''
	def __init__(self, name):
		self.name = name
		self.attributes = []

	def add_attribute(self, attribute_name):
		#print self.name
		#print "pre:", self.attributes
		self.attributes.append(attribute_name)
		#print "post:", self.attributes

def read_metadata(metadata_all):

	try:
		with open("metadata.txt", "r") as file: 
			data = file.readlines() 

			flag_table = -1 #flags for table reading: -1 normally, 0 after begin table encountered, 1 for when attributes being defined, -1 after end table
			current_table_name = ""

			for line in data:
				line = line.strip()

				if line == "<begin_table>":
					flag_table = 0

				elif line == "<end_table>":
					flag_table = -1

				elif line == "": #ignore empty lines after strip
					continue

				elif flag_table == 0: #this is going to be a table name
					current_table_name = line
					metadata_all[current_table_name] = TableMetadata(current_table_name)
					flag_table = 1

				elif flag_table == 1: #reading attributes of the current table
					metadata_all[current_table_name].add_attribute(line)

				else:
					print("Syntax Error in metadata.txt")
	except IOError:
		print("No Metadata file (Should be named metadata.txt)")
		sys.exit(1)
		#ERROR

	return metadata_all

def read_table(table_name):
	file_name = table_name + ".csv"

	table_data = []

	#Validate csv file with metadata [TODO]

	try:
		with open(file_name, "r") as file:
			data = file.readlines()

			for line in data:
				line = line.strip()
				line = line.split(",")

				table_data.append(line)
	
	except IOError:
		print("No csv file found for table:", table_name, "in the database.")
		sys.exit(1)
		#ERROR

	return table_data

def select_query(metadata_all, table_name, query_attributes_list):
	return_table = []

	table_data = read_table(table_name)

	table_attributes_list = metadata_all[table_name].attributes

	if query_attributes_list[0] == "*": #If returning all columns of table
		header_line = table_attributes_list

		new_header_line = []
		for header in header_line:
			new_header_line.append(table_name + "." + header) #adding table name to header

		return_table.append(new_header_line)

		for data_tuple in table_data:
			return_table.append(data_tuple)

	else:

		#Verification that all columns given exist in table
		for query_attribute in query_attributes_list:
			if query_attribute not in table_attributes_list:
				print("Attribute " + str(query_attribute) + " mentioned in SELECT not found in", table_name)
				sys.exit(1)
				#ERROR

		#now we need the indices to print only the columns we need from the csv file
		indices_list = []
		header_line = []

		index_count = 0
		for table_attribute in table_attributes_list:
			if table_attribute in query_attributes_list:
				indices_list.append(index_count)
				header_line.append(table_name + "." + table_attribute)

			index_count += 1

		return_table.append(header_line)

		for data_tuple in table_data:
			return_table_tuple = []

			for index in indices_list:
				return_table_tuple.append(data_tuple[index]) #Getting the required columns

			return_table.append(return_table_tuple)

	return return_table

def print_output(final_table):
	for line in final_table:
		print(",".join(line))

def check_aggregate(sql_query_parsed_tokens):
	list_allowed_aggregates = ["SUM","MAX","MIN","AVERAGE"]

	for agg in list_allowed_aggregates:
		for token in sql_query_parsed_tokens:
			if agg in token.value.upper():
				return token.value.upper() #if agg function found, return equation

	return "0" #to keep return value consistent

def aggregate_query(metadata_all, table_name, check_retval):
	column_list = []
	return_output = []

	agg_string = check_retval.replace("("," ").replace(")"," ").split(" ") #replacing brackets with spaces

	agg_func = agg_string[0]
	column_list.append(agg_string[1])

	ret_table = select_query(metadata_all, table_list[0], column_list) #Get the column

	header_line = str(agg_func + "(" + ret_table[0][0] + ")")
	
	return_output.append([header_line])

	numbers_data = [ int(x[0]) for x in ret_table[1:] ] #excluding Header Line
	
	if agg_func == "MAX":
		return_output.append([str(max(numbers_data))])
	
	elif agg_func == "MIN":
		return_output.append([str(min(numbers_data))])

	elif agg_func == "SUM":
		return_output.append([str(sum(numbers_data))])

	elif agg_func == "AVERAGE":
		avg = sum(numbers_data) / len(numbers_data)
		return_output.append([str(avg)])

	else:
		print("ERROR in Aggregate Functions")
		sys.exit(1)

	return return_output

def multiple_table_select(metadata_all, table_list, query_attributes_list):
	#Calls select for each table and puts them together in one table
	return_table = []

	if query_attributes_list[0] == "*": #Get ALL columns from all tables mentioned
		for table_name in table_list: #Creating a combined table by calling select * for EACH table

			if len(return_table) < 1: #return_table is empty (first iteration)
				return_table = select_query(metadata_all, table_name, ["*"])

			else: #(all other iterations)
				next_table = select_query(metadata_all, table_name, ["*"])

				if len(return_table) != len(next_table):
					print("Tables not of same length!")
					sys.exit(1)

				for i in range(len(next_table)):
					return_table[i] = return_table[i] + next_table[i] #Combining all the tables into one output

		return return_table

	#If it's NOT Select *

	query_list_dict = {} #Key: TableName, Value: Attributes needed from that Table

	for table_name in table_list:
		query_list_dict[table_name] = [] #Add all the keys, and empty arrays for the columns needed

	for attribute in query_attributes_list:
		if "." in attribute: #If Table Name has been mentioned
			attribute_table_name = attribute.split(".")[0]

			if attribute_table_name in query_list_dict:
				query_list_dict[attribute_table_name].append(attribute.split(".")[1]) #Add column name to dict

			else:
				print("Error: Table mentioned in Query Attribute not recognised!")
				sys.exit(1)

		else: #Find which table this column belongs to (only out of tables given after from)
			found_flag = 0

			for table_name in table_list:
				if attribute in metadata_all[table_name].attributes: #the current attribute is a part of this table
					query_list_dict[table_name].append(str(attribute))
					found_flag = 1
					break
			
			if(found_flag == 0): #Didn't find attribute in any of the mentioned tables
				print("Attribute " + str(attribute) + " Not Found in any of the mentioned tables!")
				sys.exit(1)

	for table_name, query_list in query_list_dict.items(): #Creating a combined table by calling select for EACH table

		if len(return_table) < 1: #return_table is empty (first iteration)
			return_table = select_query(metadata_all, table_name, query_list)

		else: #(all other iterations)
			next_table = select_query(metadata_all, table_name, query_list)

			if len(return_table) != len(next_table):
				print("Tables not of same length!")
				sys.exit(1)

			for i in range(len(next_table)):
				return_table[i] = return_table[i] + next_table[i] #Combining all the tables into one output

	return return_table

def parse_where(where_string):
	allowed_functions = ["<=", ">=", "<", ">", "="] #Need to check double operators first
	allowed_operators = ["AND", "OR"]
	
	parsed_where = []

	for token in where_string:
		if token.value.upper() == "WHERE" or str.isspace(str(token)): #Ignore where and Whitespace
			continue

		for allowed_func in allowed_functions:
			if allowed_func in str(token):
				split_string = str(token).split(allowed_func)

				parsed_where.append(split_string[0].strip())
				parsed_where.append(allowed_func)
				parsed_where.append(split_string[1].strip())

				break

		if token.value.upper() == "AND" or token.value.upper() == "OR":
			parsed_where.append(str(token.value.upper()))

	return parsed_where

def where_comparison_check(data_cell, check_function, check_value):
	if check_function == "<=" and data_cell <= check_value:
		return 1

	if check_function == ">=" and data_cell >= check_value:
		return 1

	if check_function == "<" and data_cell < check_value:
		return 1

	if check_function == ">" and data_cell > check_value:
		return 1

	if check_function == "=" and data_cell == check_value:
		return 1

	return 0

def where_query(metadata_all, table_list, parsed_where, column_list):
	pruned_table = []
	select_output_table = [] 

	#Get full tables
	if len(table_list) == 1: #If only one table in query
		select_output_table = select_query(metadata_all, table_list[0], ["*"])
	else: #Multiple tables
		select_output_table = multiple_table_select(metadata_all, table_list, ["*"])

	pruned_table.append(select_output_table[0]) #Add Header Line to the Pruned Table

	select_output_table = select_output_table[1:] #Remove Header Line from Select Table

	check_attribute = parsed_where[0]
	check_function = parsed_where[1]
	check_value = int(parsed_where[2])

	attr_index = 0
	found_flag = 0
	for i in range(len(pruned_table[0])):
		if check_attribute == pruned_table[0][i] or check_attribute == pruned_table[0][i].split(".")[1]:
			found_flag = 1
			attr_index = i
			break

	if found_flag == 0:
		print("Error: Attribute Mentioned in WHERE is in none of the mentioned tables.")
		sys.exit(1)

	for select_tuple in select_output_table:
		if where_comparison_check(int(select_tuple[attr_index]), check_function, check_value) == 1:
			pruned_table.append(select_tuple) #If Condition Check returns true, Add to Pruned Table


	#AND/OR present


	#Run Select on this to get only required columns 
	header_line = pruned_table[0]

	indices_list = []

	for attribute_name in column_list:
		index_count = 0
		found_flag = 0
		
		for header_attribute in header_line: 
			
			if attribute_name == header_attribute: #full name provided in query
				indices_list.append(index_count)
				found_flag = 1
				break

			if attribute_name == header_attribute.split(".")[1]: #only column name provided
				indices_list.append(index_count)
				found_flag = 1
				break

			index_count += 1

		if found_flag == 0:
			print("Column in SELECT not found in the mentioned table(s)!")
			sys.exit(1)

	#Creating Final Table
	final_table = []

	for tuple_data in pruned_table:
		final_tuple_data = []

		for index_count in indices_list:
			final_tuple_data.append(tuple_data[index_count])

		final_table.append(final_tuple_data)

	return final_table


#MAIN

metadata_all = {} #dictionary where key is tablename and value is TableMetadata object
metadata_all = read_metadata(metadata_all)

#Query Parsing
sql_query = "Select B,C from table1 where A > 0"
sql_query2 = "Select * from table1,table2"
#print(sqlparse.format(sql_query, reindent=True, keyword_case='upper'))

parsed = sqlparse.parse(sql_query)[0]

column_list = SQLParser.parse_sql_columns(sql_query)
table_list = SQLParser.parse_sql_tables(sql_query)

if len(column_list) == 0:
	column_list = ["*"]

#Checking if Where exists in Query
parsed_where = []
where_flag = 0

for token in parsed.tokens:
	if "WHERE" in token.value.upper():
		parsed_where = parse_where(token)
		where_flag = 1
		break


if where_flag == 0:

	#FOR SELECT QUERIES (NO WHERE)
	final_output = []

	if len(table_list) == 1: #If only one table in query
		final_output = select_query(metadata_all, table_list[0], column_list)
	else: #Multiple tables
		final_output = multiple_table_select(metadata_all, table_list, column_list)

else: 

	#PROCESSING WHERE
	if len(parsed_where) > 0:

		final_output = where_query(metadata_all, table_list, parsed_where, column_list)

#Checking if Aggregate Function in Query
retval = check_aggregate(parsed.tokens)

#PROCESSING AGGREGATE FUNCTION 
if(retval != "0"): 
	final_output = aggregate_query(metadata_all, table_list[0], retval)



#PRINT FINAL OUTPUT
print_output(final_output)

#
#table_data = read_table(table_name)

#return_table = select_query(metadata_all, table_name, table_data, ["*"])

#print_output(return_table)

#print(table_data)

#METADATA TESTING
'''
for table_name, table_metadata in metadata_all.items():
	print "Table Name:", table_name

	print "Attributes:"

	print table_metadata.attributes

	print "***"
'''
