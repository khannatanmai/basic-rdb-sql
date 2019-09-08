import sys
import sqlparse
import re
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
				line = line.replace('"', '') #Remove double quotes in input
				line = line.split(",")

				table_data.append(line)
	
	except IOError:
		print("No csv file found for table:", table_name, "in the database.")
		sys.exit(1)
		#ERROR

	return table_data

def select_query(metadata_all, table_name, query_attributes_list, agg_retval):
	return_table = []

	table_data = read_table(table_name)

	table_attributes_list = metadata_all[table_name].attributes

	header_line = []
	for attribute in table_attributes_list:
		header_line.append(table_name + "." + attribute) #adding table name to header

	if query_attributes_list[0] == "*" and agg_retval == "0": #If returning all columns of table and not aggregate
		
		return_table.append(header_line)

		for data_tuple in table_data:
			return_table.append(data_tuple)

	else:
		indices_list = []

		#if Aggregate function, change AGG(Column_Name) to Column_Name

		if agg_retval != "0": #There is an aggregate function in the string
			query_attributes_list = [agg_retval.replace("("," ").replace(")"," ").split(" ")[1]] #replacing brackets with spaces and giving column name]

		#Verification that all columns given exist in table AND getting only mentioned columns
		
		for query_attribute in query_attributes_list:
			found_flag = 0
			index_count = 0
			
			for header_attribute in header_line: 
				
				if query_attribute == header_attribute: #full name provided in query
					indices_list.append(index_count)
					found_flag = 1
					break

				if query_attribute == header_attribute.split(".")[1]: #only column name provided
					indices_list.append(index_count)
					found_flag = 1
					break

				index_count += 1

			if found_flag == 0:
				print("Attribute " + str(query_attribute) + " mentioned in SELECT not found in", table_name)
				sys.exit(1)

		table_data = [header_line] + table_data #Adding header_line to table_data
		
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

def aggregate_query(current_table, check_retval):
	column_list = []
	return_output = []

	agg_string = check_retval.replace("("," ").replace(")"," ").split(" ") #replacing brackets with spaces

	agg_func = agg_string[0]
	column_list.append(agg_string[1])

	header_line = str(agg_func + "(" + current_table[0][0] + ")")
	
	return_output.append([header_line])

	numbers_data = [ int(x[0]) for x in current_table[1:] ] #excluding Header Line
	
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

def multiple_table_select(metadata_all, table_list, query_attributes_list): #Works for two tables for now
	return_table = []

	table_name1 = table_list[0]
	table_name2 = table_list[1]

	table_data1 = select_query(metadata_all, table_name1, ["*"], "0")
	table_data2 = select_query(metadata_all, table_name2, ["*"], "0")

	header_line = table_data1[0] + table_data2[0]
	return_table.append(header_line) 

	table_data1 = table_data1[1:] #Remove Header Line
	table_data2 = table_data2[1:]

	for data_tuple1 in table_data1:
		for data_tuple2 in table_data2:
			return_table.append(data_tuple1 + data_tuple2)

	if query_attributes_list[0] == "*": #If select *
		return return_table

	#If it's NOT Select *

	indices_list = []

	for query_attribute in query_attributes_list:
			found_flag = 0
			index_count = 0
			
			for header_attribute in header_line: 
				
				if query_attribute == header_attribute: #full name provided in query
					indices_list.append(index_count)
					found_flag = 1
					break

				if query_attribute == header_attribute.split(".")[1]: #only column name provided
					indices_list.append(index_count)
					found_flag = 1
					break

				index_count += 1

			if found_flag == 0:
				print("Attribute " + str(query_attribute) + " mentioned in SELECT not found in the tables.")
				sys.exit(1)

	#Creating Final Table
	final_table = []

	for tuple_data in return_table:
		final_tuple_data = []

		for index_count in indices_list:
			final_tuple_data.append(tuple_data[index_count])

		final_table.append(final_tuple_data)

	return final_table

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

def where_query(metadata_all, table_list, parsed_where, column_list, agg_retval):
	pruned_table = []
	select_output_table = [] 
	join_flag = 0

	#Get full tables
	if len(table_list) == 1: #If only one table in query
		select_output_table = select_query(metadata_all, table_list[0], ["*"], "0")
	else: #Multiple tables
		select_output_table = multiple_table_select(metadata_all, table_list, ["*"])

	pruned_table.append(select_output_table[0]) #Add Header Line to the Pruned Table

	select_output_table = select_output_table[1:] #Remove Header Line from Select Table

	check_attribute = parsed_where[0]
	check_function = parsed_where[1]
	check_value = parsed_where[2]

	if len(parsed_where) <= 3: #No AND/OR in Where Condition
		if check_value.isdigit(): #normal where condition
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
				if where_comparison_check(int(select_tuple[attr_index]), check_function, int(check_value)) == 1:
					pruned_table.append(select_tuple) #If Condition Check returns true, Add to Pruned Table

		else: #it's a join condition
			join_flag = 1
			attr_index = 0
			found_flag = 0
			for i in range(len(pruned_table[0])):
				if check_attribute == pruned_table[0][i] or check_attribute == pruned_table[0][i].split(".")[1]:
					found_flag += 1
					attr_index = i

				elif check_value == pruned_table[0][i] or check_value == pruned_table[0][i].split(".")[1]:
					found_flag += 1
					attr_index_val = i

			if found_flag < 2:
				print("Error: Attribute Mentioned in WHERE is in none of the mentioned tables.")
				sys.exit(1)

			for select_tuple in select_output_table:
				if where_comparison_check(int(select_tuple[attr_index]), check_function, int(select_tuple[attr_index_val])) == 1:
					pruned_table.append(select_tuple) #If Condition Check returns true, Add to Pruned Table

	else: #AND/OR present
		check_attribute2 = parsed_where[4] #parsed_where[3] is AND/OR
		check_function2 = parsed_where[5]
		check_value2 = int(parsed_where[6])

		attr_index2 = 0
		found_flag = 0
		for i in range(len(pruned_table[0])):
			if check_attribute2 == pruned_table[0][i] or check_attribute2 == pruned_table[0][i].split(".")[1]:
				found_flag = 1
				attr_index2 = i
				break

		if found_flag == 0:
			print("Error: Attribute Mentioned in WHERE is in none of the mentioned tables.")
			sys.exit(1)

		if parsed_where[3] == "AND":
			for select_tuple in select_output_table:
				if where_comparison_check(int(select_tuple[attr_index]), check_function, check_value) == 1 and where_comparison_check(int(select_tuple[attr_index2]), check_function2, check_value2) == 1:
					pruned_table.append(select_tuple) #If BOTH Condition Check returns true, Add to Pruned Table

		elif parsed_where[3] == "OR":
			for select_tuple in select_output_table:
				if where_comparison_check(int(select_tuple[attr_index]), check_function, check_value) == 1 or where_comparison_check(int(select_tuple[attr_index2]), check_function2, check_value2) == 1:
					pruned_table.append(select_tuple) #If EITHER Condition Check returns true, Add to Pruned Table

	header_line = pruned_table[0]
	
	if column_list[0] == "*" and agg_retval == "0": 
		return pruned_table #If it's select * then just return all columns

	#If not *, Run Select on this to get only required columns 
	if agg_retval != "0": #There is an aggregate function in the string
		column_list = [agg_retval.replace("("," ").replace(")"," ").split(" ")[1]] #replacing brackets with spaces and giving column name]

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
if len(sys.argv) < 2:
	print("Error: Give SQL Query as argument!")
	sys.exit(1)

sql_query = sys.argv[1]

metadata_all = {} #dictionary where key is tablename and value is TableMetadata object
metadata_all = read_metadata(metadata_all)

#Query Parsing
parsed = sqlparse.parse(sql_query)[0]

distinct_flag = 0

for token in parsed.tokens:
	if "DISTINCT" in token.value.upper():
		distinct_flag = 1
 
if distinct_flag == 1: #Remove distinct from query -- Later we remove duplicates
	distinct_remove = re.compile(re.escape("distinct"), re.IGNORECASE)
	sql_query = distinct_remove.sub("", sql_query)

	parsed = sqlparse.parse(sql_query)[0]

column_list = SQLParser.parse_sql_columns(sql_query)
table_list = SQLParser.parse_sql_tables(sql_query)

if len(column_list) == 0:
	column_list = ["*"]

#Checking if Aggregate Function in Query
agg_retval = check_aggregate(parsed.tokens)

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
		final_output = select_query(metadata_all, table_list[0], column_list, agg_retval)
	else: #Multiple tables
		final_output = multiple_table_select(metadata_all, table_list, column_list)

else: 

	#PROCESSING WHERE
	if len(parsed_where) > 0:

		final_output = where_query(metadata_all, table_list, parsed_where, column_list, agg_retval)

#PROCESSING AGGREGATE FUNCTION 
if agg_retval != "0": 
	final_output = aggregate_query(final_output, agg_retval)

#PROCESSING DISTINCT
if distinct_flag == 1:
	new_final_output = []
	for data_tuple in final_output:
		if data_tuple not in new_final_output:
			new_final_output.append(data_tuple)

	final_output = new_final_output
#PRINT FINAL OUTPUT
print_output(final_output)
