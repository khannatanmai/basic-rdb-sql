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
				print("Attribute mentioned in SELECT not found in", table_name)
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
		exit(EXIT_FAILURE)

	return return_output

def multiple_table_select(metadata_all, table_list, query_attributes_list):
	#Calls select for each table and puts them together in one table
	return_table = []

	query_list_dict = {} #Key: TableName, Value: Attributes needed from that Table

	for table_name in table_list:
		query_list_dict[table_name] = [] #Add all the keys, and empty arrays for the columns needed

	for attribute in query_attributes_list:
		if "." in attribute: #If Table Name has been mentioned
			attribute_table = attribute.split(".")[0]

			if attribute_table in query_list_dict:
				query_list_dict[attribute_table].append(attribute.split(".")[1]) #Add column name to dict

			else:
				print("Error: Table mentioned in Query Attribute not recognised!")
				exit(EXIT_FAILURE)

		else: #Find which table this column belongs to
			pass

	for table_name, query_list in query_list_dict.items(): #Creating a combined table by calling select for EACH table

		if len(return_table) < 1: #return_table is empty (first iteration)
			return_table = select_query(metadata_all, table_name, query_list)

		else: #(all other iterations)
			next_table = select_query(metadata_all, table_name, query_list)

			if len(return_table) != len(next_table):
				print("Tables not of same length!")
				exit(EXIT_FAILURE)

			for i in range(len(next_table)):
				return_table[i] = return_table[i] + next_table[i] #Combining all the tables into one output

	return return_table



#MAIN

metadata_all = {} #dictionary where key is tablename and value is TableMetadata object
metadata_all = read_metadata(metadata_all)

#Query Parsing
sql_query = "Select A,table1.B,C,D from table1, table2"
sql_query = "Select table1.A, table1.C,table2.D from table1,table2"
#print(sqlparse.format(sql_query, reindent=True, keyword_case='upper'))

parsed = sqlparse.parse(sql_query)[0]

#for token in parsed.tokens:
	#print(token)

column_list = SQLParser.parse_sql_columns(sql_query)
table_list = SQLParser.parse_sql_tables(sql_query)

if(len(column_list) == 0):
	column_list = ["*"]


#FOR SELECT QUERIES
final_output = []

if(len(table_list) == 1): #If only one table in query
	final_output = select_query(metadata_all, table_list[0], column_list)
else: #Multiple tables
	final_output = multiple_table_select(metadata_all, table_list, column_list)

#Checking if Where exists in Query
where_flag = 0

for token in parsed.tokens:
	if "WHERE" in token.value.upper():
		where_flag = 1
		break

if(where_flag == 1):
	print("Where Query!")
	#TODO

#Checking if Aggregate Function in Query
retval = check_aggregate(parsed.tokens)
if(retval != "0"): #AGGREGATE FUNCTION PROCESSING
	#print(retval)
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
