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
					columns.append(identifier.get_real_name())
			if isinstance(token, Identifier):
				columns.append(token.get_real_name())
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


#MAIN

metadata_all = {} #dictionary where key is tablename and value is TableMetadata object
metadata_all = read_metadata(metadata_all)

final_output = []

#Query Parsing
sql_query = "Select A,B,C from table1, table2 Where table1.B=table2.B"
sql_query = "Select average(B) from table1"
#print(sqlparse.format(sql_query, reindent=True, keyword_case='upper'))

parsed = sqlparse.parse(sql_query)[0]

column_list = SQLParser.parse_sql_columns(sql_query)
table_list = SQLParser.parse_sql_tables(sql_query)

if(len(column_list) == 0):
	column_list = ["*"]

#for x in sqlparse.sql.IdentifierList(parsed).get_identifiers():
#	print(x)

#FOR SELECT QUERIES

#Checking if Where exists in Query
where_flag = 0

for token in parsed.tokens:
	if "WHERE" in token.value.upper():
		where_flag = 1
		break

if(where_flag == 1):
	print("Where Query!")


else: #No Where Present in Query
	retval = check_aggregate(parsed.tokens)

	if(retval == "0"): #No Aggregate Function Present in Query, i.e. Normal Select Query
		ret_table = select_query(metadata_all, table_list[0], column_list)
		final_output = ret_table

	else: #Aggregate Function Present
		#print(retval)
		column_list = []
		agg_string = retval.replace("("," ").replace(")"," ").split(" ") #replacing brackets with spaces

		agg_func = agg_string[0]
		column_list.append(agg_string[1])

		ret_table = select_query(metadata_all, table_list[0], column_list) #Get the column

		header_line = str(agg_func + "(" + ret_table[0][0] + ")")
		
		final_output.append([header_line])

		numbers_data = [ int(x[0]) for x in ret_table[1:] ] #excluding Header Line
		
		if agg_func == "MAX":
			final_output.append([str(max(numbers_data))])
		
		elif agg_func == "MIN":
			final_output.append([str(min(numbers_data))])

		elif agg_func == "SUM":
			final_output.append([str(sum(numbers_data))])

		elif agg_func == "AVERAGE":
			avg = sum(numbers_data) / len(numbers_data)
			final_output.append([str(avg)])

		else:
			print("ERROR in Aggregate Functions")
			exit(EXIT_FAILURE)


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
