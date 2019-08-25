import sys

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
					print "Syntax Error in metadata.txt"
	except IOError:
		print "No Metadata file (Should be named metadata.txt)"
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
		print "No csv file found for table:", table_name, "in the database."
		sys.exit(1)
		#ERROR

	return table_data

def select_query(metadata_all, table_name, table_data, query_attributes_list):
	return_table = []

	table_attributes_list = metadata_all[table_name].attributes

	if query_attributes_list[0] == "*": #If returning all columns of table
		header_line = table_attributes_list

		return_table.append(header_line)

		for data_tuple in table_data:
			return_table.append(data_tuple)

	else:

		#Verification that all columns given exist in table
		for query_attribute in query_attributes_list:
			if query_attribute not in table_attributes_list:
				print "Attribute mentioned in SELECT not found in", table_name
				sys.exit(1)
				#ERROR

		#now we need the indices to print only the columns we need from the csv file
		indices_list = []
		header_line = []

		index_count = 0
		for table_attribute in table_attributes_list:
			if table_attribute in query_attributes_list:
				indices_list.append(index_count)
				header_line.append(table_attribute)

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
		print ",".join(line)
#MAIN

metadata_all = {} #dictionary where key is tablename and value is TableMetadata object
metadata_all = read_metadata(metadata_all)
table_name = "table1"
table_data = read_table(table_name)

return_table = select_query(metadata_all, table_name, table_data, ["A","B","C","D"])

print_output(return_table)

#print(table_data)

#METADATA TESTING
'''
for table_name, table_metadata in metadata_all.items():
	print "Table Name:", table_name

	print "Attributes:"

	print table_metadata.attributes

	print "***"
'''
