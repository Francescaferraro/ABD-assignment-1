import sqlite3 
import os
from dotenv import load_dotenv
load_dotenv('config.env') #config variables are saved into an external file and loaded here, for reusability and security reasons

from abc import ABC, abstractmethod

class AbstractConnection(ABC) :
	@abstractmethod
	def getData(self, query) : pass
	@abstractmethod
	def writeData(self, query, data) : pass
	#to prevent SQL injection, every input parameter needs to be validated
	@abstractmethod
	def validateInput(self, input) : pass


#wrapper class to avoid vendor lock in : if we change the sql library, we only need to modify this class
class Connection(AbstractConnection) :
  # we use a configuration file to inizialize the connection
  def __init__(self) :
    db_name = os.getenv("DB")
    con = sqlite3.connect(db_name)
    self.__connection = con
    self.__cursor = con.cursor()
  def getData(self, query) :
    self.__cursor.execute(query)
  def validateInput(self, input) : pass
  def writeData(self, query, data) :
  	if self.validateInput(data) :
  	    self.__cursor.execute(query, data)
  	else : raise ValueError("Input non valido")


class AbstractUploader(ABC) :
	@abstractmethod
	def get_data(self, query) : pass
	@abstractmethod
	def write_data(self, query, data) : pass	

#Uploader class recalls configuration methods from Connection class; these two classes has been separated for reusability and clearessness reasons
class Uploader(AbstractUploader) :
  def __init__(self) :
    self.__connection = Connection()
  def get_data(self, query) :
  	self.__connection.getData(query)
  def write_data(self, query, data) :
  	self.__connection.writeData(query, data)

#main class specifies the queries and recalls the previous classes
class Main :
  def main(self) :
    get_query = '''
      SELECT *
      FROM T
      WHERE Column1 > Column2
    '''
    write_query = '''
      INSERT INTO T(Column1, Column2) VALUES (?,?)
    '''
    data = ["Value1", "Value2"]

    batch_uploader = Uploader()
    batch_uploader.get_data(get_query)
    batch_uploader.write_data(write_query, data)


#since we use a fake input, the program throws a ValueError exception
extractor = Main()
extractor.main()