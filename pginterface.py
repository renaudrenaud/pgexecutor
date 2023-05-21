"""
Managing PG connections
using psycopg2

to run the requirments.txt file use the following command:
pip install -r requirements.txt

"""

from typing import List
import psycopg2


class PG:
    """
    This class to manage PostGres operations from python
    
    """

    def __init__(self, database_uri: str) -> None:
        """
        firing the conn
        using databse_uri
        """

        self.__version__ = '1.0.0'
        self.conn = None
        print("Postgres interface version " + self.__version__ + " is starting")

        try:
            self.conn = psycopg2.connect(database_uri)
        except psycopg2.Error as e:
            print(f'Error: databse connexion has failed: {e}')
            raise e
        
    def clsSelect(self, strRequest: str)-> List:    
        """
        Return a list of tupples from PG
        Corresponding to the request
        ** Input
        * strRequest: string, ie select now()

        ** Return
        * List of tuples or Empty List
        """

        returnValues = []

        try:
            cur = self.conn.cursor()
            cur.execute(strRequest)
            returnValues = cur.fetchall()
        except (Exception, psycopg2.Error) as error:
            print('error on select using ' + strRequest + " ->" + str(error))
            
        finally:
            if cur:
                cur.close()

        return returnValues

    def clsExecute(self, strRequest):
        """
        return the number of affected lines
        """
        returnValue = 0

        try:
            cur = self.conn.cursor()
            cur.execute(strRequest)
            self.conn.commit()
            returnValues = cur.rowcount
        except (Exception, psycopg2.Error) as error:
            print('error on select using ' + strRequest + " ->" + str(error))
            
        finally:
            self.conn.commit
            if cur:
                cur.close()
        return returnValues
    
    def clsVersion(self):
        """
        return db version

        ** return
        - typle when ok
        - None otherwise

        """

        cur = self.conn.cursor()

        try:
            cur.execute('select version()')
            db_version = cur.fetchone()
        except (Exception, psycopg2.Error) as error:
            print('Error trying to get version', str(error))
        finally:
            if (cur):
                cur.close()
            return db_version

    def __del__(self):
        """
        Close the connexion

        """
        if self.conn is not None:
            self.conn.close()
            print('Database conn is closed now!')
        
        print("Postgres interface version " + self.__version__ + " end ")

if __name__ == '__main__':
    someUri = "postgresql://postgres:postgres@192.168.1.120:5432"

    mybase = None
    try:
        mybase = PG(someUri)
        print(str(mybase.clsVersion()))
        print(str(mybase.clsExecute("select now()")))
    except:
        print("Oulala")
    