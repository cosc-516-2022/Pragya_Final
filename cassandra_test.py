import unittest

from Cassandra import CassandraDB

class CassandraTest(unittest.TestCase):

    def setUp(self) -> None:
        print()
        print('*'*5 + 'Cassandra test begin' + '*'*5)
        self.client = None

    def test_connect(self):
        self.client = CassandraDB()
        # test
        self.assertIsNotNone(self.client, None)

    def test_load_data1(self):
        #Tests for create and load
        self.client = CassandraDB()
        session=self.client.connect()
        #self.client.create_table()
        #self.client.load_data()

        query_statement = "select * from pragya.gameevent;"
        res = session.execute(query_statement)
        self.assertEqual(len(res.all()), 10000)

    def test_load_data2(self):
        #Tests for create and load
        self.client = CassandraDB()
        session=self.client.connect()
        #self.client.create_table()
        #self.client.load_data()

        query_statement = "select * from pragya.gamestate;"
        res = session.execute(query_statement)
        self.assertEqual(len(res.all()), 1000)
    
    def test_update(self):
        self.client = CassandraDB()
        session=self.client.connect()
        gamestate={"id":11, "region":2,"oldlevel":47, "new_gold":630000,"new_level":47,"new_power":610000,"new_time":"2022-12-24 19:01:33" }
        self.client.update(gamestate)
        query="select * from pragya.gamestate where id=11 allow filtering;"
        res = session.execute(query)
        self.assertEqual(str(list(res)),"[Row(region=2, level=47, number_of_games=14, email='email11@abc.com', gold=630000, id=11, name='Name#11', power=610000, statetime=datetime.datetime(2022, 12, 24, 19, 1, 33))]")

    def test_query_1a(self):
        self.client = CassandraDB()
        self.client.connect()
        res = self.client.query_1(1)
        self.assertEqual(str(res[0]), "Row(id=1, region=2, level=21, number_of_games=13, email='email1@abc.com', gold=56607, name='Name#1', power=248393, statetime=datetime.datetime(2022, 12, 19, 15, 23, 7))")

    def test_query_1b(self):
        self.client = CassandraDB()
        self.client.connect()
        res = self.client.query_1(949)
        self.assertEqual(str(res[0]), "Row(id=949, region=3, level=12, number_of_games=9, email='email949@abc.com', gold=843488, name='Name#949', power=949991, statetime=datetime.datetime(2022, 12, 20, 22, 59, 56))")

    def test_query_2a(self):
        self.client = CassandraDB()
        self.client.connect()
        res = self.client.query_2(1)
        self.assertCountEqual(str(res),"[Row(name='Name#970', level=49),"
        +" Row(name='Name#638', level=49)," 
        +" Row(name='Name#855', level=48),"
        +" Row(name='Name#547', level=48),"
        +" Row(name='Name#260', level=47),"
        +" Row(name='Name#478', level=47),"
        +" Row(name='Name#322', level=47),"
        +" Row(name='Name#30', level=47),"
        +" Row(name='Name#756', level=46),"
        +" Row(name='Name#182', level=46)]")
    
    def test_query_2b(self):
        self.client = CassandraDB()
        self.client.connect()
        res = self.client.query_2(9)
        self.assertCountEqual(str(res),"[Row(name='Name#243', level=49), Row(name='Name#765', level=49), Row(name='Name#879', level=49), Row(name='Name#715', level=48), Row(name='Name#367', level=48), Row(name='Name#43', level=48), Row(name='Name#192', level=47), Row(name='Name#269', level=46), Row(name='Name#934', level=45), Row(name='Name#754', level=45)]")

    def test_query_3a(self):
        self.client = CassandraDB()
        self.client.connect()
        res = self.client.query_3(2,'2022-12-17 05:30:00','2022-12-17 15:00:00')
        self.assertCountEqual(str(res),"['Name#883', 'Name#353', 'Name#391', 'Name#797', 'Name#328']")

    def test_query_3b(self):
        self.client = CassandraDB()
        self.client.connect()
        res = self.client.query_3(7,'2022-12-18 00:00:00','2022-12-19 11:00:00')
        self.assertCountEqual(str(res),"['Name#503', 'Name#893', 'Name#634', 'Name#119', 'Name#916']")

if __name__ == "__main__":
    unittest.main()