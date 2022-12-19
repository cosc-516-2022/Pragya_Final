from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv
from datetime import datetime


class CassandraDB():

    def connect(self):
        #TODO: connect to database
        cloud_config= {
         'secure_connect_bundle': 'secure-connect-cosc516.zip'}
        auth_provider = PlainTextAuthProvider("QmBOhHZyqXeHdHyxOfCIfkyP",  "UZHMFNwuHTCI5Gcv0ovrgG6vH9YR0sFZjj0ZNtZ9gAiTTOd,XG4M2fSW15u-ibnpf25lWAugag+IrR.Luog7u.kmeOnfnnsGImINGEddg1Rkg3BGjKcn.RrvkipC,8QQ")
        cluster = Cluster(cloud=cloud_config,auth_provider=auth_provider)
        session = cluster.connect()
        return session

    def create_table(self):
        query="CREATE TABLE IF NOT EXISTS pragya.gamestate ( id int, statetime timestamp, region int, name text, email text, gold int, power int, level int, number_of_games int,\
             PRIMARY KEY((region),level,number_of_games)) WITH CLUSTERING ORDER BY (level DESC, number_of_games DESC);"
        session=self.connect()
        session.execute(query)

        query="CREATE TABLE IF NOT EXISTS pragya.gameevent ( eventid int, userid int, eventtime timestamp, type int, diffgold int, diffpower int, difflevel int, PRIMARY KEY(userid,eventid));"
        session.execute(query)

    def load_data(self):
        #TODO: load customer.csv into the table created
        session=self.connect()
        prepared = session.prepare("""
        INSERT INTO pragya.gamestate(id, statetime, region, name, email, gold, power, level, number_of_games)
        VALUES (?, ?, ?, ?,?,?,?,?,?)
        """)
        with open("gamestate.csv", "r") as customers:
            reader = csv.DictReader(customers)
            for customer in reader:
                date = datetime.strptime(customer['statetime'], '%Y-%m-%d %H:%M:%S')
                id=int(customer['id'])
                region=int(customer['region'])
                gold=int(customer['gold'])
                power=int(customer['power'])
                level=int(customer['level'])

                #Calculating the number of games each user has played
                res=session.execute("select count(*) from pragya.gameevent where userid ="+customer['id']+";")
                session.execute(prepared,[id,date,region,customer['name'],customer['email'],gold,power,level,res[0].count])
        customers.close()

    def load_event(self):
        #TODO: load customer.csv into the table created
        session=self.connect()
        prepared = session.prepare("""
        INSERT INTO pragya.gameevent(eventid, userid, eventtime, type, diffgold, diffpower, difflevel)
        VALUES (?, ?, ?, ?,?,?,?)
        """)
        with open("gameevent.csv", "r") as events:
            reader = csv.DictReader(events)
            for event in reader:
                date = datetime.strptime(event['eventtime'], '%Y-%m-%d %H:%M:%S')
                id=int(event['eventid'])
                userid=int(event['userid'])
                type=int(event['type'])
                gold=int(event['diffgold'])
                power=int(event['diffpower'])
                level=int(event['difflevel'])
                session.execute(prepared,[id,userid,date,type,gold,power,level])
        events.close()

    def update(self,newstate):

        query="select * from pragya.gamestate where id=11 allow filtering;"
        session=self.connect()
        res=session.execute(query)
        res=res[0]
        #First deleting the existing row
        query="Delete from pragya.gamestate where region="+str(newstate["region"])+" and level="+str(newstate["oldlevel"])+" and number_of_games=14 IF EXISTS;"
        session=self.connect()
        session.execute(query)
        
        #Inserting the updated one
        date=datetime.strptime(newstate['new_time'], '%Y-%m-%d %H:%M:%S')
        prepared = session.prepare("""
        INSERT INTO pragya.gamestate(id, statetime, region, name, email, gold, power, level, number_of_games)
        VALUES (?, ?, ?, ?,?,?,?,?,?)
        """)
        session.execute(prepared,[res.id,date,res.region,res.name,res.email,newstate["new_gold"],newstate["new_power"],newstate["new_level"],res.number_of_games])
        

    def query_1(self,id):
        query="Select * from pragya.gamestate where id="+str(id)+" allow filtering;"
        session=self.connect()
        row=session.execute(query)
        print(row[0])
        return row

    def query_2(self,region):
       
        query="Select name,level from pragya.gamestate where region="+str(region)+" limit 10 ALLOW FILTERING;"
        session=self.connect()
        q2=session.execute(query)
        q2=list(q2)
        print(q2)
        return q2

    def query_3(self,region,datestart,dateend):
       
        query1="Select userid from pragya.gameevent where eventtime >= '"+datestart+"' \
            and eventtime<= '"+dateend+"' allow filtering;"
        session=self.connect()
        res=session.execute(query1)
        userids=[]
        temp=[]
        q3={}
        for each in res:
            temp.append(each.userid)
            [userids.append(x) for x in temp if x not in userids]
        for id in userids:
            query2="Select name,number_of_games from pragya.gamestate where id ="+str(id)+" and region="+str(region)+" allow filtering;"
            res=session.execute(query2)
            if(res.one()!=None):
                q3[res.one().name]=res.one().number_of_games
        result = sorted(q3, key=q3.get, reverse=True)[:5]
        print(result)
        return result


if __name__ == '__main__':
    client = CassandraDB()
    client.connect()
    client.create_table()
    #client.load_event()
    #client.load_data()
    gamestate={"id":11, "region":2,"oldlevel":47, "new_gold":620000,"new_level":47,"new_power":600000,"new_time":"2022-12-21 19:01:33" }
    client.update(gamestate)
    client.query_1(1)
    client.query_1(949)
    client.query_2(1)
    client.query_2(9)
    client.query_3(2,'2022-12-17 05:30:00','2022-12-17 15:00:00')
    client.query_3(7,'2022-12-18 00:00:00','2022-12-19 11:00:00')