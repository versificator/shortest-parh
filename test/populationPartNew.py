from neo4j.v1 import GraphDatabase, basic_auth
import csv

def mergeRelation(fileName):
  with open(fileName) as f:
    b = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]
    for a in b:
      print a
      createLeg(a)
      createDirectedRelation('Platform', a['originSpot']+'_out', 'Leg', a['_id'], 'START_AT', '{weight:' + a['netFare'] + '}')
      createDirectedRelation('Leg', a['_id'], 'Platform', a['destinationSpot']+'_in', 'END_AT','')

def mergeNode(fileName):
  with open(fileName) as f:
    b = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]
    for a in b: 
      createStation(a)

      createPlatform(a['_id'] + '_in','Mo')
      createPlatform(a['_id'] + '_in','Tu')
      createPlatform(a['_id'] + '_in','We')
      createPlatform(a['_id'] + '_in','Th')
      createPlatform(a['_id'] + '_in','Fr')
      createPlatform(a['_id'] + '_in','Sa')
      createPlatform(a['_id'] + '_in','Su')

      createPlatform(a['_id'] + '_out','Mo')
      createPlatform(a['_id'] + '_out','Tu')
      createPlatform(a['_id'] + '_out','We')
      createPlatform(a['_id'] + '_out','Th')
      createPlatform(a['_id'] + '_out','Fr')
      createPlatform(a['_id'] + '_out','Sa')
      createPlatform(a['_id'] + '_out','Su')

      createDirectedRelation('Station', a['_id'], 'Platform', a['_id']+ '_in', 'HAS_PLATFORM', '')
      createDirectedRelation('Platform', a['_id']+'_in', 'Station', a['_id'], 'CAN_BOARD', '{weight: 50}')
      createDirectedRelation('Station', a['_id'], 'Platform', a['_id']+ '_out', 'HAS_PLATFORM','')
      createDirectedRelation('Platform', a['_id']+'_out', 'Station', a['_id'], 'CAN_BOARD', '{weight:50}')
#      createDirectedRelation('Platform', a['_id']+'_in', 'Platform', a['_id']+ '_out', 'CAN_TRANSFER_TO', '{weight: 25}')
      print a

def createStation(stationDict):
  print "CREATE (n:Station { SpotSubType: '" + stationDict['SpotSubType'] + "', _id: '" + stationDict['_id'] + "', cityId: '" + stationDict['cityId'] + "', countryCode: '" + stationDict['countryCode'] + "', lat:'" + stationDict['lat']  + "', lon: '" + stationDict['lon'] + "', name: '" + stationDict['name'] + "' });"
  session.run("CREATE (n:Station { SpotSubType: '" + stationDict['SpotSubType'] + "', _id: '" + stationDict['_id'] + "', cityId: '" + stationDict['cityId'] + "', countryCode: '" + stationDict['countryCode'] + "', lat:'" + stationDict['lat']  + "', lon: '" + stationDict['lon'] + "', name: '" + stationDict['name'] + "' });")


def createPlatform(platformID,weekDay):
  print "CREATE (n:Platform { _id : '" +  platformID + "', day : '" + weekDay + "' })"
  session.run("CREATE (n:Platform { _id : '" +  platformID + "', day : '" + weekDay + "' })");
 
def createDirectedRelation(labelFirst, idFirst, labelSecond, idSecond, relationLabel, attributes):
  print "MATCH (n:" + labelFirst + " {_id : '"+  idFirst + "'}),(m:"+ labelSecond +" {_id: '" + idSecond + "'}) CREATE (n)-[r:" + relationLabel + " " + attributes +"]->(m) RETURN r "
  session.run("MATCH (n:" + labelFirst + " {_id : '"+  idFirst + "'}),(m:"+ labelSecond +" {_id: '" + idSecond + "'}) CREATE (n)-[r:" + relationLabel + " " + attributes +"]->(m) RETURN r ")

def createLeg(legDict):
  print "CREATE (n:Leg { departureTime: '" + legDict['departureTime'] + "', _id: '" + legDict['_id'] + "', arrivalTime: '" + legDict['arrivalTime'] + "', duration: '" + legDict['duration'] + "', netFare:'" + legDict['netFare']  + "', route: '" + legDict['route'] + "' });"
  session.run("CREATE (n:Leg { departureTime: '" + legDict['departureTime'] + "', _id: '" + legDict['_id'] + "', arrivalTime: '" + legDict['arrivalTime'] + "', duration: '" + legDict['duration'] + "', netFare:'" + legDict['netFare']  + "', route: '" + legDict['route'] + "' });")



driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "zephyr"))

session = driver.session()

mergeNode('nodes.csv')
mergeRelation('relations.csv')

session.close()
