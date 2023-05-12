import logging
import sys

from src.mongomancy import Engine, Database, CollectionDefinition, Index

engine = Engine("localhost", 27017)
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[logging.StreamHandler(stream=sys.stdout)],
)
logger = logging.getLogger()

print(f"ping after start = {engine.ping()}")
engine.dispose()
print(f"ping after dispose = {engine.ping()}")

db = Database("gaming", engine=engine, logger=logger)
game = CollectionDefinition(name="game", indices=[Index(fields={"genre": 1})])
player = CollectionDefinition(name="player", indices=[Index(fields={"player_id": 1}, unique=True)])
db.add_collection(game)
db.add_collection(player)

db.create_all()
docs = list(db["game"].find({"genre": "adventure"}))
print(f"adventure games: {docs}")
if not docs:
    db["game"].insert_one({"_id": "tails_of_iron", "genre": "adventure"})
    docs = list(db["game"].find({"genre": "adventure"}))
    print(f"adventure games: {docs}")
engine.dispose()
print(f'adventure game: {db["game"].find_one({"genre": "adventure"})}')
