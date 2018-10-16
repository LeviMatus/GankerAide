import cassiopeia as cass
import logging
import arrow
import random
import os
import re

from datetime import datetime
from cassiopeia.core import Summoner, MatchHistory, Match, Champion
from cassiopeia import Queue, Patch
from cassiopeia.core.match import Participant, ParticipantStats
from sortedcontainers import SortedList
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

target_host = os.getenv('DB_HOST', '')
client = MongoClient(host=target_host, port=27017)
db: Database = client.league_time

cass.set_riot_api_key("")

logging.basicConfig(level=logging.INFO, filename="gankeraidelog.out", format='%(levelname)s: %(message)s')

curr_time = datetime.now()
logging.info(
    "{}-{}-{} {}:{} STARTING MINING OPERATION".format(curr_time.year, curr_time.month, curr_time.day, curr_time.hour,
                                                      curr_time.minute))


def filter_match_history(summoner, patch):
    end_time = patch.end
    if end_time is None:
        end_time = arrow.now()
    match_history = MatchHistory(summoner=summoner, queues={Queue.ranked_flex_fives, Queue.ranked_solo_fives}, begin_time=patch.start, end_time=end_time)
    return match_history


def collect_matches():
    initial_summoner_name = ""
    region = "NA"

    summoner = Summoner(name=initial_summoner_name, region=region)
    patch = Patch.from_str("8.9", region=region)

    unpulled_summoner_ids = SortedList([summoner.id])
    pulled_summoner_ids = SortedList()

    unpulled_match_ids = SortedList()
    pulled_match_ids = SortedList()

    while unpulled_summoner_ids:
        logging.info(len(unpulled_summoner_ids), "to go.")
        new_summoner_id = random.choice(unpulled_summoner_ids)
        new_summoner = Summoner(id=new_summoner_id, region=region)
        matches = filter_match_history(new_summoner, patch)
        unpulled_match_ids.update([match.id for match in matches])
        unpulled_summoner_ids.remove(new_summoner_id)
        pulled_summoner_ids.add(new_summoner_id)

        while unpulled_match_ids:
            new_match_id = random.choice(unpulled_match_ids)
            new_match = Match(id=new_match_id, region=region)
            for participant in new_match.participants:
                save_participant(participant, new_match)
                if participant.summoner.id not in pulled_summoner_ids and participant.summoner.id not in unpulled_summoner_ids:
                    unpulled_summoner_ids.add(participant.summoner.id)
            unpulled_match_ids.remove(new_match_id)
            pulled_match_ids.add(new_match_id)

    client.close()


def save_participant(participant: Participant, match):
    db_stats: Collection = db.player_stats

    summoner: Summoner = participant.summoner
    champion: Champion = participant.champion
    stats: ParticipantStats = participant.stats
    hash = "{name}-{summid}-{match_id}-{region}".format(name=summoner.sanitized_name, summid=summoner.id, match_id=match.id, region=summoner.platform.value)
    if db_stats.find({"_id": hash}).count() == 0:

        if match.queue == Queue.ranked_solo_fives:
            rank_dict = summoner.ranks.get(Queue.ranked_solo_fives, None)
            if rank_dict is not None:
                rank = "{}-{}".format(rank_dict.tier.value, rank_dict.division.value)
            else:
                rank = 'UNRANKED'
        else:
            rank_dict = summoner.ranks.get(Queue.ranked_flex_fives, None)
            if rank_dict is not None:
                rank = "{}-{}".format(rank_dict.tier.value, rank_dict.division.value)
            else:
                rank = 'UNRANKED'

        if participant.role is not None:
            if isinstance(participant.role, str):
                role = participant.role
            else:
                role = participant.role.value
        else:
            role = None

        participant_stats = {
            "_id": hash,
            "rank": rank,
            "champion_name": champion.name,
            "champion_id": champion.id,
            "kills": stats.kills,
            "deaths": stats.deaths,
            "assists": stats.assists,
            "kda": stats.kda,
            "cs": stats.total_minions_killed,
            "income": stats.gold_earned,
            "spending": stats.gold_spent,
            "wards_used": stats.wards_placed,
            "wards_denied": stats.wards_killed,
            "v_wards_bought": stats.vision_wards_bought_in_game,
            "s_wards_bought": stats.sight_wards_bought_in_game,
            "wards_bought": stats.sight_wards_bought_in_game + stats.vision_wards_bought_in_game,
            "vision_score": stats.vision_score,
            "lane": participant.lane.name if participant.lane is not None else None,
            "role": role,
            "team": participant.side.value,
            "spell_d": participant.summoner_spell_d.name,
            "spell_f": participant.summoner_spell_f.name,
            "items": [item.id if item is not None else None for item in stats.items],
            "perks": [perk.id for perk in list(participant.runes.keys())],
        }
        for i, frame in enumerate(participant.timeline.frames[:-1]):
            key = "{}_minutes_".format(i)
            participant_stats["{}{}".format(key, "cs")] = frame.creep_score
            participant_stats["{}{}".format(key, "ncs")] = frame.neutral_minions_killed
            participant_stats["{}{}".format(key, "current_gold")] = frame.current_gold
            participant_stats["{}{}".format(key, "gold_earned")] = frame.gold_earned
            participant_stats["{}{}".format(key, "x")] = frame.position.x
            participant_stats["{}{}".format(key, "y")] = frame.position.y

            event_types = ["ITEM_PURCHASED", "ITEM_SOLD", "ITEM_DESTROYED", "ITEM_UNDO", "CHAMPION_KILL"]
            events = (event for event in participant.timeline.events if event.type in event_types and event.timestamp.seconds < i*60)
            shop_stack = []
            kill_count = death_count = assist_count = purchase_count = sell_count = item_destroyed_count = 0
            for event in events:
                if event.type == "ITEM_PURCHASED":
                    participant_stats["{}purchase-{}".format(key, purchase_count)] = event.item_id
                    shop_stack.append("{}purchase-{}".format(key, purchase_count))
                    purchase_count += 1

                elif event.type == "ITEM_SOLD":
                    participant_stats["{}sold-{}".format(key, sell_count)] = event.item_id
                    shop_stack.append("{}sold-{}".format(key, purchase_count))
                    sell_count += 1

                elif event.type == "ITEM_DESTROYED":
                    participant_stats["{}item_destroyed-{}".format(key, item_destroyed_count)] = event.item_id
                    item_destroyed_count += 1

                elif event.type == "ITEM_UNDO":
                    previous_key = shop_stack.pop()
                    if re.match(r'purchase', previous_key) is not None:
                        purchase_count -= 1
                        participant_stats.pop(previous_key)
                    elif re.match(r'sold', previous_key) is not None:
                        sell_count -= 1
                        participant_stats.pop(previous_key)

                elif event.type == "CHAMPION_KILL":
                    if event.victim_id == participant.id:
                        participant_stats["{}death-{}_x".format(key, death_count)] = event.position.x
                        participant_stats["{}death-{}_y".format(key, death_count)] = event.position.y
                        death_count += 1
                    elif participant.id in event.assisting_participants:
                        participant_stats["{}assist-{}_x".format(key, assist_count)] = event.position.x
                        participant_stats["{}assist-{}_y".format(key, assist_count)] = event.position.y
                        assist_count += 1
                    else:
                        participant_stats["{}kill-{}_x".format(key, kill_count)] = event.position.x
                        participant_stats["{}kill-{}_y".format(key, kill_count)] = event.position.y
                        kill_count += 1
        participant_stats["win"] = stats.win
        result = db_stats.insert_one(participant_stats)
        logging.info('Created {}'.format(result.inserted_id))
    else:
        logging.info(hash, "Already exists in DB.")


if __name__ == "__main__":
    collect_matches()
