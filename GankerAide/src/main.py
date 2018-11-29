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

target_host = os.getenv('DB_HOST', 'localhost')
client = MongoClient(host=target_host, port=27017)
db: Database = client.league_matches

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

    db_stats: Collection = db.team_stats

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

            team_1_hash = "{}-{}-100".format(new_match.id, new_match.platform.value)
            team_2_hash = "{}-{}-200".format(new_match.id, new_match.platform.value)

            if db_stats.find({"_id": team_1_hash}).count() == 0:

                team_1_stats = {
                    "_id": team_1_hash,
                    "team_kills": 0,
                    "team_deaths": 0,
                    "team_assists": 0,
                    "team_kda": 0,
                    "team_cs": 0,
                    "team_income": 0,
                    "team_spending": 0,
                    "team_wards_used": 0,
                    "team_wards_denied": 0,
                    "team_v_wards_bought": 0,
                    "team_s_wards_bought": 0,
                    "team_wards_bought": 0,
                    "team_vision_score": 0,
                }

                team_2_stats = {
                    "_id": team_2_hash,
                    "team_kills": 0,
                    "team_deaths": 0,
                    "team_assists": 0,
                    "team_kda": 0,
                    "team_cs": 0,
                    "team_income": 0,
                    "team_spending": 0,
                    "team_wards_used": 0,
                    "team_wards_denied": 0,
                    "team_v_wards_bought": 0,
                    "team_s_wards_bought": 0,
                    "team_wards_bought": 0,
                    "team_vision_score": 0,
                }

                for participant in new_match.participants:
                    if participant.side.value == 100:
                        process_participant(participant, team_1_stats, new_match)
                    elif participant.side.value == 200:
                        process_participant(participant, team_2_stats, new_match)

                    if participant.summoner.id not in pulled_summoner_ids and participant.summoner.id not in unpulled_summoner_ids:
                        unpulled_summoner_ids.add(participant.summoner.id)

                result = db_stats.insert_many([team_1_stats, team_2_stats])
                print("Created: {}".format(result.inserted_ids))
            unpulled_match_ids.remove(new_match_id)
            pulled_match_ids.add(new_match_id)

    client.close()


def process_participant(participant: Participant, team_stats, match):
    summoner: Summoner = participant.summoner
    champion: Champion = participant.champion
    stats: ParticipantStats = participant.stats
    hash = "{name}-{summid}-{match_id}-{region}".format(name=summoner.sanitized_name, summid=summoner.id, match_id=match.id, region=summoner.platform.value)

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
        "{}_id".format(role): hash,
        "{}_rank".format(role): rank,
        "{}_champion_name".format(role): champion.name,
        "{}_champion_id".format(role): champion.id,
        "{}_kills".format(role): stats.kills,
        "{}_deaths".format(role): stats.deaths,
        "{}_assists".format(role): stats.assists,
        "{}_kda".format(role): stats.kda,
        "{}_cs".format(role): stats.total_minions_killed,
        "{}_income".format(role): stats.gold_earned,
        "{}_spending".format(role): stats.gold_spent,
        "{}_wards_used".format(role): stats.wards_placed,
        "{}_wards_denied".format(role): stats.wards_killed,
        "{}_v_wards_bought".format(role): stats.vision_wards_bought_in_game,
        "{}_s_wards_bought".format(role): stats.sight_wards_bought_in_game,
        "{}_wards_bought".format(role): stats.sight_wards_bought_in_game + stats.vision_wards_bought_in_game,
        "{}_vision_score".format(role): stats.vision_score,
        "{}_lane".format(role): participant.lane.name if participant.lane is not None else None,
        "{}_spell_d".format(role): participant.summoner_spell_d.name,
        "{}_spell_f".format(role): participant.summoner_spell_f.name,
        "{}_items".format(role): [item.id if item is not None else None for item in stats.items],
        "{}_perks".format(role): [perk.id for perk in list(participant.runes.keys())],
    }
    for i, frame in enumerate(participant.timeline.frames[:-1]):
        key = "{}_{}_minutes_".format(role, i)
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

        team_stats.update(participant_stats)
        team_stats["team_kills"] += participant_stats["{}_kills".format(role)]
        team_stats["team_deaths"] += participant_stats["{}_deaths".format(role)]
        team_stats["team_assists"] += participant_stats["{}_assists".format(role)]
        team_stats["team_kda"] += participant_stats["{}_kda".format(role)]
        team_stats["team_cs"] += participant_stats["{}_cs".format(role)]
        team_stats["team_income"] += participant_stats["{}_income".format(role)]
        team_stats["team_spending"] += participant_stats["{}_spending".format(role)]
        team_stats["team_wards_used"] += participant_stats["{}_wards_used".format(role)]
        team_stats["team_wards_denied"] += participant_stats["{}_wards_denied".format(role)]
        team_stats["team_v_wards_bought"] += participant_stats["{}_v_wards_bought".format(role)]
        team_stats["team_s_wards_bought"] += participant_stats["{}_s_wards_bought".format(role)]
        team_stats["team_wards_bought"] += participant_stats["{}_wards_bought".format(role)]
        team_stats["team_vision_score"] += participant_stats["{}_vision_score".format(role)]
        team_stats["win"] = stats.win
        return participant_stats


if __name__ == "__main__":
    collect_matches()
