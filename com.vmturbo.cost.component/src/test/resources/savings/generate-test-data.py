#!/usr/bin/env python3

"""Generate cloud savings/investment value for the savings database.

This tool is driven by an event list that is modeled after the similar XL events:

- Timestamp, Entity created (TEP), entity name, compute tier
- Timestamp, Entity removed (TEP), entity name
- Timestamp, Provider change (TEP), entity name, new provider
- Timestamp, Action executed (AO), entity name
- Timestamp, Resize recommendation (A0), entity name, new provider

The timestamp format is:

  day hh:mm:ss

Events that occur at the top of the hour occur after that hour's processing pass.  For
example, a VM create event at 3:00 will be processed at 4:00.  All timestamps are relative to the start of the
scenario, as defined by any start command.

The events are:

- start - start the scenario.  This is optional.  If absent, the first event serves as the implicit start.  If a
          date is provided, that serves at the real-time start of the generated data.  This time is rounded down
          to the nearest hour.
- create - a new entity was detected.  This is not required at this time.  Resize recommendations create the entities.
- remove - an existing entity was removed
- power - an entity changed power state
- change - an entity resize was detected via discovery
- resize - a resize recommendation was generated
- execute - an action was successfully executed
- stop - stop the scenario.  If this is absent, the last event serves as the implicit end.

It is assumed that the savings processor runs at the top of the hour.

In addition, a compute tier to cost map is defined, which will be used as a stub cost journal.

tier_to_code[tier_name] = cost.  The events can either refer to a tier or to a cost directly.

Entities can be referred to by name.  The conversion process will use a numeric hash as the UUID in the database table.

All text starting with # is ignored.

# Example scenario documented in the wiki
0 0:00 start
0 4:15 create VM-A t3.medium  # This is a comment
0 4:15 power VM-A on
1 10:15 resize VM-A t3.medium t3.nano (note resize from X -> X indicates a recommendation disappeared)
4 6:30 power VM-A off
4 8:15 power VM-A on
10 5:30 remove VM-A

"""

import logging
import sys
import argparse
from time import time
from datetime import datetime

# Definitions from the protobuf files
# BCTODO compile the protobuf files for these definitions.


class EntitySavingsStatsType:
    REALIZED_SAVINGS = 1
    REALIZED_INVESTMENTS = 2
    MISSED_SAVINGS = 3
    MISSED_INVESTMENTS = 4
    CUMULATIVE_REALIZED_SAVINGS = 5
    CUMULATIVE_REALIZED_INVESTMENTS = 6
    CUMULATIVE_MISSED_SAVINGS = 7
    CUMULATIVE_MISSED_INVESTMENTS = 8


class EntityType:
    VIRTUAL_MACHINE = 10
    VIRTUAL_VOLUME = 60


ONE_HOUR_IN_MS = 3600 * 1000
SQL_INSERT_CHUNK_SIZE = 100

TIER_TO_COST = {
    'tier-1': 2.00,
    't3.nano': 1.00,
    't3.medium': 4.00
}

args = None
vm_to_uuid_map = {}
sql_output = []
base_timestamp = 0


class InternalState:
    def __init__(self):
        self.entity_state = {}

    def create_entity(self, event, entity_type):
        entity_name = event.get_entity_name()
        self.entity_state[entity_name] = entity = EntityState(entity_name, entity_type, event.timestamp)
        return entity

    def get_entity(self, event):
        """Locate saving for an existing entity."""
        entity_name = event.get_entity_name()
        if entity_name in self.entity_state:
            return self.entity_state[entity_name]


class Event:
    commands = {
        'nop': [],
        'start': ['date=now', 'time="0:00"'],
        'stop': [],
        'create': ['vm_name', 'tier', 'state=off'],  # BCTODO this is always a no-op.  Remove it.
        'remove': ['vm_name'],
        'power': ['vm_name', 'state'],
        'resize': ['vm_name', 'source-tier', 'dest-tier'],
        'change': ['vm_name', 'source-tier', 'dest-tier'],
        'execute': ['vm_name', 'source-tier', 'dest-tier'],
        'uuid': ['vm_name', 'uuid']
    }

    def __init__(self, event='nop'):
        self.event = event
        self.timestamp = None
        self.params = {'vm_name': None, 'state': None, 'source-tier': None, 'dest-tier': None}

    def get_entity_name(self):
        return self.params['vm_name']

    def get_state(self):
        return self.params['state']

    def get_source_tier(self):
        return self.params['source-tier']

    def get_dest_tier(self):
        return self.params['dest-tier']

    def __repr__(self):
        return "%d %s %s" % (self.timestamp, self.event, str(self.params))


class Recommendation:
    def __init__(self, event):
        self.source_tier = event.get_source_tier()
        self.dest_tier = event.get_dest_tier()
        self.delta = get_cost(self.dest_tier) - get_cost(self.source_tier)

    def get_savings(self):
        return -min(0.0, self.delta)

    def get_investment(self):
        return max(0.0, self.delta)


class EntityState:
    ACTIVE = 0
    SHUTTING_DOWN = 1

    def __init__(self, vm_name, entity_type, timestamp):
        # Identity
        self.state = EntityState.ACTIVE
        self.vm_name = vm_name
        self.uuid = get_uuid(vm_name)
        self.entity_type = entity_type

        # Transition handling
        self.segment_start = timestamp
        self.power_factor = 1.0  # power is on, so accrue savings/investment at 100%

        # This is used for Algorithm 1 (track total savings and investments since baseline, which is T-0).
        self.baseline = 0.00  # This isn't used for the initial/simple algorithm
        # Tracks the currently active action.  We ignore all size changes other than this one.
        self.current_recommendation = None

        # These are updated on SKU change or action execution

        #
        # Realized
        #
        self.previous_savings = 0.00
        self.previous_investment = 0.00

        self.current_savings = 0.00
        self.current_investment = 0.00

        self.periodic_savings = 0.00
        self.periodic_investment = 0.00

        #
        # Missed
        #
        self.previous_missed_savings = 0.00
        self.previous_missed_investment = 0.00

        self.current_missed_savings = 0.00
        self.current_missed_investment = 0.00

        self.periodic_missed_savings = 0.00
        self.periodic_missed_investment = 0.00

        # These are the values that are written to the savings table. The periodic values
        # are calculated based on the previous cumulative values and the current accumulator.
        self.cumulative_savings = 0.00
        self.cumulative_investment = 0.00
        self.cumulative_missed_savings = 0.00
        self.cumulative_missed_investment = 0.00

    def end_segment(self, timestamp):
        """Close out the current segment in preparation for a transition"""
        segment_length = (timestamp - self.segment_start) / ONE_HOUR_IN_MS
        self.segment_start = timestamp
        self.periodic_savings += self.current_savings * segment_length * self.power_factor
        self.periodic_investment += self.current_investment * segment_length * self.power_factor
        self.periodic_missed_savings += self.current_missed_savings * segment_length * self.power_factor
        self.periodic_missed_investment += self.current_missed_investment * segment_length * self.power_factor

    def end_period(self, timestamp):
        """Close out the current interval.  This resets periodic values and prepares for the next interval."""
        self.end_segment(timestamp)
        self.cumulative_savings += self.periodic_savings
        self.cumulative_investment += self.periodic_investment
        self.cumulative_missed_savings += self.periodic_missed_savings
        self.cumulative_missed_investment += self.periodic_missed_investment

        # Output the savings record
        tstr = datetime.fromtimestamp(timestamp / 1000.0).strftime("%Y-%m-%d %H:%M:%S")

        def generate_entry(entry_type, amount):
            global sql_output
            if amount != 0:
                if args.sql:
                    sql_output += [
                        "(%s,'%s',%d,%f)" % (self.uuid, tstr, entry_type, amount)]
                if args.csv:
                    et = ['', 'RS', 'RI', 'MS', 'MI', 'CRS', 'CRI', 'CMS', 'CMI'][entry_type]\
                        if args.csv2 else str(entry_type)
                    print("%s,%s,%s,%f" % (self.uuid, tstr, et, amount))

        generate_entry(EntitySavingsStatsType.REALIZED_SAVINGS, self.periodic_savings)
        generate_entry(EntitySavingsStatsType.CUMULATIVE_REALIZED_SAVINGS, self.cumulative_savings)
        generate_entry(EntitySavingsStatsType.REALIZED_INVESTMENTS, self.periodic_investment)
        generate_entry(EntitySavingsStatsType.CUMULATIVE_REALIZED_INVESTMENTS, self.cumulative_investment)
        generate_entry(EntitySavingsStatsType.MISSED_SAVINGS, self.periodic_missed_savings)
        generate_entry(EntitySavingsStatsType.CUMULATIVE_MISSED_SAVINGS, self.cumulative_missed_savings)
        generate_entry(EntitySavingsStatsType.MISSED_INVESTMENTS, self.periodic_missed_investment)
        generate_entry(EntitySavingsStatsType.CUMULATIVE_MISSED_INVESTMENTS, self.cumulative_missed_investment)

        self.previous_savings = self.current_savings
        self.previous_investment = self.current_investment
        self.periodic_savings = 0.00
        self.periodic_investment = 0.00
        self.periodic_missed_savings = 0.00
        self.periodic_missed_investment = 0.00

    def shutdown(self, internal_state, timestamp):
        """Flag this entry for removal during next processing pass. Simulate a power off to stop accumulation
        of savings and investments."""
        self.state = EntityState.SHUTTING_DOWN
        event = Event('power')
        event.timestamp = timestamp
        event.params = {'vm_name': self.vm_name, 'state': 'off', 'source-tier': None, 'dest-tier': None}
        handle_power(event, internal_state)


def get_uuid(vm_name):
    """Generate a UUID suitable for use by XL based on the VM name.  Here is an example
    UUID: 73794338510672 (0x431d95555750).  This seems to be a 48 bit value that starts
    at 0x4300 0000 0000.  Let use 0x430000000000 - 0x43FFFFFFFFFF as the range.

    If the entity name is numeric, use it as-is as the UUID.
    """
    if vm_name.isnumeric():
        return vm_name
    elif vm_name in vm_to_uuid_map:
        return vm_to_uuid_map[vm_name]
    else:
        return str(0x430000000000 + (id(vm_name) and 0xFFFFFFFFFF))


def parse_timestamp(fields):
    """Parse (and consume) the first two fields as a timestamp"""
    day = int(fields.pop())
    hm = [int(x) for x in fields.pop().split(':')]
    offset_ms = day * 24 * 60 * 60 * 1000 + hm[0] * 60 * 60 * 1000 + hm[1] * 60 * 1000
    return base_timestamp + offset_ms


def parse_line(raw_line):
    """Parse a line and return a Event"""

    def parse_parameter(parameter):
        f = parameter.split('=')
        argument = f[0]
        return argument, f[1] if len(f) == 2 else None

    event = Event()
    # Remove comments
    line = raw_line.split('#', 1)[0]
    # Extract fields
    fields = line.strip().split()
    fields.reverse()
    if len(fields) == 0:
        # Empty line or line only contains a comment
        return event
    if fields[-1] == 'start' or fields[-1] == 'uuid':
        # Inject a dummy timestamp
        fields += ['0:00', '0']
    if len(fields) < 3:
        # Missing at least event a timestamp
        logging.error("Missing timestamp and/or event in '%s'" % raw_line)
        return event
    event.timestamp = parse_timestamp(fields)
    cmd = fields.pop().lower()
    required_params = Event.commands[cmd]
    try:
        for param in required_params:
            arg, default = parse_parameter(param)
            if len(fields) == 0:
                # A parameter is missing.  If this required parameter has a default value, use it
                if default:
                    event.params[arg] = default
                else:
                    logging.warning("Missing required parameter %s in line '%s'" % (param, raw_line.strip()))
                    return event
            else:
                event.params[arg] = fields.pop()
        # Parse timestamp
    except IndexError:
        logging.error("Missing parameters in line '%s'" % raw_line)
        return event
    event.event = cmd
    return event


def roundup(value, multiple):
    return int((value + multiple - 1) / multiple) * multiple


# New method that is event-driven
# - parse events and send events to the related entity as they are parsed.
# - As each top of hour processing pass is encountered, call process_events().
#   That will iterate over each entity in the internal state table and each will
#   process all events that are queued up on it.
def parse_script(filename):
    internal_state = InternalState()
    last_processing_time = None
    last_ts = 0
    with open(filename, 'r') as fs:
        for line in fs:
            event = parse_line(line)
            if event.event == 'nop':
                continue
            # Generate process events at the top of each hour before inserting
            # the next event
            current_ts = event.timestamp
            if last_ts > current_ts:
                logging.error("ERROR: timestamp of '%s' is before the previous timestamp - skipping" % line.strip())
                continue
            # Process events hourly until it's time to execute this command
            while last_processing_time is not None and last_processing_time <= event.timestamp:
                logging.debug("PROCESSING at %d" % last_processing_time)
                process_events(last_processing_time, internal_state)
                last_processing_time += ONE_HOUR_IN_MS
            logging.debug("EVENT: (%d) %s" % (event.timestamp, line.strip()))
            handlers[event.event](event, internal_state)
            if last_processing_time is None:
                last_processing_time = roundup(event.timestamp, ONE_HOUR_IN_MS)
            last_ts = event.timestamp
    handle_stop(None, None)


# BCTODO remove tier to cost and require costs in the event scripts.  It's unnecessary complexity.
def get_cost(compute_tier):
    if compute_tier in TIER_TO_COST:
        return TIER_TO_COST[compute_tier]
    else:
        return float(compute_tier)


def parse_date(ts, top_of_hour=False):
    """Parse a date string.  If top_of_hour=True, then round to the top of the current hour.
    If the year is missing, the current year will be used."""
    date = None
    if ts.startswith('now '):
        date = datetime.now()
    else:
        for template in ['%m-%d-%Y %H:%M', '%m-%d-%y %H:%M', '%m-%d %H:%M']:
            try:
                date = datetime.strptime(ts, template)
                if date.year == 1900:
                    now = datetime.now()
                    date = date.replace(year=now.year)
                break
            except ValueError:
                pass
    if date and top_of_hour:
        date = date.replace(minute=0, second=0, microsecond=0)
    return date


def handle_uuid(event, _unused_internal_state):
    """Establish a VM name to UUID mapping"""
    vm_name = event.params['vm_name']
    if vm_name in vm_to_uuid_map:
        logging.warning('UUID mapping in script for "%s" was overridden on the command line - ignoring' % vm_name)
        return
    vm_to_uuid_map[vm_name] = event.params['uuid']


def handle_start(event, _unused_internal_state):
    """Script start - set the starting timestamp for the scenario"""
    logging.debug("Handling start event")
    global base_timestamp
    start_ts = event.params['date'] + ' ' + event.params['time']
    date = parse_date(start_ts, True)
    if not date:
        logging.error('Invalid timestamp: "%s". Format is "M-D[-[CC]YY]] HH:MM' % start_ts[:-7])
        return
    base_timestamp = int(date.timestamp() * 1000)
    event.timestamp = base_timestamp


def handle_stop(_unused_event, _unused_internal_state):
    """Script end - stop generation of events"""
    logging.debug("Handling stop event")
    if args.sql:
        flush_sql()
    sys.exit(0)


def handle_create(event, _unused_internal_state):
    """Handle entity created event.  We do nothing with these.  We rely on a resize recommendation to start tracking
    an entity."""
    entity_name = event.get_entity_name()
    logging.debug("Handling create event: %s", entity_name)


def handle_remove(event, internal_state):
    # Mark entity for removal during next processing pass
    vm = internal_state.get_entity(event)
    logging.debug("Handling remove event for %s" % event.get_entity_name())
    if vm:
        vm.shutdown(internal_state, event.timestamp)


def handle_power(event, internal_state):
    # Locate existing savings.  If we aren't tracking, then ignore the event
    entity_state = internal_state.get_entity(event)
    if entity_state:
        new_state = event.get_state()
        logging.debug("Handling power event: %s -> %s" % (event.get_entity_name(), new_state))
        # Accumulate ps, pi, pms, pmi up to this point
        entity_state.end_segment(event.timestamp)
        entity_state.power_factor = 1.0 if new_state == 'on' else 0.0


def handle_resize_recommendation(event, internal_state):
    """Handle a resize recommendation that has not yet been executed.  This starts accumulating missed savings
    or investments."""
    # BCTODO need to handle recommendations going away in the real code.
    logging.debug("Handling resize event")
    # We have a recommendation for an entity, so we are going to start tracking savings/investments.  Get the
    # current savings for this entity, or create a new savings tracker for it.

    recommendation = Recommendation(event)
    entity_state = internal_state.get_entity(event)
    if not entity_state:
        entity_state = internal_state.create_entity(event, EntityType.VIRTUAL_MACHINE)
        entity_state.baseline = get_cost(recommendation.source_tier)

    # Close out the current segment and open a new with the new periodic missed savings/investment
    entity_state.end_segment(event.timestamp)

    # If there is an existing recommendation, the new one overrides the old one.  Back it out.
    if entity_state.current_recommendation:
        r = entity_state.current_recommendation
        entity_state.current_missed_savings -= r.get_savings()
        entity_state.current_missed_investment -= r.get_investment()

    entity_state.current_missed_savings += recommendation.get_savings()
    entity_state.current_missed_investment += recommendation.get_investment()

    # Save the current recommendation so that we can match it up with a future action execution.
    entity_state.current_recommendation = recommendation


def handle_action_executed(event, internal_state):
    common_resize_handler(event, internal_state, False)


def handle_provider_change(event, internal_state):
    common_resize_handler(event, internal_state, True)


def common_resize_handler(event, internal_state, require_previous_recommendation):
    """This tool handles detected resizes.  This same logic runs for both detected resizes and successful
    resize executions."""
    logging.debug("Handling execute event")
    entity_state = internal_state.get_entity(event)
    entity_state.end_segment(event.timestamp)  # Flush previous accumulation
    if entity_state:
        rec = entity_state.current_recommendation
        if entity_state.current_recommendation:
            # BCTODO in the code, this is dangerous if we are processing an action from before the feature was enabled
            if require_previous_recommendation:
                if rec.source_tier != event.get_source_tier() or rec.dest_tier != event.get_dest_tier():
                    # There's a current recommendation, but it doesn't match this resize, so ignore it
                    return
            # Back out missed savings/investment incurred by this recommendation.
            entity_state.current_missed_savings -= entity_state.current_recommendation.get_savings()
            entity_state.current_missed_investment -= entity_state.current_recommendation.get_investment()
            entity_state.current_recommendation = None
        elif require_previous_recommendation:
            # No active resize action, so ignore this.
            return
        delta = get_cost(event.get_dest_tier()) - get_cost(event.get_source_tier())
        if delta < 0:
            entity_state.current_savings = entity_state.previous_savings - delta
        else:
            entity_state.current_investment = delta + entity_state.previous_investment


handlers = {
    'nop': lambda x, y: None,
    'start': handle_start,
    'stop': handle_stop,
    'create': handle_create,
    'remove': handle_remove,
    'power': handle_power,
    'resize': handle_resize_recommendation,
    'execute': handle_action_executed,
    'change': handle_provider_change,
    'uuid': handle_uuid
}


def process_events(timestamp, entity_state):
    # For each entity that we are tracking, process transitions and update new periodic amounts
    for vm_name, entity_state in entity_state.entity_state.items():
        entity_state.end_period(timestamp)


def parse_arguments():
    parser = argparse.ArgumentParser(description='Generate test savings data from a scenario script')
    parser.add_argument('script', help='scenario script to use', type=str)
    parser.add_argument('entity=uuid', help='Define entity to UUID mapping', type=str, nargs='*')
    # BCTODO Next phase will consult an appliance to map entity names to UUIDs.  For now, use a valid UUID in
    #  place of the entity name.
    # parser.add_argument('--server', help='address of appliance', type=str)
    # parser.add_argument('--user', help='appliance username (default administrator)', type=str,
    #                     default='administrator')
    # parser.add_argument('--pass', help='appliance password (default "a")', type=str, default='a')
    parser.add_argument('--csv', help='generate CSV output', action='store_true')
    parser.add_argument('--csv2', help='generate CSV output with mnemonic entry types', action='store_true')
    parser.add_argument('--sql', help='generate SQL output', action='store_true')
    parser.add_argument('--drop', help='drop table before generating SQL output', action='store_true')
    parser.add_argument('--verbose', help='generate verbose/debug output', action='store_true')
    return parser.parse_args()


def create_table():
    logging.debug("Dropping and recreating entity_savings_stats_hourly table")
    print("""DROP TABLE IF EXISTS   entity_savings_stats_hourly;
        CREATE TABLE entity_savings_stats_hourly (
          entity_id BIGINT(20) NOT NULL,
          stats_time DATETIME NOT NULL,
          stats_type INT NOT NULL,
          stats_value DECIMAL(20, 7) NOT NULL,
          PRIMARY KEY (entity_id, stats_time, stats_type)
        );""")


def flush_sql():
    for offset in range(0, len(sql_output), SQL_INSERT_CHUNK_SIZE):
        print('INSERT INTO entity_savings_stats_hourly VALUES %s;'
              % ','.join(sql_output[offset:offset+SQL_INSERT_CHUNK_SIZE]))


def main():
    global args
    args = parse_arguments()
    args.csv = args.csv or args.csv2  # csv2 implies csv
    for mapping in args.__dict__['entity=uuid']:
        f = mapping.split('=')
        vm_to_uuid_map[f[0]] = f[1]
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    # Emit headers
    if args.csv and args.verbose:
        print("uuid,timestamp,entry_type,value")
    if args.sql:
        print('USE cost;')
        if args.drop:
            create_table()
    # Parse script file and generate data
    parse_script(args.script)


if __name__ == "__main__":
    main()
