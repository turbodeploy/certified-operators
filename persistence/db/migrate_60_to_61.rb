@mysql = 'mysql -u root -uvmtplatform -pvmturbo vmtdb'

def execMysql(command)
	cmd = "#{@mysql} --skip-column-names -e \"" + command + "\""
	#puts cmd
	`#{cmd}`
end

def log(msg)
	puts "[#{Time.new.strftime('%b-%d %H:%M:%S')}] #{msg}"
end

tables = %w(vm pm ds app vdc sw da sc ch iom)
timeFrames = %w(hour day month)

def epochToDate(table, colName, colType)
	execMysql("ALTER TABLE #{table} ADD COLUMN #{colName}_tmp #{colType} AFTER #{colName};")
	execMysql("UPDATE      #{table} SET #{colName}_tmp = from_unixtime(#{colName}/1000);")
	execMysql("ALTER TABLE #{table} DROP COLUMN #{colName}" +
		(table.include?('stats') ? ', DROP COLUMN std_dev':'') +
		';') 
	execMysql("ALTER TABLE #{table} CHANGE #{colName}_tmp #{colName} #{colType};")
end


##############
#### MAIN ####
##############

tables.product(timeFrames).each do |table, tFrame|
	#table name of stats-by-timeframe
	table += "_stats_by_#{tFrame}"
	colType = tFrame=='hour' ? 'DATETIME' : 'DATE'

	log "Alter table: "+table

	epochToDate(table, 'snapshot_time', colType)
end

log "Alter table: audit_log_entries"
epochToDate('audit_log_entries', 'snapshot_time', 'DATETIME')

log "Alter table entities"
execMysql("ALTER TABLE entities  
	CHANGE created_at created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
	DROP COLUMN updated_at,
	ADD INDEX(name), ADD INDEX(created_at);")

log "Alter table entity_attrs"
execMysql("ALTER TABLE entity_attrs 
	DROP COLUMN data_type, 
	ADD CONSTRAINT ent_attr_fk_src_id foreign key (entity_entity_id) references entities(id) ON DELETE CASCADE
	;")

log "Alter table entity_assns"
execMysql("ALTER TABLE entity_assns 
	ADD CONSTRAINT ent_assn_fk_src_id foreign key (entity_entity_id) references entities(id) ON DELETE CASCADE
	;")

log "Alter table entity_assns_members_entities"
execMysql("ALTER TABLE entity_assns_members_entities 
	ADD CONSTRAINT ent_assn_mem_ent_fk_assn_id foreign key (entity_assn_src_id) references entity_assns(id) ON DELETE CASCADE
	;")


log("Alter table notifications")
epochToDate('notifications', 'clear_time', 'DATETIME')
epochToDate('notifications', 'last_notify_time', 'DATETIME')

## TODO: MODIFY REPORT_SUBSCRIPTIONS

log "Done!"