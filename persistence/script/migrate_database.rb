
require 'fileutils'

# Check MySQL availability
TEnd = Time.now.to_i + 120
res = `mysql -uvmtplatform -pvmturbo vmtdb -e 'select * from version_info'`
while res.empty? && Time.now.to_i < TEnd
	sleep(1)
	res = `mysql -uvmtplatform -pvmturbo vmtdb -e 'select * from version_info'`
end
if res.empty?
	puts  "Error: We are unable to connect to MySQL, please contact VMTurbo support." 
	exit(1)
else
	puts "MySQL restart finished.."
end


# Get the version no of the installed database

begin
  vinfo = VersionInfo.find(:first)
rescue
  vinfo = nil
end

curr_version = vinfo ? vinfo.version.to_i : 18      
  # version info was introduced in version 19, so it is at least as old as 18

#
# Given that the version no is available, find the migrate scripts that will
# up-version the database and apply them in order.
#

puts "Curr Version is #{curr_version.to_s}"

FileUtils.cd "#{ENV['persistence_BASE']}/db"


# Find the maximum target version number

up_version = curr_version
Dir.new(".").each { |filename|
  if filename[/migrate_\d+_to_(\d+)/] then
    file_up_version = filename.match(/migrate_\d+_to_(\d+)/)[1].to_i
    up_version = [up_version, file_up_version].max
  end
}

puts "Up Version is #{up_version}"


# run the migrate scripts up to and including the latest number

if up_version > curr_version then
  begin
    for i in curr_version+1..up_version do
      mig_file = "migrate_#{i-1}_to_#{i}.sql"
      puts "Running mysql -uvmtplatform -pvmturbo < #{mig_file}"
      if(i==61)
        puts `ruby migrate_60_to_61.rb`
      end
      puts `mysql -uvmtplatform -pvmturbo < #{mig_file} 2>&1`
    end
  rescue
    puts "*** Error: Migration failed while executing upgrade scripts"
  end

else
  puts "No schema migrations are required"

end

unless File.exists?("/tmp/user_reports_2010.sql")
  puts "Backing up existing custom reports..."
  puts `mysqldump -f -uvmtplatform -pvmturbo vmtdb --extended_insert=false --tables user_reports > /tmp/user_reports_2010.sql`

  puts "Removing obsolete custom reports..."
  for rpt in UserReport.find(:all) do
    rpt.delete if rpt.is_obsolete?
  end
end

puts "Adding/updating reference data..."
puts `mysql -uvmtplatform -pvmturbo < add_ref_data.sql 2>&1`

unless File.exist?("../doc_queue")
  FileUtils.mkdir("../doc_queue")
end

#puts "Loading entity data..."
#puts `cp entity_data.xml ../doc_queue/. 2>&1`
#puts `ruby ../script/runner -e production ../script/entity_loader.rb 2>&1`

puts "Updating views..."
puts `mysql -uvmtplatform -pvmturbo < views.sql 2>&1`
puts "Updating functions..."
puts `mysql -uvmtplatform -pvmturbo < functions.sql 2>&1`
puts "Updating stored procedures..."
puts `mysql -uvmtplatform -pvmturbo < stored_procedures.sql 2>&1`
